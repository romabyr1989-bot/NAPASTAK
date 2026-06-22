# Scala pipeline steps

A pipeline step that hands data to a `scala-cli` subprocess and reads the
result back. The user code operates on a **Spark `DataFrame`** named
`df`; whatever `df` looks like at the end of the script is what gets
ingested into `target_table`.

This is the Scala/Spark counterpart of the [Python step](PYTHON_STEPS.md):
same input/output plumbing, different front-end. It unlocks Spark SQL
transformations, JVM libraries, and the Spark DataFrame API from inside a
DFO pipeline — without standing up a separate Spark cluster (Spark runs
embedded in `local[*]` mode).

## Requirements

- `scala-cli` on `$PATH` (the gateway uses `execvp`).
- A JVM (`scala-cli` manages the Scala toolchain itself).
- Network access on the **first** run: `scala-cli` resolves and caches
  `spark-sql` via the `//> using dep` directive in the generated wrapper.
  Subsequent runs reuse the cache.

If `scala-cli` isn't installed, the step fails fast with `exit=127` in
`pipelines.error_msg` (and the `stderr` tail in the run log).

`scala-cli` manages its own JVM (downloaded via coursier on first use), so a
system JVM is not required. The generated wrapper carries the
`--add-opens=java.base/...` JVM flags Spark 3.5 needs to start on JDK 17
(harmless on JDK 8/11) — without them SparkContext init dies with an
`IllegalAccessError` on `sun.nio.ch.DirectBuffer`.

## Step shape

A Scala step is just a regular `PipelineStep` with two extra fields:

```json
{
  "id": "score_filter",
  "name": "double-and-filter",
  "transform_sql":     "SELECT user_id, score FROM raw_events",
  "scala_code":        "import org.apache.spark.sql.functions._\ndf = df.withColumn(\"score\", col(\"score\").cast(\"int\") * 2)\ndf = df.filter(col(\"score\") > 60)",
  "scala_timeout_sec": 600,
  "target_table":      "high_scorers"
}
```

| Field               | Purpose                                                                          |
|---------------------|----------------------------------------------------------------------------------|
| `scala_code`        | User Scala (≤ 8 KB). Reassigns `df`. Required for the step to be a Scala step.    |
| `scala_timeout_sec` | Wall-clock deadline. Default 600. After this the gateway sends `SIGKILL`.         |
| `transform_sql`     | Optional. If set, its result is fed to the script as CSV on **stdin**.           |
| `target_table`      | Optional. If set, the final `df` is written there.                               |

> The timeout default is **600s** (not 300s like Python) because Spark's
> first-run dependency resolution + JVM/SparkSession cold-start is slow.

## Execution model

```
                ┌──────────────┐
   transform_sql│ exec_stmt    │ → CSV bytes
                └─────┬────────┘
                      │ stdin
                      ▼
          ┌──────────────────────┐  stderr ─► error tail (1 KB)
          │ scala-cli run <wrap> │
          └─────┬────────────────┘
                │ stdout
                ▼
            CSV bytes ─► write_rs_to_table → target_table
```

The wrapper script DFO compiles around your code is roughly:

```scala
//> using scala 2.13.14
//> using dep org.apache.spark::spark-sql:3.5.1
import org.apache.spark.sql.{SparkSession, DataFrame}
object DfoScalaStep {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dfo-scala-step").master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val lines = scala.io.Source.stdin.getLines().toList
    var df: DataFrame =
      if (lines.isEmpty) spark.emptyDataFrame
      else spark.read.option("header", "true").option("inferSchema", "false")
                .csv(lines.toDS())
    // ── user code ──
    <your code>
    // ── /user code ──
    // df is collected and emitted as CSV on stdout
    spark.stop()
  }
}
```

So:
- `df` is a `var` — reassign it (`df = df.filter(...)`), don't try to mutate
  it in place (Spark DataFrames are immutable).
- **all input columns arrive as `String`** (`inferSchema=false`), mirroring
  the CSV round-trip of the Python step. Cast explicitly when you need
  numeric ops: `col("score").cast("int")`.
- if `transform_sql` is empty, `df` starts as an empty DataFrame — you can
  build one from scratch (e.g. `df = Seq((1,"a")).toDF("x","y")`).
- output goes through `df.collect()` → CSV, so non-string types round-trip
  via CSV (re-inferred when ingested — usually fine).

`spark` and `spark.implicits._` are in scope for the user code.

## Failure modes

| What                          | Result                                                |
|-------------------------------|-------------------------------------------------------|
| `scala-cli` not on `$PATH`    | step fails, `error_msg` says exit=127                 |
| Spark dep resolution fails    | step fails, last 1 KB of stderr in `error_msg`        |
| Compile error in `scala_code` | step fails, last 1 KB of stderr in `error_msg`        |
| Script exceeds timeout        | gateway sends `SIGKILL`, step fails with timeout msg  |
| Output CSV malformed          | step fails (no header / unparseable rows)             |

Failures use the same retry policy as any other step (`max_retries`,
`retry_delay_sec`). Set `max_retries: 0` if a deterministic failure
should not be retried.

## UI

The pipeline builder's **Тип шага** dropdown has a **⚡ Scala (Spark)**
option. Selecting it:

- clears `connector_type` / `connector_config`
- exposes a code textarea for `scala_code` (with a starter snippet)
- exposes a numeric input for `scala_timeout_sec`
- the **▶ Scala** button runs just this step against current tables

## Security

This step runs arbitrary code as the gateway user (same trust model as the
Python and bash/connector steps). It is gated by:

- the JWT/RBAC required to *create* a pipeline (`PIPELINE_WRITE`)
- the timeout — the script is killed at the deadline regardless

There is **no** sandbox / chroot / seccomp around the child. In a
multi-tenant deployment, restrict pipeline-write permissions to trusted
users.

## Limits

- `scala_code` ≤ 8192 bytes (`PipelineStep.scala_code`)
- input + output CSV go through pipes — buffered in the gateway's arena, so
  very large frames (≫ 100 MB) should use a `connector` step instead
- one subprocess per step (no pool / no warm JVM) — every run pays the
  SparkSession startup cost
