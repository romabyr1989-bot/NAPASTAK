# NAPASTAK — Column Types and Cross-System Conversion

A pipeline usually moves data between two *different* systems: PostgreSQL to
Kafka, Oracle to MS SQL, CSV to Greenplum. There is no pairwise conversion
table. Every connector maps its native types onto one small logical set, and
every sink maps that set back onto its own native types.

## The logical set

| Logical type | Stored as | Meaning |
|---|---|---|
| `int64` | `int64_t` | 64-bit signed integer |
| `double` | `double` | binary floating point (`float4`/`float8`, `BINARY_DOUBLE`) |
| `decimal` | text | **exact** decimal of arbitrary precision — money, identifiers |
| `date` | text | calendar date, ISO-8601 `YYYY-MM-DD` |
| `timestamp` | text | date and time, ISO-8601 |
| `bool` | `int64_t` | true/false |
| `text` | text | anything else |

"Stored as text" is the important part: a `decimal` never passes through a
binary mantissa, so `12345678901234567890.12345` survives a full round trip —
25 significant digits, well beyond what a `double` can hold. The logical type
is metadata; it decides how the value is *rendered* and what column the sink
creates, not how it is kept.

`decimal`, `date` and `timestamp` were appended to the end of the enum on
purpose: the numeric codes of the older types do not shift, so table schemas
written by an earlier build keep loading. A name the build does not know falls
back to `text` — the value is still there, only its presentation is lost.

## Source mapping

| Source type | Logical |
|---|---|
| PostgreSQL `int2/int4/int8`, `serial` | `int64` |
| PostgreSQL `float4/float8` | `double` |
| PostgreSQL `numeric`, `decimal` | `decimal` |
| PostgreSQL `date` / `timestamp[tz]` | `date` / `timestamp` |
| Oracle `NUMBER(p,0)`, p ≤ 18 | `int64` |
| Oracle `NUMBER` otherwise | `decimal` |
| Oracle `DATE` / `TIMESTAMP[ TZ\|LTZ]` | `date` / `timestamp` |
| MS SQL `tinyint/smallint/int/bigint` | `int64` |
| MS SQL `real/float` | `double` |
| MS SQL `decimal/numeric/money/smallmoney` | `decimal` |
| MS SQL `date` / `datetime[2]`, `smalldatetime`, `datetimeoffset` | `date` / `timestamp` |
| CSV, JSON/HTTP, XML, SOAP, Siebel | `text` |

Oracle `NUMBER` is capped at `int64` only up to 18 digits: the type holds 38,
and `int64` tops out at 19, so anything wider goes to `decimal` rather than
being clipped.

## Sink mapping

| Logical | PostgreSQL / Greenplum | Oracle | MS SQL |
|---|---|---|---|
| `int64` | `BIGINT` | `NUMBER(19)` | `BIGINT` |
| `double` | `NUMERIC` | `BINARY_DOUBLE` | `FLOAT` |
| `decimal` | `NUMERIC` | `NUMBER` | *text* — see below |
| `date` | `DATE` | `DATE` | `DATE` |
| `timestamp` | `TIMESTAMP` | `TIMESTAMP` | `DATETIME2` |
| `bool` | `BOOLEAN` | `NUMBER(1)` | `BIT` |
| `text` | `TEXT` | `VARCHAR2(4000)` | `NVARCHAR(MAX)` |

`NUMERIC` in PostgreSQL and `NUMBER` in Oracle take no parameters and are
therefore unbounded — a `decimal` lands natively and loses nothing.

**MS SQL is the exception.** `DECIMAL` there has no unbounded form: a bare
`DECIMAL` means `DECIMAL(18,0)` and silently drops the fractional part, and
picking `DECIMAL(38,10)` by guesswork would silently round somebody else's
data. Until the schema carries precision and scale, a `decimal` is written to
SQL Server as text — lossless, if less convenient.

Oracle has no `BOOLEAN` for table columns before 23c; `NUMBER(1)` is the usual
substitute.

## Forcing a type

`CAST` in `transform_sql` propagates the declared type to the sink, so a text
column can be landed as a real numeric or date one:

```sql
SELECT CAST(amount AS numeric) AS amount,
       CAST(created  AS timestamp) AS created
FROM staging
```

Recognised names: `int*`/`big*`/`small*`, `num*`/`dec*`/`money*`,
`float`/`double`/`real`, `date`, `timestamp`/`datetime`, `bool*`.

## What is exact and what is not

Exact: storage, transport between any source and any sink, `WHERE`
comparisons, `ORDER BY`, `MIN`, `MAX`, and rendering. A `decimal` is compared
numerically and a `date` chronologically (ISO-8601 sorts correctly as text).

Not exact: `SUM` and `AVG` are computed in `double`. For money this is exact
well past any realistic balance — a control sum of `10000123456790.16` comes
back to the kopeck — but a `decimal` beyond about 2^53 in magnitude will round
in an aggregate. Exact decimal arithmetic needs a bignum layer in the engine
and is not implemented.

Values are formatted with the shortest decimal string that reads back to the
same bits, so `0.1` stays `0.1` while a large sum keeps every digit it needs.
