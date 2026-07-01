# NAPASTAK — Python SDK

Python client for the [NAPASTAK](https://github.com/your-org/dfo) data platform.

## Installation

```bash
pip install dataflow-os
# or from source
pip install -e ".[dev]"
```

## Quick start

```python
from dataflow_os import Client

# Authenticate with API key
client = Client("https://localhost:8080", api_key="dfo_...")

# Or with username / password
client = Client("https://localhost:8080", username="admin", password="secret")
```

## Query

```python
import pandas as pd

df = client.query("SELECT id, name, age FROM users WHERE age > 25")
print(df.head())
```

## Ingest

```python
import pandas as pd

data = pd.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "carol"]})
result = client.ingest("users", data)
# {"inserted": 3}
```

## Table proxy

```python
users = client.table("users")

# First 10 rows
print(users.head(10))

# Row count
print(users.count())

# Selective columns
df = users.select("id, name")

# Schema
print(users.schema())

# Insert a DataFrame
import pandas as pd
users.insert(pd.DataFrame({"id": [4], "name": ["dave"]}))
```

## Transactions

```python
with client.transaction() as txn:
    txn.query("INSERT INTO orders VALUES (1, 'item-A', 42.0)")
    txn.query("UPDATE inventory SET qty = qty - 1 WHERE item = 'item-A'")
# COMMIT is sent automatically on successful exit
# ROLLBACK is sent automatically on exception
```

## Pipelines

```python
# List pipelines
pipelines = client.pipelines()

# Create a pipeline
cfg = {"name": "etl-users", "source": "csv://data/users.csv", "target": "users"}
p = client.create_pipeline(cfg)

# Run a pipeline
client.run_pipeline(p["id"])

# Using the Pipeline proxy
pipeline = client.pipeline(p["id"])
pipeline.run()
info = pipeline.get()
history = pipeline.runs()
pipeline.delete()
```

## Health check

```python
print(client.health())
# {"status": "ok", ...}
```

## Error handling

```python
from dataflow_os import AuthError, PermissionError, TableNotFoundError, DataFlowError

try:
    df = client.query("SELECT * FROM restricted")
except AuthError:
    print("Invalid credentials or token expired")
except PermissionError:
    print("Access denied")
except TableNotFoundError:
    print("Table does not exist")
except DataFlowError as e:
    print(f"Server error: {e}")
```

## Running tests

```bash
pip install -e ".[dev]"
pytest tests/ -v --cov=dataflow_os
```
