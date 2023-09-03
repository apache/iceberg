---
hide:
  - navigation
---

<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Python API

PyIceberg is based around catalogs to load tables. First step is to instantiate a catalog that loads tables. Let's use the following configuration to define a catalog called `prod`:

```yaml
catalog:
  prod:
    uri: http://rest-catalog/ws/
    credential: t-1234:secret
```

This information must be placed inside a file called `.pyiceberg.yaml` located either in the `$HOME` or `%USERPROFILE%` directory (depending on whether the operating system is Unix-based or Windows-based, respectively) or in the `$PYICEBERG_HOME` directory (if the corresponding environment variable is set).

For more details on possible configurations refer to the [specific page](https://py.iceberg.apache.org/configuration/).

Then load the `prod` catalog:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "docs",
    **{
        "uri": "http://127.0.0.1:8181",
        "s3.endpoint": "http://127.0.0.1:9000",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }
)
```

Let's create a namespace:

```python
catalog.create_namespace("docs_example")
```

And then list them:

```python
ns = catalog.list_namespaces()

assert ns == [("docs_example",)]
```

And then list tables in the namespace:

```python
catalog.list_tables("docs_example")
```

## Create a table

To create a table from a catalog:

```python
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)

schema = Schema(
    NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=True),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=True),
    NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
    NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
    NestedField(
        field_id=5,
        name="details",
        field_type=StructType(
            NestedField(
                field_id=4, name="created_by", field_type=StringType(), required=False
            ),
        ),
        required=False,
    ),
)

from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

partition_spec = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)

from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

# Sort on the symbol
sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

catalog.create_table(
    identifier="docs_example.bids",
    schema=schema,
    partition_spec=partition_spec,
    sort_order=sort_order,
)
```

## Load a table

### Catalog table

Loading the `bids` table:

```python
table = catalog.load_table("docs_example.bids")
# Equivalent to:
table = catalog.load_table(("docs_example", "bids"))
# The tuple syntax can be used if the namespace or table contains a dot.
```

This returns a `Table` that represents an Iceberg table that can be queried and altered.

### Static table

To load a table directly from a metadata file (i.e., **without** using a catalog), you can use a `StaticTable` as follows:

```python
from pyiceberg.table import StaticTable

static_table = StaticTable.from_metadata(
    "s3://warehouse/wh/nyc.db/taxis/metadata/00002-6ea51ce3-62aa-4197-9cf8-43d07c3440ca.metadata.json"
)
```

The static-table is considered read-only.

## Schema evolution

PyIceberg supports full schema evolution through the Python API. It takes care of setting the field-IDs and makes sure that only non-breaking changes are done (can be overriden).

In the examples below, the `.update_schema()` is called from the table itself.

```python
with table.update_schema() as update:
    update.add_column("some_field", IntegerType(), "doc")
```

You can also initiate a transaction if you want to make more changes than just evolving the schema:

```python
with table.transaction() as transaction:
    with transaction.update_schema() as update_schema:
        update.add_column("some_other_field", IntegerType(), "doc")
    # ... Update properties etc
```

### Add column

Using `add_column` you can add a column, without having to worry about the field-id:

```python
with table.update_schema() as update:
    update.add_column("retries", IntegerType(), "Number of retries to place the bid")
    # In a struct
    update.add_column("details.confirmed_by", StringType(), "Name of the exchange")
```

### Rename column

Renaming a field in an Iceberg table is simple:

```python
with table.update_schema() as update:
    update.rename("retries", "num_retries")
    # This will rename `confirmed_by` to `exchange`
    update.rename("properties.confirmed_by", "exchange")
```

### Move column

Move a field inside of struct:

```python
with table.update_schema() as update:
    update.move_first("symbol")
    update.move_after("bid", "ask")
    # This will move `confirmed_by` before `exchange`
    update.move_before("details.created_by", "details.exchange")
```

### Update column

Update a fields' type, description or required.

```python
with table.update_schema() as update:
    # Promote a float to a double
    update.update_column("bid", field_type=DoubleType())
    # Make a field optional
    update.update_column("symbol", required=False)
    # Update the documentation
    update.update_column("symbol", doc="Name of the share on the exchange")
```

Be careful, some operations are not compatible, but can still be done at your own risk by setting `allow_incompatible_changes`:

```python
with table.update_schema(allow_incompatible_changes=True) as update:
    # Incompatible change, cannot require an optional field
    update.update_column("symbol", required=True)
```

### Delete column

Delete a field, careful this is a incompatible change (readers/writers might expect this field):

```python
with table.update_schema(allow_incompatible_changes=True) as update:
    update.delete_column("some_field")
```

## Table properties

Set and remove properties through the `Transaction` API:

```python
with table.transaction() as transaction:
    transaction.set_properties(abc="def")

assert table.properties == {"abc": "def"}

with table.transaction() as transaction:
    transaction.remove_properties("abc")

assert table.properties == {}
```

Or, without context manager:

```python
table = table.transaction().set_properties(abc="def").commit_transaction()

assert table.properties == {"abc": "def"}

table = table.transaction().remove_properties("abc").commit_transaction()

assert table.properties == {}
```

## Query the data

To query a table, a table scan is needed. A table scan accepts a filter, columns, optionally a limit and a snapshot ID:

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual

catalog = load_catalog("default")
table = catalog.load_table("nyc.taxis")

scan = table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
    limit=100,
)

# Or filter using a string predicate
scan = table.scan(
    row_filter="trip_distance > 10.0",
)

[task.file.file_path for task in scan.plan_files()]
```

The low level API `plan_files` methods returns a set of tasks that provide the files that might contain matching rows:

```json
[
  "s3://warehouse/wh/nyc/taxis/data/00003-4-42464649-92dd-41ad-b83b-dea1a2fe4b58-00001.parquet"
]
```

In this case it is up to the engine itself to filter the file itself. Below, `to_arrow()` and `to_duckdb()` that already do this for you.

### Apache Arrow

<!-- prettier-ignore-start -->

!!! note "Requirements"
    This requires [PyArrow to be installed](index.md).

<!-- prettier-ignore-end -->

Using PyIceberg it is filter out data from a huge table and pull it into a PyArrow table:

```python
table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
).to_arrow()
```

This will return a PyArrow table:

```
pyarrow.Table
VendorID: int64
tpep_pickup_datetime: timestamp[us, tz=+00:00]
tpep_dropoff_datetime: timestamp[us, tz=+00:00]
----
VendorID: [[2,1,2,1,1,...,2,2,2,2,2],[2,1,1,1,2,...,1,1,2,1,2],...,[2,2,2,2,2,...,2,6,6,2,2],[2,2,2,2,2,...,2,2,2,2,2]]
tpep_pickup_datetime: [[2021-04-01 00:28:05.000000,...,2021-04-30 23:44:25.000000]]
tpep_dropoff_datetime: [[2021-04-01 00:47:59.000000,...,2021-05-01 00:14:47.000000]]
```

This will only pull in the files that that might contain matching rows.

### DuckDB

<!-- prettier-ignore-start -->

!!! note "Requirements"
    This requires [DuckDB to be installed](index.md).

<!-- prettier-ignore-end -->

A table scan can also be converted into a in-memory DuckDB table:

```python
con = table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
).to_duckdb(table_name="distant_taxi_trips")
```

Using the cursor that we can run queries on the DuckDB table:

```python
print(
    con.execute(
        "SELECT tpep_dropoff_datetime - tpep_pickup_datetime AS duration FROM distant_taxi_trips LIMIT 4"
    ).fetchall()
)
[
    (datetime.timedelta(seconds=1194),),
    (datetime.timedelta(seconds=1118),),
    (datetime.timedelta(seconds=1697),),
    (datetime.timedelta(seconds=1581),),
]
```

### Ray

<!-- prettier-ignore-start -->

!!! note "Requirements"
    This requires [Ray to be installed](index.md).

<!-- prettier-ignore-end -->

A table scan can also be converted into a Ray dataset:

```python
ray_dataset = table.scan(
    row_filter=GreaterThanOrEqual("trip_distance", 10.0),
    selected_fields=("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
).to_ray()
```

This will return a Ray dataset:

```
Dataset(
    num_blocks=1,
    num_rows=1168798,
    schema={
        VendorID: int64,
        tpep_pickup_datetime: timestamp[us, tz=UTC],
        tpep_dropoff_datetime: timestamp[us, tz=UTC]
    }
)
```

Using [Ray Dataset API](https://docs.ray.io/en/latest/data/api/dataset.html) to interact with the dataset:

```python
print(ray_dataset.take(2))
[
    {
        "VendorID": 2,
        "tpep_pickup_datetime": datetime.datetime(2008, 12, 31, 23, 23, 50),
        "tpep_dropoff_datetime": datetime.datetime(2009, 1, 1, 0, 34, 31),
    },
    {
        "VendorID": 2,
        "tpep_pickup_datetime": datetime.datetime(2008, 12, 31, 23, 5, 3),
        "tpep_dropoff_datetime": datetime.datetime(2009, 1, 1, 16, 10, 18),
    },
]
```
