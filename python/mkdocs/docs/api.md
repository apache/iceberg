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

catalog = load_catalog("prod")

catalog.list_namespaces()
```

Returns two namespaces:

```python
[("default",), ("nyc",)]
```

Listing the tables in the `nyc` namespace:

```python
catalog.list_tables("nyc")
```

Returns as list with tuples, containing a single table `taxis`:

```python
[("nyc", "taxis")]
```

## Load a table

### From a catalog

Loading the `taxis` table:

```python
catalog.load_table("nyc.taxis")
# Equivalent to:
catalog.load_table(("nyc", "taxis"))
# The tuple syntax can be used if the namespace or table contains a dot.
```

This returns a `Table` that represents an Iceberg table:

```python
Table(
  identifier=('nyc', 'taxis'),
  metadata_location='s3a://warehouse/wh/nyc.db/taxis/metadata/00002-6ea51ce3-62aa-4197-9cf8-43d07c3440ca.metadata.json',
  metadata=TableMetadataV2(
    location='s3a://warehouse/wh/nyc.db/taxis',
    table_uuid=UUID('ebd5d172-2162-453d-b586-1cdce52c1116'),
    last_updated_ms=1662633437826,
    last_column_id=19,
    schemas=[Schema(
        NestedField(field_id=1, name='VendorID', field_type=LongType(), required=False),
        NestedField(field_id=2, name='tpep_pickup_datetime', field_type=TimestamptzType(), required=False),
        NestedField(field_id=3, name='tpep_dropoff_datetime', field_type=TimestamptzType(), required=False),
        NestedField(field_id=4, name='passenger_count', field_type=DoubleType(), required=False),
        NestedField(field_id=5, name='trip_distance', field_type=DoubleType(), required=False),
        NestedField(field_id=6, name='RatecodeID', field_type=DoubleType(), required=False),
        NestedField(field_id=7, name='store_and_fwd_flag', field_type=StringType(), required=False),
        NestedField(field_id=8, name='PULocationID', field_type=LongType(), required=False),
        NestedField(field_id=9, name='DOLocationID', field_type=LongType(), required=False),
        NestedField(field_id=10, name='payment_type', field_type=LongType(), required=False),
        NestedField(field_id=11, name='fare_amount', field_type=DoubleType(), required=False),
        NestedField(field_id=12, name='extra', field_type=DoubleType(), required=False),
        NestedField(field_id=13, name='mta_tax', field_type=DoubleType(), required=False),
        NestedField(field_id=14, name='tip_amount', field_type=DoubleType(), required=False),
        NestedField(field_id=15, name='tolls_amount', field_type=DoubleType(), required=False),
        NestedField(field_id=16, name='improvement_surcharge', field_type=DoubleType(), required=False),
        NestedField(field_id=17, name='total_amount', field_type=DoubleType(), required=False),
        NestedField(field_id=18, name='congestion_surcharge', field_type=DoubleType(), required=False),
        NestedField(field_id=19, name='airport_fee', field_type=DoubleType(), required=False)
      ),
      schema_id=0,
      identifier_field_ids=[]
    )],
    current_schema_id=0,
    partition_specs=[PartitionSpec(spec_id=0)],
    default_spec_id=0,
    last_partition_id=999,
    properties={
      'owner': 'root',
      'write.format.default': 'parquet'
    },
    current_snapshot_id=8334458494559715805,
    snapshots=[
      Snapshot(
        snapshot_id=7910949481055846233,
        parent_snapshot_id=None,
        sequence_number=None,
        timestamp_ms=1662489306555,
        manifest_list='s3a://warehouse/wh/nyc.db/taxis/metadata/snap-7910949481055846233-1-3eb7a2e1-5b7a-4e76-a29a-3e29c176eea4.avro',
        summary=Summary(
          Operation.APPEND,
          **{
            'spark.app.id': 'local-1662489289173',
            'added-data-files': '1',
            'added-records': '2979431',
            'added-files-size': '46600777',
            'changed-partition-count': '1',
            'total-records': '2979431',
            'total-files-size': '46600777',
            'total-data-files': '1',
            'total-delete-files': '0',
            'total-position-deletes': '0',
            'total-equality-deletes': '0'
          }
        ),
        schema_id=0
      ),
    ],
    snapshot_log=[
      SnapshotLogEntry(
        snapshot_id='7910949481055846233',
        timestamp_ms=1662489306555
      )
    ],
    metadata_log=[
      MetadataLogEntry(
        metadata_file='s3a://warehouse/wh/nyc.db/taxis/metadata/00000-b58341ba-6a63-4eea-9b2f-e85e47c7d09f.metadata.json',
        timestamp_ms=1662489306555
      )
    ],
    sort_orders=[SortOrder(order_id=0)],
    default_sort_order_id=0,
    refs={
      'main': SnapshotRef(
        snapshot_id=8334458494559715805,
        snapshot_ref_type=SnapshotRefType.BRANCH,
        min_snapshots_to_keep=None,
        max_snapshot_age_ms=None,
        max_ref_age_ms=None
      )
    },
    format_version=2,
    last_sequence_number=1
  )
)
```

### Directly from a metadata file

To load a table directly from a metadata file (i.e., **without** using a catalog), you can use a `StaticTable` as follows:

```python
table = StaticTable.from_metadata(
    "s3a://warehouse/wh/nyc.db/taxis/metadata/00002-6ea51ce3-62aa-4197-9cf8-43d07c3440ca.metadata.json"
)
```

For the rest, this table behaves similarly as a table loaded using a catalog. Note that `StaticTable` is intended to be _read only_.

Any properties related to file IO can be passed accordingly:

```python
table = StaticTable.from_metadata(
    "s3a://warehouse/wh/nyc.db/taxis/metadata/00002-6ea51ce3-62aa-4197-9cf8-43d07c3440ca.metadata.json",
    {PY_IO_IMPL: "pyiceberg.some.FileIO.class"},
)
```

## Create a table

To create a table from a catalog:

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import TimestampType, DoubleType, StringType, NestedField

schema = Schema(
    NestedField(
        field_id=1, name="datetime", field_type=TimestampType(), required=False
    ),
    NestedField(field_id=2, name="bid", field_type=DoubleType(), required=False),
    NestedField(field_id=3, name="ask", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="symbol", field_type=StringType(), required=False),
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

sort_order = SortOrder(SortField(source_id=4, transform=IdentityTransform()))

catalog = load_catalog("prod")

catalog.create_table(
    identifier="default.bids",
    location="/Users/fokkodriesprong/Desktop/docker-spark-iceberg/wh/bids/",
    schema=schema,
    partition_spec=partition_spec,
    sort_order=sort_order,
)
```

Which returns a newly created table:

```python
Table(
    identifier=('default', 'bids'),
    metadata_location='/Users/fokkodriesprong/Desktop/docker-spark-iceberg/wh/bids//metadata/00000-c8cd93ab-f784-474d-a167-b1a86b05195f.metadata.json',
    metadata=TableMetadataV2(
        location='/Users/fokkodriesprong/Desktop/docker-spark-iceberg/wh/bids/',
        table_uuid=UUID('38d4cb39-4945-4bf2-b374-984b5c4984d2'),
        last_updated_ms=1661847562069,
        last_column_id=4,
        schemas=[
            Schema(
                NestedField(field_id=1, name='datetime', field_type=TimestampType(), required=False),
                NestedField(field_id=2, name='bid', field_type=DoubleType(), required=False),
                NestedField(field_id=3, name='ask', field_type=DoubleType(), required=False),
                NestedField(field_id=4, name='symbol', field_type=StringType(), required=False)),
                schema_id=1,
                identifier_field_ids=[])
        ],
        current_schema_id=1,
        partition_specs=[
            PartitionSpec(
                PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name='datetime_day'),))
        ],
        default_spec_id=0,
        last_partition_id=1000,
        properties={},
        current_snapshot_id=None,
        snapshots=[],
        snapshot_log=[],
        metadata_log=[],
        sort_orders=[
            SortOrder(order_id=1, fields=[SortField(source_id=4, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST)])
        ],
        default_sort_order_id=1,
        refs={},
        format_version=2,
        last_sequence_number=0
    )
)
```

## Query a table

To query a table, a table scan is needed. A table scan accepts a filter, columns and optionally a limit and a snapshot ID:

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
  "s3a://warehouse/wh/nyc/taxis/data/00003-4-42464649-92dd-41ad-b83b-dea1a2fe4b58-00001.parquet"
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
print(
    ray_dataset.take(2)
)
[
    {
        'VendorID': 2,
        'tpep_pickup_datetime': datetime.datetime(2008, 12, 31, 23, 23, 50, tzinfo=<UTC>),
        'tpep_dropoff_datetime': datetime.datetime(2009, 1, 1, 0, 34, 31, tzinfo=<UTC>)
    },
    {
        'VendorID': 2,
        'tpep_pickup_datetime': datetime.datetime(2008, 12, 31, 23, 5, 3, tzinfo=<UTC>),
        'tpep_dropoff_datetime': datetime.datetime(2009, 1, 1, 16, 10, 18, tzinfo=<UTC>)
    }
]
```
