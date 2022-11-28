<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# PyIceberg

Much of the python api conforms to the Java API. You can get more info about the java api [here](https://iceberg.apache.org/docs/latest/java-api-quickstart/).

## Installing

You can install the latest release version from pypi:

```sh
pip3 install "pyiceberg[s3fs,hive]"
```

Or install the latest development version locally:

```sh
git clone https://github.com/apache/iceberg.git
cd iceberg/python
pip3 install -e ".[s3fs,hive]"
```

You can mix and match optional dependencies:

| Key       | Description:                                                         |
|-----------|----------------------------------------------------------------------|
| hive      | Support for the Hive metastore                                       |
| glue      | Support for AWS Glue                                                 |
| pyarrow   | PyArrow as a FileIO implementation to interact with the object store |
| s3fs      | S3FS as a FileIO implementation to interact with the object store    |
| snappy    | Support for snappy Avro compression                                  |

# Python CLI Quickstart

Pyiceberg comes with a CLI that's available after installing the `pyiceberg` package.

```sh
➜  pyiceberg --help
Usage: pyiceberg [OPTIONS] COMMAND [ARGS]...

Options:
--catalog TEXT
--verbose BOOLEAN
--output [text|json]
--uri TEXT
--credential TEXT
--help                Show this message and exit.

Commands:
describe    Describes a namespace xor table
drop        Operations to drop a namespace or table
list        Lists tables or namespaces
location    Returns the location of the table
properties  Properties on tables/namespaces
rename      Renames a table
schema      Gets the schema of the table
spec        Returns the partition spec of the table
uuid        Returns the UUID of the table
```

# Configuration

There are three ways of setting the configuration.

For the CLI you can pass it in using `--uri` and `--credential` and it will automatically detect the type based on the scheme (`http(s)` for rest, `thrift` for Hive).

Secondly, YAML based configuration is supported `cat ~/.pyiceberg.yaml`:

```yaml
catalog:
  default:
    uri: thrift://localhost:9083
    s3.endpoint: http://localhost:9000
    s3.access-key-id: admin
    s3.secret-access-key: password

  rest:
    uri: http://rest-catalog/ws/
    credential: t-1234:secret

  mtls-secured-catalog:
    uri: https://rest-catalog/ws/
    ssl:
      client:
        cert: /absolute/path/to/client.crt
        key: /absolute/path/to/client.key
      cabundle: /absolute/path/to/cabundle.pem

  glue:
    type: glue
```

Lastly, you can also set it using environment variables:

```sh
export PYICEBERG_CATALOG__DEFAULT__URI=thrift://localhost:9083

export PYICEBERG_CATALOG__REST__URI=http://rest-catalog/ws/
export PYICEBERG_CATALOG__REST__CREDENTIAL=t-1234:secret

export PYICEBERG_CATALOG__GLUE__TYPE=glue
```

Where the structure is equivalent to the YAML. The levels are separated using a double underscore (`__`).

If you want to use AWS Glue as the catalog, you can use the last two ways to configure the pyiceberg and refer
[How to configure AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) to set your AWS account credentials locally.

## FileIO configuration

For the FileIO there are several configuration options available:

| Key                  | Example             | Description                                                                                                                                                                                                                                               |
|----------------------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| s3.endpoint          | https://10.0.19.25/ | Configure an alternative endpoint of the S3 service for the FileIO to access. This could be used to use S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. |
| s3.access-key-id     | admin               | Configure the static secret access key used to access the FileIO.                                                                                                                                                                                         |
| s3.secret-access-key | password            | Configure the static session token used to access the FileIO.                                                                                                                                                                                             |
| s3.signer            | bearer              | Configure the signature version of the FileIO.                                                                                                                                                                                                            |

# CLI Quickstart

This example assumes that you have a default catalog set. If you want to load another catalog, for example, the rest example above. Then you need to set `--catalog rest`.

```sh
➜  pyiceberg list
default
nyc
```

```sh
➜  pyiceberg list nyc
nyc.taxis
```

```sh
➜  pyiceberg describe nyc.taxis
Table format version  1
Metadata location     file:/.../nyc.db/taxis/metadata/00000-aa3a3eac-ea08-4255-b890-383a64a94e42.metadata.json
Table UUID            6cdfda33-bfa3-48a7-a09e-7abb462e3460
Last Updated          1661783158061
Partition spec        []
Sort order            []
Current schema        Schema, id=0
├── 1: VendorID: optional long
├── 2: tpep_pickup_datetime: optional timestamptz
├── 3: tpep_dropoff_datetime: optional timestamptz
├── 4: passenger_count: optional double
├── 5: trip_distance: optional double
├── 6: RatecodeID: optional double
├── 7: store_and_fwd_flag: optional string
├── 8: PULocationID: optional long
├── 9: DOLocationID: optional long
├── 10: payment_type: optional long
├── 11: fare_amount: optional double
├── 12: extra: optional double
├── 13: mta_tax: optional double
├── 14: tip_amount: optional double
├── 15: tolls_amount: optional double
├── 16: improvement_surcharge: optional double
├── 17: total_amount: optional double
├── 18: congestion_surcharge: optional double
└── 19: airport_fee: optional double
Current snapshot      Operation.APPEND: id=5937117119577207079, schema_id=0
Snapshots             Snapshots
└── Snapshot 5937117119577207079, schema 0: file:/.../nyc.db/taxis/metadata/snap-5937117119577207079-1-94656c4f-4c66-4600-a4ca-f30377300527.avro
Properties            owner                 root
write.format.default  parquet
```

Or output in JSON for automation:

```sh
➜  pyiceberg --output json describe nyc.taxis | jq
{
  "identifier": [
    "nyc",
    "taxis"
  ],
  "metadata_location": "file:/.../nyc.db/taxis/metadata/00000-aa3a3eac-ea08-4255-b890-383a64a94e42.metadata.json",
  "metadata": {
    "location": "file:/.../nyc.db/taxis",
    "table-uuid": "6cdfda33-bfa3-48a7-a09e-7abb462e3460",
    "last-updated-ms": 1661783158061,
    "last-column-id": 19,
    "schemas": [
      {
        "type": "struct",
        "fields": [
          {
            "id": 1,
            "name": "VendorID",
            "type": "long",
            "required": false
          },
...
          {
            "id": 19,
            "name": "airport_fee",
            "type": "double",
            "required": false
          }
        ],
        "schema-id": 0,
        "identifier-field-ids": []
      }
    ],
    "current-schema-id": 0,
    "partition-specs": [
      {
        "spec-id": 0,
        "fields": []
      }
    ],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "properties": {
      "owner": "root",
      "write.format.default": "parquet"
    },
    "current-snapshot-id": 5937117119577207000,
    "snapshots": [
      {
        "snapshot-id": 5937117119577207000,
        "timestamp-ms": 1661783158061,
        "manifest-list": "file:/.../nyc.db/taxis/metadata/snap-5937117119577207079-1-94656c4f-4c66-4600-a4ca-f30377300527.avro",
        "summary": {
          "operation": "append",
          "spark.app.id": "local-1661783139151",
          "added-data-files": "1",
          "added-records": "2979431",
          "added-files-size": "46600777",
          "changed-partition-count": "1",
          "total-records": "2979431",
          "total-files-size": "46600777",
          "total-data-files": "1",
          "total-delete-files": "0",
          "total-position-deletes": "0",
          "total-equality-deletes": "0"
        },
        "schema-id": 0
      }
    ],
    "snapshot-log": [
      {
        "snapshot-id": "5937117119577207079",
        "timestamp-ms": 1661783158061
      }
    ],
    "metadata-log": [],
    "sort-orders": [
      {
        "order-id": 0,
        "fields": []
      }
    ],
    "default-sort-order-id": 0,
    "refs": {
      "main": {
        "snapshot-id": 5937117119577207000,
        "type": "branch"
      }
    },
    "format-version": 1,
    "schema": {
      "type": "struct",
      "fields": [
        {
          "id": 1,
          "name": "VendorID",
          "type": "long",
          "required": false
        },
...
        {
          "id": 19,
          "name": "airport_fee",
          "type": "double",
          "required": false
        }
      ],
      "schema-id": 0,
      "identifier-field-ids": []
    },
    "partition-spec": []
  }
}
```

# Python API

To instantiate a catalog:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("prod")

catalog.list_namespaces()
```

Returns:

```
[('default',), ('nyc',)]
```

Listing the tables in the `nyc` namespace:

```python
catalog.list_tables("nyc")
```

Returns:

```
[('nyc', 'taxis')]
```

Loading the `taxis` table:

```python
catalog.load_table(("nyc", "taxis"))
```

```
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

And to create a table from a catalog:

```python
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

from pyiceberg.catalog.hive import HiveCatalog

catalog = HiveCatalog(name="prod", uri="thrift://localhost:9083/")

catalog.create_table(
    identifier="default.bids",
    location="/Users/fokkodriesprong/Desktop/docker-spark-iceberg/wh/bids/",
    schema=schema,
    partition_spec=partition_spec,
    sort_order=sort_order,
)
```

Which returns a newly created table:

```
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

# Feature Support

The goal is that the python library will provide a functional, performant subset of the Java library. The initial focus has been on reading table metadata and provide a convenient CLI to go through the catalog.

## Metadata

| Operation                | Java  | Python |
|:-------------------------|:-----:|:------:|
| Get Schema               |    X  |   X    |
| Get Snapshots            |    X  |   X    |
| Plan Scan                |    X  |   X    |
| Plan Scan for Snapshot   |    X  |   X    |
| Update Current Snapshot  |    X  |        |
| Create Table             |    X  |   X    |
| Rename Table             |    X  |   X    |
| Drop Table               |    X  |   X    |
| Alter Table              |    X  |        |
| Set Table Properties     |    X  |        |
| Create Namespace         |    X  |   X    |
| Drop Namespace           |    X  |   X    |
| Set Namespace Properties |    X  |   X    |

## Types

The types are kept in `pyiceberg.types`.

Primitive types:

- `BooleanType`
- `StringType`
- `IntegerType`
- `LongType`
- `FloatType`
- `DoubleType`
- `DateType`
- `TimeType`
- `TimestampType`
- `TimestamptzType`
- `BinaryType`
- `UUIDType`

Complex types:

- `StructType`
- `ListType`
- `MapType`
- `FixedType(16)`
- `DecimalType(8, 3)`
