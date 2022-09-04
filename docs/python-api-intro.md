---
title: "Python API"
url: python-api-intro
aliases:
    - "python/api-intro"
menu:
    main:
        parent: "API"
        weight: 500
---
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

# Iceberg Python API

Much of the python api conforms to the Java API. You can get more info about the java api [here](../api).

## Instal

You can install the latest release version from pypi:

```sh
pip3 install "pyiceberg[s3fs,hive]"
```

Or install the latest development version locally:

```
pip3 install poetry --upgrade
pip3 install -e ".[s3fs,hive]"
```

With optional dependencies:

| Key       | Description:                                                          |
|-----------|-----------------------------------------------------------------------|
| hive      | Support for the Hive metastore                                        |
| pyarrow   | PyArrow as a FileIO implementation to interact with the object store  |
| s3fs      | S3FS as a FileIO implementation to interact with the object store     |
| zstandard | Support for zstandard Avro compresssion                               |
| snappy    | Support for snappy Avro compresssion                                  |

## Catalog

To instantiate a catalog:

``` python
>>> from pyiceberg.catalog.hive import HiveCatalog
>>> catalog = HiveCatalog(name='prod', uri='thrift://localhost:9083/')

>>> catalog.list_namespaces()
[('default',), ('nyc',)]

>>> catalog.list_tables('nyc')
[('nyc', 'taxis')]

>>> catalog.load_table(('nyc', 'taxis'))
Table(identifier=('nyc', 'taxis'), ...)
```

And to create a table from a catalog:

``` python
from pyiceberg.schema import Schema
from pyiceberg.types import TimestampType, DoubleType, StringType, NestedField

schema = Schema(
    NestedField(field_id=1, name="datetime", field_type=TimestampType(), required=False),
    NestedField(field_id=2, name="bid", field_type=DoubleType(), required=False),
    NestedField(field_id=3, name="ask", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="symbol", field_type=StringType(), required=False),
)

from pyiceberg.table.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

partition_spec = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day")
)

from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

sort_order = SortOrder(
    SortField(source_id=4, transform=IdentityTransform())
)

from pyiceberg.catalog.hive import HiveCatalog
catalog = HiveCatalog(name='prod', uri='thrift://localhost:9083/')

catalog.create_table(
    identifier='default.bids',
    location='/Users/fokkodriesprong/Desktop/docker-spark-iceberg/wh/bids/',
    schema=schema,
    partition_spec=partition_spec,
    sort_order=sort_order
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

## Types

The types are located in `pyiceberg.types`.

Primitive types:

- BooleanType
- StringType
- IntegerType
- LongType
- FloatType
- DoubleType
- DateType
- TimeType
- TimestampType
- TimestamptzType
- BinaryType
- UUIDType

Complex types:

- StructType
- ListType
- MapType
- FixedType(16)
- DecimalType(8, 3)
