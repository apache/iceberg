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

Much of the python api conforms to the java api. You can get more info about the java api [here](../api).

## Catalog

The Catalog interface, like java provides search and management operations for tables.

To create a catalog:

``` python
from iceberg.hive import HiveTables

# instantiate Hive Tables
conf = {"hive.metastore.uris": 'thrift://{hms_host}:{hms_port}',
        "hive.metastore.warehouse.dir": {tmpdir} }
tables = HiveTables(conf)
```

and to create a table from a catalog:

``` python
from iceberg.api.schema import Schema\
from iceberg.api.types import TimestampType, DoubleType, StringType, NestedField
from iceberg.api.partition_spec import PartitionSpecBuilder

schema = Schema(NestedField.optional(1, "DateTime", TimestampType.with_timezone()),
                NestedField.optional(2, "Bid", DoubleType.get()),
                NestedField.optional(3, "Ask", DoubleType.get()),
                NestedField.optional(4, "symbol", StringType.get()))
partition_spec = PartitionSpecBuilder(schema).add(1, 1000, "DateTime_day", "day").build()

tables.create(schema, "test.test_123", partition_spec)
```


## Tables

The Table interface provides access to table metadata

+ schema returns the current table `Schema`
+ spec returns the current table `PartitonSpec`
+ properties returns a map of key-value `TableProperties`
+ currentSnapshot returns the current table `Snapshot`
+ snapshots returns all valid snapshots for the table
+ snapshot(id) returns a specific snapshot by ID
+ location returns the table’s base location

Tables also provide refresh to update the table to the latest version.

### Scanning
Iceberg table scans start by creating a `TableScan` object with `newScan`.

``` python
scan = table.new_scan();
```

To configure a scan, call filter and select on the `TableScan` to get a new `TableScan` with those changes.

``` python
filtered_scan = scan.filter(Expressions.equal("id", 5))
```

String expressions can also be passed to the filter method.

``` python
filtered_scan = scan.filter("id=5")
```

`Schema` projections can be applied against a `TableScan` by passing a list of column names.

``` python
filtered_scan = scan.select(["col_1", "col_2", "col_3"])
```

Because some data types cannot be read using the python library, a convenience method for excluding columns from projection is provided.

``` python
filtered_scan = scan.select_except(["unsupported_col_1", "unsupported_col_2"])
```


Calls to configuration methods create a new `TableScan` so that each `TableScan` is immutable.

When a scan is configured, `planFiles`, `planTasks`, and `Schema` are used to return files, tasks, and the read projection.

``` python
scan = table.new_scan() \
    .filter("id=5") \
    .select(["id", "data"])

projection = scan.schema
for task in scan.plan_tasks():
    print(task)
```

## Types

Iceberg data types are located in `iceberg.api.types.types`

### Primitives

Primitive type instances are available from static methods in each type class. Types without parameters use `get`, and types like `DecimalType` use factory methods:

```python
IntegerType.get()    # int
DoubleType.get()     # double
DecimalType.of(9, 2) # decimal(9, 2)
```

### Nested types
Structs, maps, and lists are created using factory methods in type classes.

Like struct fields, map keys or values and list elements are tracked as nested fields. Nested fields track [field IDs](https://iceberg.apache.org/evolution/#correctness) and nullability.

Struct fields are created using `NestedField.optional` or `NestedField.required`. Map value and list element nullability is set in the map and list factory methods.

```python
# struct<1 id: int, 2 data: optional string>
struct = StructType.of([NestedField.required(1, "id", IntegerType.get()),
                        NestedField.optional(2, "data", StringType.get()])
  )
```
```python
# map<1 key: int, 2 value: optional string>
map_var = MapType.of_optional(1, IntegerType.get(),
                          2, StringType.get())
```
```python
# array<1 element: int>
list_var = ListType.of_required(1, IntegerType.get());
```

## Expressions
Iceberg’s `Expressions` are used to configure table scans. To create `Expressions`, use the factory methods in `Expressions`.

Supported `Predicate` expressions are:

+ `is_null`
+ `not_null`
+ `equal`
+ `not_equal`
+ `less_than`
+ `less_than_or_equal`
+ `greater_than`
+ `greater_than_or_equal`

Supported expression `Operations`are:

+ `and`
+ `or`
+ `not`

Constant expressions are:

+ `always_true`
+ `always_false`
