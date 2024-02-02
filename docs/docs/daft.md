---
title: "Daft"
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

# Daft

[Daft](www.getdaft.io) is a Python/Rust-based distributed query engine with a Python DataFrame API.

Iceberg supports reading of Iceberg tables into Daft DataFrames by using the Python client library [PyIceberg](https://py.iceberg.apache.org/).

For Python users, Daft is complementary to PyIceberg as a pure query engine layer:

* **PyIceberg:** catalog/table management tasks (e.g. creation of tables, modifying table schemas)
* **Daft:** querying tables (e.g. previewing tables, data ETL and analysis)

In database terms, PyIceberg is the Data Description Language (DDL) for database administration and Daft is the Data Manipulation Language (DML) for querying data.

## Enabling Iceberg support in Daft

To use Iceberg with Daft, simply ensure that the [PyIceberg](https://py.iceberg.apache.org/) library is also installed in your current Python environment.

```
pip install getdaft pyiceberg
```

## Querying Iceberg using Daft

### Reading PyIceberg tables

Daft interacts natively with [PyIceberg](https://py.iceberg.apache.org/) to read from Iceberg.

Simply load a PyIceberg table and pass it into Daft as follows:

``` py
import daft
from pyiceberg import load_catalog

table = load_catalog("my_catalog").load_table("my_namespace.my_table")
df = daft.read_iceberg(table)
df.show()
```

Any subsequent filter operations on the Daft `df` DataFrame object will be correctly optimized to take advantage of Iceberg features such as hidden partitioning and file-level statistics for efficient reads.

``` py
# Filter which takes advantage of partition pruning capabilities of Iceberg
df = df.where(df["partition_key"] < 1000)
df.show()
```

### Type compatibility

Daft and Iceberg have compatible type systems. Here are how types are converted across the two systems.

When reading from an Iceberg source into Daft:

| Iceberg | Daft |
|---------|------|
| **Primitive Types** |
| `boolean` | [`daft.DataType.bool()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.bool) |
| `int` | [`daft.DataType.int32()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.int32) |
| `long` | [`daft.DataType.int64()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.int64) |
| `float` | [`daft.DataType.float32()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.float32) |
| `double` | [`daft.DataType.float64()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.float64) |
| `decimal(precision, scale)` | [`daft.DataType.decimal128(precision, scale)`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.decimal128) |
| `date` | [`daft.DataType.date()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.date) |
| `time` | [`daft.DataType.int64()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.int64) |
| `timestamp` | [`daft.DataType.timestamp(timeunit="us", timezone=None)`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.timestamp) |
| `timestampz` | [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.timestamp) |
| `string` | [`daft.DataType.string()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.string) |
| `uuid` | [`daft.DataType.binary()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.binary) |
| `fixed(L)` | [`daft.DataType.binary()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.binary) |
| `binary` | [`daft.DataType.binary()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.binary) |
| **Nested Types** |
| `struct(**fields)` | [`daft.DataType.struct(**fields)`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.struct) |
| `list(child_type)` | [`daft.DataType.list(child_type)`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.list) |
| `map(K, V)` | [`daft.DataType.struct({"key": K, "value": V})`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.struct) |
