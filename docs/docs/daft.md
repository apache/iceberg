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

For Python users, Daft is complementary to PyIceberg as a query engine layer:

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

table = load_catalog("my_catalog").load_table("my_tpch_namespace.lineitem")
df = daft.read_iceberg(table)
df = df.select("L_SHIPDATE", "L_ORDERKEY", "L_COMMENT")
df.show()
```

```
╭────────────┬────────────┬────────────────────────────────╮
│ L_SHIPDATE ┆ L_ORDERKEY ┆ L_COMMENT                      │
│ ---        ┆ ---        ┆ ---                            │
│ Date       ┆ Int64      ┆ Utf8                           │
╞════════════╪════════════╪════════════════════════════════╡
│ 1992-01-02 ┆ 2186280097 ┆ ions sleep about the si        │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1992-01-02 ┆ 175366628  ┆ gular accoun                   │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1992-01-02 ┆ 2186602151 ┆ blithely even                  │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1992-01-02 ┆ 3937663654 ┆ ake boldly among the ideas. s… │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1992-01-02 ┆ 2186781220 ┆ thely. slyly pending ideas ar… │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1992-01-02 ┆ 3937999493 ┆  haggle at the regular, pen    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1992-01-02 ┆ 2186933061 ┆ ickly. slyly                   │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1992-01-02 ┆ 3938167204 ┆ carefully silent instructions… │
╰────────────┴────────────┴────────────────────────────────╯

(Showing first 8 rows)
```

Any subsequent filter operations on the Daft `df` DataFrame object will be correctly optimized to take advantage of Iceberg features such as hidden partitioning and file-level statistics for efficient reads.

``` py
import datetime

# Filter which takes advantage of partition pruning capabilities of Iceberg
df = df.where(df["L_SHIPDATE"] > datetime.date(1993, 1, 1))
df.show()
```

```
╭────────────┬────────────┬────────────────────────────────╮
│ L_SHIPDATE ┆ L_ORDERKEY ┆ L_COMMENT                      │                                                                        
│ ---        ┆ ---        ┆ ---                            │
│ Date       ┆ Int64      ┆ Utf8                           │
╞════════════╪════════════╪════════════════════════════════╡
│ 1993-01-02 ┆ 5695313125 ┆  slyly special p               │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1993-01-02 ┆ 2701326853 ┆ ironic instru                  │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1993-01-02 ┆ 5695313766 ┆ ly according                   │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1993-01-02 ┆ 2701330720 ┆ y alongside of the blithely    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1993-01-02 ┆ 5695315200 ┆ ckly final foxes haggle car    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1993-01-02 ┆ 2701331524 ┆ ns doze slyly pending instruc… │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1993-01-02 ┆ 5695317377 ┆ re about the ironic, silen     │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1993-01-02 ┆ 2701342819 ┆ fully even pinto beans wa      │
╰────────────┴────────────┴────────────────────────────────╯

(Showing first 8 rows)
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
| `time` | [`daft.DataType.time(timeunit="us")`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.int64) |
| `timestamp` | [`daft.DataType.timestamp(timeunit="us", timezone=None)`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.timestamp) |
| `timestampz` | [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.timestamp) |
| `string` | [`daft.DataType.string()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.string) |
| `uuid` | [`daft.DataType.binary()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.binary) |
| `fixed(L)` | [`daft.DataType.binary()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.binary) |
| `binary` | [`daft.DataType.binary()`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.binary) |
| **Nested Types** |
| `struct(**fields)` | [`daft.DataType.struct(**fields)`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.struct) |
| `list(child_type)` | [`daft.DataType.list(child_type)`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.list) |
| `map(K, V)` | [`daft.DataType.map(K, V)`](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html#daft.DataType.map) |
