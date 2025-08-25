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

[Daft](https://www.daft.ai) is a distributed query engine written in Python and Rust, two fast-growing ecosystems in the data engineering and machine learning industry.

It exposes its flavor of the familiar [Python DataFrame API](https://docs.daft.ai/en/latest/api/dataframe/) which is a common abstraction over querying tables of data in the Python data ecosystem.

Daft DataFrames are a powerful interface to power use-cases across ML/AI training, batch inference, feature engineering and traditional analytics. Daft's tight integration with Iceberg unlocks novel capabilities for both traditional analytics and Pythonic ML workloads on your data catalog.

## Enabling Iceberg support in Daft

[PyIceberg](https://py.iceberg.apache.org/) supports reading of Iceberg tables into Daft DataFrames.

To use Iceberg with Daft, ensure that the [PyIceberg](https://py.iceberg.apache.org/) library is also installed in your current Python environment.

```
pip install daft pyiceberg
```

## Querying Iceberg using Daft

Daft interacts natively with [PyIceberg](https://py.iceberg.apache.org/) to read Iceberg tables.

### Reading Iceberg tables

**Setup Steps**

To follow along with this code, first create an Iceberg table following [the Spark Quickstart tutorial](https://iceberg.apache.org/spark-quickstart/). PyIceberg must then be correctly configured by ensuring that the `~/.pyiceberg.yaml` file contains an appropriate catalog entry:

```
catalog:
  default:
    # URL to the Iceberg REST server Docker container
    uri: http://localhost:8181
    # URL and credentials for the MinIO Docker container
    s3.endpoint: http://localhost:9000
    s3.access-key-id: admin
    s3.secret-access-key: password
```

Here is how the Iceberg table `demo.nyc.taxis` can be loaded into Daft:

``` py
import daft
from pyiceberg.catalog import load_catalog

# Configure Daft to use the local MinIO Docker container for any S3 operations
daft.set_planning_config(
    default_io_config=daft.io.IOConfig(
        s3=daft.io.S3Config(endpoint_url="http://localhost:9000"),
    )
)

# Load a PyIceberg table into Daft, and show the first few rows
table = load_catalog("default").load_table("nyc.taxis")
df = daft.read_iceberg(table)
df.show()
```

```
╭───────────┬─────────┬───────────────┬─────────────┬────────────────────╮
│ vendor_id ┆ trip_id ┆ trip_distance ┆ fare_amount ┆ store_and_fwd_flag │
│ ---       ┆ ---     ┆ ---           ┆ ---         ┆ ---                │
│ Int64     ┆ Int64   ┆ Float32       ┆ Float64     ┆ Utf8               │
╞═══════════╪═════════╪═══════════════╪═════════════╪════════════════════╡
│ 1         ┆ 1000371 ┆ 1.8           ┆ 15.32       ┆ N                  │
├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         ┆ 1000374 ┆ 8.4           ┆ 42.13       ┆ Y                  │
├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2         ┆ 1000372 ┆ 2.5           ┆ 22.15       ┆ N                  │
├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2         ┆ 1000373 ┆ 0.9           ┆ 9.01        ┆ N                  │
╰───────────┴─────────┴───────────────┴─────────────┴────────────────────╯

(Showing first 4 of 4 rows)
```

Note that the operation above will produce a warning from PyIceberg that "no partition filter was specified" and that "this will result in a full table scan". Any filter operations on the Daft dataframe, `df`, will [push down the filters](https://iceberg.apache.org/docs/latest/performance/#data-filtering), correctly account for [hidden partitioning](https://iceberg.apache.org/docs/latest/partitioning/), and utilize [table statistics to inform query planning](https://iceberg.apache.org/docs/latest/performance/#scan-planning) for efficient reads.

Let's try the above query again, but this time with a filter applied on the table's partition column `"vendor_id"` which Daft will correctly use to elide a full table scan.

``` py
df = df.where(df["vendor_id"] > 1)
df.show()
```

```
╭───────────┬─────────┬───────────────┬─────────────┬────────────────────╮
│ vendor_id ┆ trip_id ┆ trip_distance ┆ fare_amount ┆ store_and_fwd_flag │
│ ---       ┆ ---     ┆ ---           ┆ ---         ┆ ---                │
│ Int64     ┆ Int64   ┆ Float32       ┆ Float64     ┆ Utf8               │
╞═══════════╪═════════╪═══════════════╪═════════════╪════════════════════╡
│ 2         ┆ 1000372 ┆ 2.5           ┆ 22.15       ┆ N                  │
├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2         ┆ 1000373 ┆ 0.9           ┆ 9.01        ┆ N                  │
╰───────────┴─────────┴───────────────┴─────────────┴────────────────────╯

(Showing first 2 of 2 rows)
```

### Type compatibility

Daft and Iceberg have compatible type systems. Here are how types are converted across the two systems.


| Iceberg | Daft |
|---------|------|
| **Primitive Types** |
| `boolean` | [`daft.DataType.bool()`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.bool) |
| `int` | [`daft.DataType.int32()`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.int32) |
| `long` | [`daft.DataType.int64()`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.int64) |
| `float` | [`daft.DataType.float32()`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.float32) |
| `double` | [`daft.DataType.float64()`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.float64) |
| `decimal(precision, scale)` | [`daft.DataType.decimal128(precision, scale)`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.decimal128) |
| `date` | [`daft.DataType.date()`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.date) |
| `time` | [`daft.DataType.time(timeunit="us")`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.int64) |
| `timestamp` | [`daft.DataType.timestamp(timeunit="us", timezone=None)`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.timestamp) |
| `timestampz` | [`daft.DataType.timestamp(timeunit="us", timezone="UTC")`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.timestamp) |
| `string` | [`daft.DataType.string()`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.string) |
| `uuid` | [`daft.DataType.binary()`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.binary) |
| `fixed(L)` | [`daft.DataType.binary()`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.fixed_size_binary) |
| `binary` | [`daft.DataType.binary()`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.binary) |
| **Nested Types** |
| `struct(**fields)` | [`daft.DataType.struct(**fields)`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.struct) |
| `list(child_type)` | [`daft.DataType.list(child_type)`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.list) |
| `map(K, V)` | [`daft.DataType.map(K, V)`](https://docs.daft.ai/en/latest/api/datatypes/#daft.datatype.DataType.map) |
