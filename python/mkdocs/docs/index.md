---
hide:
  - navigation
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

# PyIceberg

PyIceberg is a Python implementation for accessing Iceberg tables, without the need of a JVM.

## Install

You can install the latest release version from pypi:

```sh
pip3 install "pyiceberg[s3fs,hive]"
```

Install it directly for Github (not recommended), but sometimes handy:

```
pip install "git+https://github.com/apache/iceberg.git#subdirectory=python&egg=pyiceberg[s3fs]"
```

Or clone the repository for local development:

```sh
git clone https://github.com/apache/iceberg.git
cd iceberg/python
pip3 install -e ".[s3fs,hive]"
```

You can mix and match optional dependencies depending on your needs:

| Key      | Description:                                                         |
| -------- | -------------------------------------------------------------------- |
| hive     | Support for the Hive metastore                                       |
| glue     | Support for AWS Glue                                                 |
| dynamodb | Support for AWS DynamoDB                                             |
| pyarrow  | PyArrow as a FileIO implementation to interact with the object store |
| pandas   | Installs both PyArrow and Pandas                                     |
| duckdb   | Installs both PyArrow and DuckDB                                     |
| ray      | Installs PyArrow, Pandas, and Ray                                    |
| s3fs     | S3FS as a FileIO implementation to interact with the object store    |
| adlfs    | ADLFS as a FileIO implementation to interact with the object store   |
| snappy   | Support for snappy Avro compression                                  |

You either need to install `s3fs`, `adlfs` or `pyarrow` for fetching files.

There is both a [CLI](cli.md) and [Python API](api.md) available.
