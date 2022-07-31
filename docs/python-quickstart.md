---
title: "Python Quickstart"
url: python-quickstart
aliases:
    - "python/quickstart"
menu:
    main:
        parent: "API"
        weight: 400
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


# Python API Quickstart

## Installation

Iceberg python is currently in development, for development and testing purposes the best way to install the library is to perform the following steps:
```
git clone https://github.com/apache/iceberg.git
cd iceberg/python
pip install -e .
```

## Testing
Testing is done using tox. The config can be found in `tox.ini` within the python directory of the iceberg project.

```
# simply run tox from within the python dir
tox
```

# Examples

## Inspect Table Metadata
``` python

from iceberg.hive import HiveTables

# instantiate Hive Tables
conf = {"hive.metastore.uris": 'thrift://{hms_host}:{hms_port}'}
tables = HiveTables(conf)

# load table
tbl = tables.load("iceberg_db.iceberg_test_table")

# inspect metadata
print(tbl.schema())
print(tbl.spec())
print(tbl.location())

# get table level record count
from pprint import pprint
pprint(int(tbl.current_snapshot().summary.get("total-records")))
```
