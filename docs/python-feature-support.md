---
title: "Python Feature Support"
url: python-feature-support
aliases:
    - "python/feature-support"
menu:
    main:
        parent: "API"
        weight: 600
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

# Feature Support

The goal is that the python library will provide a functional, performant subset of the Java library. The initial focus has been on reading table metadata and provide a convenient CLI to go through the catalog.

## Feature Comparison

### Metadata

| Operation               | Java  | Python |
|:------------------------|:-----:|:------:|
| Get Schema              |    X  |   X    |
| Get Snapshots           |    X  |   X    |
| Plan Scan               |    X  |        |
| Plan Scan for Snapshot  |    X  |        |
| Update Current Snapshot |    X  |        |
| Set Table Properties    |    X  |   X    |
| Create Table            |    X  |   X    |
| Drop Table              |    X  |   X    |
| Alter Table             |    X  |        |


### Read Support

Pyarrow is used for reading parquet files, so read support is limited to what is currently supported in the pyarrow.parquet package.

### Write Support

The python client does not currently support write capability
