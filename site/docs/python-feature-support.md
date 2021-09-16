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

The goal is that the python library will provide a functional, performant subset of the java library. The initial focus has been on reading table metadata as well as providing the capability to both plan and execute a scan.

## Feature Comparison

### Metadata

| Operation               | Java  | Python |
|:------------------------|:-----:|:------:|
| Get Schema              |    X  |    X   |
| Get Snapshots           |    X  |    X   |
| Plan Scan               |    X  |    X   |
| Plan Scan for Snapshot  |    X  |    X   |
| Update Current Snapshot |    X  |        |
| Set Table Properties    |    X  |        |
| Create Table            |    X  |    X   |
| Drop Table              |    X  |    X   |
| Alter Table             |    X  |        |


### Read Support

Pyarrow is used for reading parquet files, so read support is limited to what is currently supported in the pyarrow.parquet package.

#### Primitive Types


| Data Type               | Java | Python |
|:------------------------|:----:|:------:|
| BooleanType             |   X  |    X   |
| DateType                |   X  |    X   |
| DecimalType             |   X  |    X   |
| FloatType               |   X  |    X   |
| IntegerType             |   X  |    X   |
| LongType                |   X  |    X   |
| TimeType                |   X  |    X   |
| TimestampType           |   X  |    X   |

#### Nested Types

| Data Type               | Java | Python |
|:------------------------|:----:|:------:|
| ListType of primitives  |   X  |    X   |
| MapType of primitives   |   X  |    X   |
| StructType of primitives|   X  |    X   |
| ListType of Nested Types|   X  |        |
| MapType of Nested Types |   X  |        |

### Write Support

The python client does not currently support write capability
