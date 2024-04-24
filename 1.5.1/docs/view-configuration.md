---
title: "Configuration"
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

# Configuration

## View properties

Iceberg views support properties to configure view behavior. Below is an overview of currently available view properties.


| Property                                   | Default | Description                                                                        |
|--------------------------------------------|---------|------------------------------------------------------------------------------------|
| write.metadata.compression-codec           | gzip    | Metadata compression codec: `none` or `gzip`                                       |
| version.history.num-entries                | 10      | Controls the number of `versions` to retain                                        |
| replace.drop-dialect.allowed               | false   | Controls whether a SQL dialect is allowed to be dropped during a replace operation |


### View behavior properties


| Property                            | Default             | Description                                                        |
|-------------------------------------|---------------------|--------------------------------------------------------------------|
| commit.retry.num-retries            | 4                   | Number of times to retry a commit before failing                   |
| commit.retry.min-wait-ms            | 100                 | Minimum time in milliseconds to wait before retrying a commit      |
| commit.retry.max-wait-ms            | 60000 (1 min)       | Maximum time in milliseconds to wait before retrying a commit      |
| commit.retry.total-timeout-ms       | 1800000 (30 min)    | Total retry timeout period in milliseconds for a commit            |

