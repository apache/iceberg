<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Snowflake Integration Tests

Runs integration tests against an actual snowflake deployment. 

## Setup

Currently, the test suite uses both jdbc and the Catalog to interact with the Snowflake deployment. This is required as
Catalog doesn't support write operations as of now. Iceberg tables are a private preview feature in Snowflake, 
so the user need to contact support to enable it. 

The test suite would need following environment variables to be set:
- `SNOW_URI`: [Required] JDBC driver URI, e.g. `jdbc:snowflake://https://account.snowflakecomputing.com:8085/`
- `SNOW_TEST_DB_NAME`: [Required] Test database to create, e.g. `Test_db`
- `SNOW_USER`: [Optional] JDBC user, could be specified as part of uri as well, e.g. `admin`
- `SNOW_PASSWORD`: [Optional] JDBC password, could be specified as part of uri as well, e.g. `password`
- `SNOW_WAREHOUSE`: [Optional] Snowflake warehouse name, could be specified as part of uri as well, e.g. `my_warehouse`

