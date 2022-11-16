---
title: "REST Catalog"
url: rest-catalog
weight: 400
menu: main
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

# REST Catalog

In Iceberg, catalogs are used to atomically track the metadata files in tables.
Iceberg support a veriaty of catalog implementations, such as Hive Metastore, AWS Glue and Project Nessie. 

In the past, every time a new catalog was supported it required re-implementing many interfaces and supporting libraries.
The REST catalog creates a universal interface to simplify existing catalog solutions.

The REST catalog is based on the [Open API specification](https://spec.openapis.org/oas/latest.html). 
Following this specification, the Apache Iceberg repository contains a [YAML file that specifies the REST Catalog](https://github.com/apache/iceberg/blob/master/open-api/rest-catalog-open-api.yaml).
This allows anyone to implement their own catalog as a REST API. 
So as new catalog implementations are created, if you follow the API specification, you won’t need to create a new connector, since any engine that supports the REST API catalog implementation can also use the new catalog.

The REST catalog specification allows a server to own all of the catalog implementation details, while exposing a predictable REST API for Iceberg clients to connect to.


