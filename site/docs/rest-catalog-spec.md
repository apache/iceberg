---
title: "REST Catalog Spec"
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

## REST Catalog API Specification

Iceberg defines a REST-based Catalog API for managing table metadata and performing catalog operations. You can find the OpenAPI specification here:
[REST Catalog OpenAPI YAML](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).

You can also explore the API interactively using the [Swagger UI](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml).

## REST Catalog Protocol

As the Iceberg project grew to support more languages and engines, pluggable catalogs started to cause some practical problems. Catalogs needed to be implemented in multiple languages and it proved difficult for commercial offerings to support many different catalogs and clients.

To solve compatibility problems and pave a path for new features, the community created the REST catalog protocol, a common API (using the OpenAPI spec) for interacting with any Iceberg catalog. This is analogous to Hive's thrift protocol for HMS.

The REST protocol is important for several reasons:

- **Language and Engine Compatibility**: New languages and engines can support any catalog with just one client implementation.
- **Improved Reliability**: It uses change-based commits to enable server-side deconfliction and retries — fewer failures!
- **Simplified Metadata Management**: Metadata version upgrades are easier because root metadata is written by the catalog service.
- **Advanced Features**: It enables new features such as lazy snapshot loading, multi-table commits, and caching.
- **Security**: The protocol supports secure table sharing using credential vending or remote signing.

You can use the REST catalog protocol with any built-in catalog using translation in the `CatalogHandlers` class, or using the community maintained [`iceberg-rest-fixture`](https://hub.docker.com/r/apache/iceberg-rest-fixture) docker image.
