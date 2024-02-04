---
title: "Iceberg Catalogs"
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

# Iceberg Catalogs

## Overview

You may think of Iceberg as a format for managing data in a single table, but the Iceberg library needs a way to keep track of those tables by name. Tasks like creating, dropping, and renaming tables are the responsibility of a catalog. Catalogs manage a collection of tables that are usually grouped into namespaces. The most important responsibility of a catalog is tracking a table's current metadata, which is provided by the catalog when you load a table.

The first step when using an Iceberg client is almost always initializing and configuring a catalog. The configured catalog is then used by compute engines to execute catalog operations. Multiple types of compute engines using a shared Iceberg catalog allows them to share a common data layer. 

A catalog is almost always configured through the processing engine which passes along a set of properties during initialization. Different processing engines have different ways to configure a catalog. When configuring a catalog, it’s always best to refer to the [Iceberg documentation](docs/latest/configuration.md#catalog-properties) as well as the docs for the specific processing engine being used. Ultimately, these configurations boil down to a common set of catalog properties that will be passed to configure the Iceberg catalog.

## Catalog Implementations

Iceberg catalogs are flexible and can be implemented using almost any backend system. They can be plugged into any Iceberg runtime, and allow any processing engine that supports Iceberg to load the tracked Iceberg tables. Iceberg also comes with a number of catalog implementations that are ready to use out of the box.

This includes:

* REST: a server-side catalog that’s exposed through a REST API
* Hive Metastore: tracks namespaces and tables using a Hive metastore
* JDBC: tracks namespaces and tables in a simple JDBC database
* Nessie: a transactional catalog that tracks namespaces and tables in a database with git-like version control

There are more catalog types in addition to the ones listed here as well as custom catalogs that are developed to include specialized functionality.

## Decoupling Using the REST Catalog

The REST catalog was introduced in the Iceberg 0.14.0 release and provides greater control over how Iceberg catalogs are implemented. Instead of using technology-specific logic contained in the catalog clients, the implementation details of a REST catalog lives on the catalog server. If you’re familiar with Hive, this is somewhat similar to the Hive thrift service that allows access to a hive server over a single port. The server-side logic can be written in any language and use any custom technology, as long as the API follows the [Iceberg REST Open API specification](https://github.com/apache/iceberg/blob/master/open-api/rest-catalog-open-api.yaml).

A great benefit of the REST catalog is that it allows you to use a single client to talk to any catalog backend. This increased flexibility makes
it easier to make custom catalogs compatible with engines like Athena or Starburst without requiring the inclusion of a Jar into the classpath.
