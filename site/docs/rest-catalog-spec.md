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

## Testing REST Catalog Implementations

### REST Compatibility Kit (RCK)

Apache Iceberg provides a comprehensive REST Compatibility Kit (RCK) to validate REST catalog implementations against the full specification. The RCK ensures your catalog correctly implements all required endpoints and behaviors.

### Quick Start with the REST Compatibility Kit Runner

The easiest way to test your REST catalog implementation is using the `rest-ckit-runner.sh` script, which provides a zero-setup testing experience.

#### Download and Run Directly from GitHub

You can download and run the script directly without cloning the repository:

```bash
# Download the script
curl -fsSL https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-ckit-runner.sh -o rest-ckit-runner.sh

# Make it executable
chmod +x rest-ckit-runner.sh

# Run immediately
./rest-ckit-runner.sh --iceberg.rest.catalog.uri=http://localhost:8181
```

Or as a one-liner for quick testing:

```bash
# Download and run in one command
curl -fsSL https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-ckit-runner.sh | bash -s -- --iceberg.rest.catalog.uri=http://localhost:8181
```

#### Basic Usage Examples

```bash
# Basic usage - test your catalog immediately
./rest-ckit-runner.sh --iceberg.rest.catalog.uri=http://localhost:8181

# With authentication
./rest-ckit-runner.sh \
  --iceberg.rest.catalog.uri=https://your-catalog.com \
  --iceberg.rest.auth.token=your-bearer-token
  --iceberg.rest.catalog.warehouse=gs://your-bucket/warehouse

# Test with different Iceberg versions
ICEBERG_VERSION=1.9.0 ./rest-ckit-runner.sh \
  --iceberg.rest.catalog.uri=http://localhost:8181
```

### Supported Authentication Methods

The RCK runner supports all standard REST catalog authentication methods:

```bash
# Bearer token authentication
--iceberg.rest.auth.token=your-token

# Basic authentication
--iceberg.rest.auth.basic=username:password

# OAuth2 token
--iceberg.rest.oauth2.token=oauth-token

# Google Auth
--iceberg.rest.oauth2.type=google
```

### What the RCK Tests

The REST Compatibility Kit validates your catalog implementation against:

- **Namespace Operations**: Create, list, update, and delete namespaces
- **Table Lifecycle**: Create, alter, rename, and drop tables
- **Schema Evolution**: Add, rename, reorder, and drop columns
- **Partition Management**: Create and update partition specifications
- **Metadata Operations**: Table properties, location updates, and statistics
- **Transaction Handling**: Multi-operation commits and conflict resolution
- **Error Handling**: Proper HTTP status codes and error responses
- **Authentication**: Token validation and authorization checks

### Getting Help

For complete usage information and examples:

```bash
./rest-ckit-runner.sh --help
```

This provides comprehensive documentation including all configuration options, environment variables, and troubleshooting guidance.
