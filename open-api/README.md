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

# Open API spec

The `rest-catalog-open-api.yaml` defines the REST catalog interface.

## Lint

To make sure that the open-api definition is valid, you can run the `lint` command:

```sh
make install
make lint
```

## Generate Python code

When updating `rest-catalog-open-api.yaml`, make sure to update `rest-catalog-open-api.py` with the spec changes by running the following commands:

```sh
make install
make generate
```

The generated code is not being used in the project, but helps to see what the changes in the open-API definition are in the generated code.

# REST Compatibility Kit (RCK)

The REST Compatibility Kit (RCK) is a Technology Compatibility Kit (TCK) implementation for the 
Iceberg REST Specification.  This includes a series of tests based on the Java reference
implementation of the REST Catalog that can be executed against any REST server that implements the
spec.

## Test Configuration

The RCK can be configured using either environment variables or java system properties and allows
for configuring both the tests and the REST client.  Environment variables prefixed by `CATALOG_`
are passed through the catalog configuring with the following mutations:

1. The `CATALOG_` prefix is stripped from the key name
2. Single underscore (`_`) is replaced with a dot (`.`)
3. Double underscore (`__`) is replaced with a dash (`-`)
4. The key names are converted to lowercase

A basic environment configuration would look like the following:

```shell
CATALOG_URI=https://my_rest_server.io/                    ## -> uri=https://my_rest_server.io/
CATALOG_WAREHOUSE=test_warehouse                          ## -> warehouse=test_warehouse
CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO       ## -> io-impl=org.apache.iceberg.aws.s3.S3FileIO
CATALOG_CREDENTIAL=<oauth_key>:<oauth_secret>             ## -> credential=<oauth_key>:<oauth_secret>
```

Java properties passed to the test must be prefixed with `rck.`, which can be used to configure some
test configurations described below and any catalog client properties.

An example of the same configuration using java system properties would look like the following:
```shell
rck.uri=https://my_rest_server.io/                    ## -> uri=https://my_rest_server.io/
rck.warehouse=test_warehouse                          ## -> warehouse=test_warehouse
rck.io-impl=org.apache.iceberg.aws.s3.S3FileIO        ## -> io-impl=org.apache.iceberg.aws.s3.S3FileIO
rck.credential=<oauth_key>:<oauth_secret>             ## -> credential=<oauth_key>:<oauth_secret>
```

Some test behaviors are configurable depending on the catalog implementations.  Not all behaviors
are strictly defined by the REST Specification.  The following are currently configurable:

| config                        | default |
|-------------------------------|---------|
| rck.requires-namespace-create | true    |
| rck.supports-serverside-retry | true    |


## Running Compatibility Tests

The compatibility tests can be invoked via gradle with the following:

Note:  The default behavior is to run a local http server with a jdbc backend for testing purposes,
so `-Drck.local=false` must be set to point to an external REST server.

```shell
./gradlew :iceberg-open-api:test --tests RESTCompatibilityKitSuite \
 -Drck.local=false \
 -Drck.requires-namespace-create=true \
 -Drck.uri=https://my_rest_server.io/ \
 -Drck.warehouse=test_warehouse \
 -Drck.credential=<oauth_key>:<oauth_secret>
```