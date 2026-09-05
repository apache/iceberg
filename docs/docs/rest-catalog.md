---
title: "REST Catalog"
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

# Iceberg REST Catalog

An Iceberg REST catalog is any catalog service that implements the
[Iceberg REST Catalog API specification](../../rest-catalog-spec.md). Instead of
requiring a catalog-specific client in every engine and language, the catalog
logic lives behind an HTTP API, and a single client implementation works with
any compliant server.

This page describes how to connect to a REST catalog from engines. For the protocol's features see the
[REST Catalog Protocol](rest-protocol.md) concept page; for the protocol
definition itself, see the [spec page](../../rest-catalog-spec.md). To try the
protocol locally, the community publishes the
[`apache/iceberg-rest-fixture`](https://hub.docker.com/r/apache/iceberg-rest-fixture)
Docker image, which serves the REST API backed by an in-memory catalog; see its
[README](https://github.com/apache/iceberg/blob/main/docker/iceberg-rest-fixture/README.md)
for how to run and configure it, or the
[Spark quickstart](../../spark-quickstart.md).

## Examples

### Spark

```shell
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-{{ sparkVersionMajor }}:{{ icebergVersion }} \
    --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.type=rest \
    --conf spark.sql.catalog.my_catalog.uri=https://catalog-service/api/catalog \
    --conf spark.sql.catalog.my_catalog.warehouse=my_warehouse \
    --conf spark.sql.catalog.my_catalog.rest.auth.type=oauth2 \
    --conf spark.sql.catalog.my_catalog.credential=<client_id>:<client_secret>
```

See [Spark catalog configuration](spark-configuration.md#catalog-configuration)
for details.

### Flink

```sql
CREATE CATALOG prod WITH (
  'type'='iceberg',
  'catalog-type'='rest',
  'uri'='https://catalog-service/api/catalog'
);
```

See the [Flink catalog documentation](flink.md#rest-catalog) for details.

## Configuration

Connecting to a REST catalog requires at minimum a `uri` pointing at the
service. The following properties configure the client side of the
connection; the
[common catalog properties](catalog-properties.md) (such as `warehouse` and
`io-impl`) apply as well. Note that the server can adjust this configuration
at connection time through
[endpoint discovery](rest-protocol.md#endpoint-discovery-and-server-provided-configuration).

| Property                              | Default           | Description                                                                                                                                                                                      |
|---------------------------------------|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `snapshot-loading-mode`               | `ALL`             | Controls how snapshots are loaded from the REST server. Supported values: `ALL` (load all snapshots), `REFS` (load only referenced snapshots).                                                  |
| `rest-metrics-reporting-enabled`      | `true`            | Whether to enable metrics reporting to the REST server.                                                                                                                                          |
| `view-endpoints-supported`            | `false`           | For backwards compatibility with older REST servers. Set to `true` if the server supports view endpoints but doesn't send the `endpoints` field in the ConfigResponse.                          |
| `rest-page-size`                      | null              | The page size to use when listing namespaces, tables, or other paginated resources.                                                                                                              |
| `namespace-separator`                 | `%1F`             | The separator character used for namespace levels when communicating with the REST server.                                                                                                       |
| `scan-planning-mode`                  | `CLIENT`          | Controls where scan planning is performed. Supported values: `CLIENT` (client-side planning), `SERVER` (server-side planning). Can be overridden per-table by the server in LoadTableResponse. |
| `rest.client.max-retries`           | `5`     | Maximum number of times to retry a failed HTTP request, using exponential backoff between attempts.                                                                                                                                                                                                                 |
| `rest.client.max-connections`       | `100`   | Maximum total number of connections in the HTTP client connection pool. A JVM system property with the same name takes precedence over this catalog property.                                                                                                                                                       |
| `rest.client.connections-per-route` | `100`   | Maximum number of pooled connections per route.                                                                                                                                                                                                                                                                     |
| `rest.client.connection-timeout-ms` | null    | Timeout in milliseconds for establishing a connection to the server. If not set, the HTTP client library default is used.                                                                                                                                                                                           |
| `rest.client.socket-timeout-ms`     | null    | Socket timeout in milliseconds for awaiting data on a connection. If not set, the HTTP client library default is used.                                                                                                                                                                                              |
| `rest.client.user-agent`            | null    | Custom `User-Agent` header value to send with each request. If not set, the HTTP client library default is used.                                                                                                                                                                                                    |
| `rest.client.proxy.hostname`        | null    | Hostname of an HTTP proxy to route requests through. Must be set together with `rest.client.proxy.port` to take effect.                                                                                                                                                                                             |
| `rest.client.proxy.port`            | null    | Port of the HTTP proxy. Must be set together with `rest.client.proxy.hostname` to take effect.                                                                                                                                                                                                                      |
| `rest.client.proxy.username`        | null    | Username for proxy authentication. Must be set together with `rest.client.proxy.password` to take effect, and only applies when the proxy hostname and port are configured. Only Basic authentication is supported.                                                                                                 |
| `rest.client.proxy.password`        | null    | Password for proxy authentication. Must be set together with `rest.client.proxy.username` to take effect.                                                                                                                                                                                                           |
| `rest.client.tls.configurer-impl`   | null    | A custom `org.apache.iceberg.rest.auth.TLSConfigurer` implementation to customize TLS settings such as the `SSLContext`, hostname verifier, supported protocols, and cipher suites. The implementation must have a no-arg constructor and is initialized with the catalog properties. If it returns a non-null hostname verifier, that verifier replaces the built-in JSSE hostname verification. |

### Table cache properties

The following properties configure the table cache used for freshness-aware table loading. Note, this cache is different from the one that can be configured at catalog level in general.

| Property                                 | Default           | Description                                                                            |
|-------------------------------------------|-------------------|------------------------------------------------------------------------------------------|
| `rest-table-cache.expire-after-write-ms` | `300000` (5 min)  | Time in milliseconds after which cached table entries expire.                          |
| `rest-table-cache.max-entries`           | `100`             | Maximum number of table entries to cache.                                              |

## Authentication

Most deployments require authentication, which is specific to the catalog
service. The following catalog properties configure the potential
authentication mechanisms: Basic, OAuth2, SigV4, and Google.

### REST auth properties

| Property                             | Default          | Description                                                                                                       |
|--------------------------------------|------------------|-------------------------------------------------------------------------------------------------------------------|
| `rest.auth.type`                     | `none`           | Authentication mechanism for REST catalog access. Supported values: `none`, `basic`, `oauth2`, `sigv4`, `google`. |
| `rest.auth.basic.username`           | null             | Username for Basic authentication. Required if `rest.auth.type` = `basic`.                                        |
| `rest.auth.basic.password`           | null             | Password for Basic authentication. Required if `rest.auth.type` = `basic`.                                        |
| `rest.auth.sigv4.delegate-auth-type` | `oauth2`         | Auth type to delegate to after `sigv4` signing.                                                                   |

### OAuth2 auth properties
Required and optional properties to include while using `oauth2` authentication

| Property                | Default           | Description                                                                                                                                                           |
|-------------------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `token`                 | null              | A Bearer token to interact with the server. Either `token` or `credential` is required.                                                                               |
| `credential`            | null              | Credential string in the form of `client_id:client_secret` to exchange for a token in the OAuth2 client credentials flow. Either `token` or `credential` is required. |
| `oauth2-server-uri`     | `v1/oauth/tokens` | OAuth2 token endpoint URI. Required if the REST catalog is not the OAuth2 authentication server.                                                                      |
| `token-expires-in-ms`   | 3600000 (1 hour)  | Time in milliseconds after which a bearer token is considered expired. Used to decide when to refresh or re-exchange a token.                                         |
| `token-refresh-enabled` | true              | Determines whether tokens are automatically refreshed when expiration details are available.                                                                          |
| `token-exchange-enabled`| true              | Determines whether to use the token exchange flow to acquire new tokens. Disabling this will allow fallback to the client credential flow.                            |
| `scope`                 | `catalog`         | Additional scope for `oauth2`.                                                                                                                                        |
| `audience`              | null              | Optional param to specify token `audience`                                                                                                                            |
| `resource`              | null              | Optional param to specify `resource`                                                                                                                                  |

!!! warning
    `credential` and `token` are secrets. Engines may expose catalog configuration in their UIs and logs —
    Apache Spark, for example, lists it in the Environment tab and writes it to event logs. Confirm that your engine
    redacts these values; for Spark, check that they are covered by `spark.redaction.regex` and add them to
    the pattern if they are not.

### SigV4 auth properties
Required and optional properties to include while using `sigv4` authentication

| Property                             | Default          | Description                                                                                                       |
|--------------------------------------|------------------|-------------------------------------------------------------------------------------------------------------------|
| `rest.signing-region`                     | null           | Region to be used by the SigV4 protocol for signing requests. |
| `rest.signing-name`                       | `execute-api`  | The service name to be used by the SigV4 protocol for signing requests. |
| `rest.access-key-id`                      | null           | Configure the static access key ID used for SigV4 signing. |
| `rest.secret-access-key`                  | null           | Configure the static secret access key used for SigV4 signing. |
| `rest.session-token`                      | null           | Configure the static session token used for SigV4. |
| `client.credentials-provider`             | null           | When configured, REST catalog requests will use this provider to get AWS credentials to sign the request instead of reading the default credential chain. |
| `client.assume-role.arn`            | null, requires user input                | ARN of the role to assume, e.g. arn:aws:iam::123456789:role/myRoleToAssume  |
| `client.assume-role.region`         | null, requires user input                | All AWS clients except the STS client will use the given region instead of the default region chain  |
| `client.assume-role.external-id`    | null                                     | An optional [external ID](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)  |
| `client.assume-role.timeout-sec`    | 1 hour                                   | Timeout of each assume role session. At the end of the timeout, a new set of role session credentials will be fetched through an STS client.  |

When `rest.access-key-id`, `rest.secret-access-key`, and optionally `rest.session-token` are configured, REST Catalog requests will be signed with the provided basic or session credentials instead of using the default credential chain. If `rest.session-token` is set, session credential is used, otherwise basic credential is used.

When basic or session credentials are provided, the provided credentials will be used instead of `client.credentials-provider`. `client.credentials-provider` must contain a static `create` or `create(Map<String, String>)` method to be used by REST catalog requests.

When `client.assume-role.arn` and `client.assume-role.region` are configured, Iceberg will assume the role using the default credential chain to sign REST catalog requests. These parameters will have no effect if `rest.access-key-id`, `rest.secret-access-key`, or `client.credentials-provider` are configured.

### Google auth properties
Required and optional properties to include while using `google` authentication

| Property                   | Default                                          | Description                                      |
|----------------------------|--------------------------------------------------|--------------------------------------------------|
| `gcp.auth.credentials-path`| Application Default Credentials (ADC)            | Path to a service account JSON key file.         |
| `gcp.auth.credentials-json` | Application Default Credentials (ADC)            | JSON string of a service account credential.     |
| `gcp.auth.scopes`          | `https://www.googleapis.com/auth/cloud-platform` | Comma-separated list of OAuth scopes to request. |
