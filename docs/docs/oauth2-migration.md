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

# REST OAuth2 Migration Guide

This guide explains how to migrate from the legacy REST OAuth2 configuration to the new
OAuth2 Auth Manager (v2).

## Overview

The new OAuth2 Auth Manager (`org.apache.iceberg.rest.auth.oauth2.OAuth2Manager`) replaces the
legacy implementation (`org.apache.iceberg.rest.auth.OAuth2Manager`). It provides:

- Standards-compliant OAuth2/OIDC support via the [Nimbus OAuth2 SDK](https://connect2id.com/products/nimbus-oauth-openid-connect-sdk)
- OpenID Connect Discovery for automatic endpoint resolution
- Proper client authentication methods (`client_secret_basic`, `client_secret_post`, `none`)
- Token Exchange support ([RFC 8693](https://datatracker.ietf.org/doc/html/rfc8693))
- Custom token endpoint parameters (e.g. Auth0 `audience`)
- Automatic background token refresh

## Enabling the New Auth Manager

The legacy implementation remains the default. To opt in to the new Auth Manager, set:

```properties
rest.auth.type=org.apache.iceberg.rest.auth.oauth2.OAuth2Manager
```

## Automatic Migration

If you enable the new Auth Manager but keep using legacy property names, they will be
**automatically migrated** at runtime with deprecation warnings. No immediate changes to your
configuration are required.

However, the automatic migration will be removed in a future Iceberg release. You should update
your configuration to use the new property names as described below.

## Property Mapping

### Basic Properties

| Legacy Property     | New Property                                                       | Notes                                                                                                                           |
|---------------------|--------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `credential`        | `rest.auth.oauth2.client-id` + `rest.auth.oauth2.client-secret`    | Legacy format was `client_id:client_secret`. If no colon is present (client secret alone), the client ID defaults to `iceberg`. |
| `token`             | `rest.auth.oauth2.token`                                           | Static Bearer token.                                                                                                            |
| `oauth2-server-uri` | `rest.auth.oauth2.token-endpoint` or `rest.auth.oauth2.issuer-url` | Prefer `issuer-url` for automatic endpoint discovery. See [Endpoint Configuration](#endpoint-configuration).                    |
| `scope`             | `rest.auth.oauth2.scope`                                           | No change in semantics.                                                                                                         |

### Token Refresh Properties

| Legacy Property          | New Property                                            | Notes                                                                  |
|--------------------------|---------------------------------------------------------|------------------------------------------------------------------------|
| `token-refresh-enabled`  | `rest.auth.oauth2.token-refresh.enabled`                | Same semantics, defaults to `true`.                                    |
| `token-expires-in-ms`    | `rest.auth.oauth2.token-refresh.access-token-lifespan`  | New format is an ISO-8601 duration (e.g. `PT1H` instead of `3600000`). |
| `token-exchange-enabled` | `rest.auth.oauth2.token-refresh.token-exchange-enabled` | Same semantics.                                                        |

### Token Exchange Properties

| Legacy Property           | New Property                                                                                           | Notes                                                                                                                                                              |
|---------------------------|--------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `audience`                | `rest.auth.oauth2.token-exchange.audiences`                                                            | Same semantics. Supports comma-separated list.                                                                                                                     |
| `resource`                | `rest.auth.oauth2.token-exchange.resources`                                                            | Same semantics. Supports comma-separated list.                                                                                                                     |
| *(token type URN as key)* | `rest.auth.oauth2.token-exchange.subject-token` + `rest.auth.oauth2.token-exchange.subject-token-type` | Legacy usage of token type URNs (e.g. `urn:ietf:params:oauth:token-type:access_token`) as property keys is replaced by explicit subject/actor token configuration. |

### New Properties (No Legacy Equivalent)

Many properties are new in v2 and have no legacy counterpart, here are some of them:

| Property                                               | Description                                                                                      |
|--------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `rest.auth.oauth2.issuer-url`                          | Authorization server root URL for OIDC Discovery.                                                |
| `rest.auth.oauth2.grant-type`                          | Explicit grant type (`client_credentials` or `urn:ietf:params:oauth:grant-type:token-exchange`). |
| `rest.auth.oauth2.client-auth`                         | Client authentication method (`client_secret_basic`, `client_secret_post`, `none`).              |
| `rest.auth.oauth2.extra-params.*`                      | Additional token endpoint parameters (e.g. `rest.auth.oauth2.extra-params.audience` for Auth0).  |
| `rest.auth.oauth2.timeout`                             | Token acquisition timeout (ISO-8601 duration, default `PT5M`).                                   |
| `rest.auth.oauth2.session-cache.timeout`               | Session cache eviction timeout (ISO-8601 duration, default `PT1H`).                              |
| `rest.auth.oauth2.token-exchange.actor-token`          | Actor token for token exchange.                                                                  |
| `rest.auth.oauth2.token-exchange.requested-token-type` | Requested token type for token exchange.                                                         |
| `rest.auth.oauth2.token-refresh.safety-margin`         | How early to refresh before expiration (ISO-8601 duration, default `PT10S`).                     |

For the full reference of all new configuration properties, see [REST OAuth2 Configuration](oauth2-configuration.md).

## Endpoint Configuration

The legacy `oauth2-server-uri` pointed to a single token endpoint. The new Auth Manager supports
two approaches:

**Option 1: Issuer URL with automatic discovery (recommended)**

```properties
rest.auth.oauth2.issuer-url=https://keycloak.example.com/realms/my-realm
```

The token endpoint will be automatically discovered via the
`.well-known/openid-configuration` or `.well-known/oauth-authorization-server` metadata endpoints.

**Option 2: Explicit token endpoint**

If the authorization server does not support discovery, you can also specify the token endpoint explicitly:

```properties
rest.auth.oauth2.token-endpoint=https://keycloak.example.com/realms/my-realm/protocol/openid-connect/token
```

### Legacy Fallback Behavior

If neither `issuer-url` nor `token-endpoint` is set, and no static token is provided, the new Auth
Manager will fall back to the catalog server's built-in token endpoint (i.e. `{catalog-uri}/v1/oauth/tokens`).
A warning will be logged. This fallback will be removed in a future Iceberg release.

Similarly, relative token endpoint URLs (e.g. `v1/oauth/tokens`) will be resolved against the
catalog URI. This behavior will also be removed in a future release.

## Migration Examples

### Client Credentials Flow

Before:

```properties
rest.auth.type=oauth2
credential=my-client-id:my-client-secret
oauth2-server-uri=https://auth.example.com/token
scope=catalog
```

After:

```properties
rest.auth.type=org.apache.iceberg.rest.auth.oauth2.OAuth2Manager
rest.auth.oauth2.issuer-url=https://auth.example.com
rest.auth.oauth2.client-id=my-client-id
rest.auth.oauth2.client-secret=my-client-secret
rest.auth.oauth2.scope=catalog
```

### Static Token

Before:

```properties
rest.auth.type=oauth2
token=eyJhbGciOiJSUzI1NiIs...
```

After:

```properties
rest.auth.type=org.apache.iceberg.rest.auth.oauth2.OAuth2Manager
rest.auth.oauth2.token=eyJhbGciOiJSUzI1NiIs...
```

### Auth0 with Audience Parameter

This was not possible with the legacy Auth Manager. With v2:

```properties
rest.auth.type=org.apache.iceberg.rest.auth.oauth2.OAuth2Manager
rest.auth.oauth2.issuer-url=https://my-tenant.auth0.com
rest.auth.oauth2.client-id=my-client-id
rest.auth.oauth2.client-secret=my-client-secret
rest.auth.oauth2.extra-params.audience=https://iceberg-rest-catalog/api
```

## Deprecation Timeline

- In Iceberg **1.12.0**, the legacy `org.apache.iceberg.rest.auth.OAuth2Manager` and all properties in
  `org.apache.iceberg.rest.auth.OAuth2Properties` are deprecated. The default Auth Manager remains v1
  (`org.apache.iceberg.rest.auth.OAuth2Manager`) for compatibility.
- In Iceberg **1.13.0**, the default Auth Manager will be changed to v2
  (`org.apache.iceberg.rest.auth.oauth2.OAuth2Manager`).
- In Iceberg **1.14.0**, the legacy Auth Manager and properties will be removed.
