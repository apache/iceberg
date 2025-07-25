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

<!--
This page is automatically generated from the code. Do not edit it manually.
To update this page, run: `./gradlew :iceberg-core:generateOAuth2Docs`.
-->

# REST OAuth2 Configuration

## Basic Settings

Basic OAuth2 properties. These properties are used to configure the basic OAuth2 options such as the issuer URL, token endpoint, client ID, and client secret.

### `rest.auth.oauth2.token`

The initial access token to use. Optional. If this is set, the OAuth2 client will not attempt to fetch an initial token from the Authorization server, but will use this token instead.

This option should be avoided as in most cases, the token cannot be refreshed.

### `rest.auth.oauth2.issuer-url`

The root URL of the Authorization server, which will be used for discovering supported endpoints and their locations. For Keycloak, this is typically the realm URL: `https://<keycloak-server>/realms/<realm-name>`.

Two "well-known" paths are supported for endpoint discovery: `.well-known/openid-configuration` and `.well-known/oauth-authorization-server`. The full metadata discovery URL will be constructed by appending these paths to the issuer URL.

Unless a static token (`rest.auth.oauth2.token`) is provided, either this property or `rest.auth.oauth2.token-endpoint` must be set.

### `rest.auth.oauth2.token-endpoint`

URL of the OAuth2 token endpoint. For Keycloak, this is typically `https://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/token`.

Unless a static token (`rest.auth.oauth2.token`) is provided, either this property or `rest.auth.oauth2.issuer-url` must be set. In case it is not set, the token endpoint will be discovered from the issuer URL (`rest.auth.oauth2.issuer-url`), using the OpenID Connect Discovery metadata published by the issuer.

### `rest.auth.oauth2.grant-type`

The grant type to use when authenticating against the OAuth2 server. Valid values are:

- `client_credentials`
- `urn:ietf:params:oauth:grant-type:token-exchange`

Optional, defaults to `client_credentials`.

### `rest.auth.oauth2.client-id`

Client ID to use when authenticating against the OAuth2 server. Required, unless a static token (`rest.auth.oauth2.token`) is provided.

### `rest.auth.oauth2.client-auth`

The OAuth2 client authentication method to use. Valid values are:

- `none`: the client does not authenticate itself at the token endpoint, because it is a public client with no client secret or other authentication mechanism.
- `client_secret_basic`: client secret is sent in the HTTP Basic Authorization header.
- `client_secret_post`: client secret is sent in the request body as a form parameter.

The default is `client_secret_basic`.

### `rest.auth.oauth2.client-secret`

Client secret to use when authenticating against the OAuth2 server. Required if the client is private and is authenticated using the standard "client-secret" methods.

### `rest.auth.oauth2.scope`

Space-separated list of scopes to include in each request to the OAuth2 server. Optional, defaults to empty (no scopes).

The scope names will not be validated by the OAuth2 client; make sure they are valid according to [RFC 6749 Section 3.3](https://datatracker.ietf.org/doc/html/rfc6749#section-3.3).

### `rest.auth.oauth2.extra-params.*`

Extra parameters to include in each request to the token endpoint. This is useful for custom parameters that are not covered by the standard OAuth2 specification. Optional, defaults to empty.

This is a prefix property, and multiple values can be set, each with a different key and value. The values must NOT be URL-encoded. Example:

```
rest.auth.oauth2.extra-params.custom_param1=custom_value1
rest.auth.oauth2.extra-params.custom_param2=custom_value2
```

For example, Auth0 requires the `audience` parameter to be set to the API identifier. This can be done by setting the following configuration:

```
rest.auth.oauth2.extra-params.audience=https://iceberg-rest-catalog/api
```

### `rest.auth.oauth2.timeout`

The token acquisition timeout. Optional, defaults to `PT5M`. The default timeout is intentionally large, in order to accommodate for long-running flows that require human intervention (e.g. Authorization Code flow).

Must be a valid [ISO-8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations).

### `rest.auth.oauth2.session-cache.timeout`

The session cache timeout. Cached sessions will become eligible for eviction after this duration of inactivity. Defaults to 1 hour. Must be a valid [ISO-8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations).

This value is used for housekeeping; it does not mean that cached sessions will stop working after this time, but that the session cache will evict the session after this time of inactivity. If the context is used again, a new session will be created and cached.

This property can only be specified at catalog session level. It is ignored if present in other levels.
## Token Refresh Settings

Configuration properties for the token refresh feature.

### `rest.auth.oauth2.token-refresh.enabled`

Whether to enable token refresh. If enabled, the OAuth2 client will automatically refresh its access token when it expires. If disabled, the OAuth2 client will only fetch the initial access token, but won't refresh it. Defaults to `true`.

### `rest.auth.oauth2.token-refresh.token-exchange-enabled`

Whether to use the token exchange grant to refresh tokens.

When enabled, the token exchange grant will be used to refresh the access token, if no refresh token is available.

Optional, defaults to `true` if the initial grant is `client_credentials`.

### `rest.auth.oauth2.token-refresh.access-token-lifespan`

Default access token lifespan; if the OAuth2 server returns an access token without specifying its expiration time, this value will be used.

Optional, defaults to `PT1H`. Must be a valid [ISO-8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations).

### `rest.auth.oauth2.token-refresh.safety-margin`

Refresh safety margin to use; a new token will be fetched when the current token's remaining lifespan is less than this value. Optional, defaults to `PT10S`. Must be a valid [ISO-8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations).
## Token Exchange Settings

Configuration properties for the [Token Exchange](https://datatracker.ietf.org/doc/html/rfc8693) flow.

This flow allows a client to exchange one token for another, typically to obtain a token that is more suitable for the target resource or service.

### `rest.auth.oauth2.token-exchange.subject-token`

The subject token to exchange. Required.

The special value `::parent::` can be used to indicate that the subject token should be obtained from the parent OAuth2 session.

### `rest.auth.oauth2.token-exchange.subject-token-type`

The type of the subject token. Must be a valid URN. Required. If not set, the default is `urn:ietf:params:oauth:token-type:access_token`.

### `rest.auth.oauth2.token-exchange.actor-token`

The actor token to exchange. Optional.

The special value `::parent::` can be used to indicate that the actor token should be obtained from the parent OAuth2 session.

### `rest.auth.oauth2.token-exchange.actor-token-type`

The type of the actor token. Must be a valid URN. Required if an actor token is used. If not set, the default is `urn:ietf:params:oauth:token-type:access_token`.

### `rest.auth.oauth2.token-exchange.requested-token-type`

The type of the requested token. Must be a valid URN. Optional.

### `rest.auth.oauth2.token-exchange.resources`

One or more URIs that indicate the target service(s) or resource(s) where the client intends to use the requested token.

Optional. Can be a single value or a comma-separated list of values.

### `rest.auth.oauth2.token-exchange.audiences`

The logical name(s) of the target service where the client intends to use the requested token. This serves a purpose similar to the resource parameter but with the client providing a logical name for the target service.

Optional. Can be a single value or a comma-separated list of values.
