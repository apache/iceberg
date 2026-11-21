/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.rest.auth.oauth2;

import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.rest.RESTUtil;
import org.immutables.value.Value;

/** The configuration of an {@link OAuth2Manager}. */
@Value.Immutable(prehash = true)
@Value.Style(redactedMask = "****")
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
interface OAuth2Config {

  String PREFIX = "rest.auth.oauth2.";

  /** The issuer URL, used to discover the token endpoint (RFC 8414 or OpenID Connect). */
  String ISSUER_URL = PREFIX + "issuer-url";

  /** The token endpoint URL. Takes precedence over the issuer URL. */
  String TOKEN_ENDPOINT = PREFIX + "token-endpoint";

  /** The client ID. */
  String CLIENT_ID = PREFIX + "client-id";

  /** The client authentication method: client_secret_basic (default) or client_secret_post. */
  String CLIENT_AUTH = PREFIX + "client-auth";

  /** The client secret. */
  String CLIENT_SECRET = PREFIX + "client-secret";

  /** The scope(s) to request, space-separated. */
  String SCOPE = PREFIX + "scope";

  /**
   * The resource server(s) the token is intended for, as absolute URIs separated by commas (RFC
   * 8707).
   */
  String RESOURCE = PREFIX + "resource";

  /**
   * The intended audience(s) of the token, separated by commas. The {@code audience} parameter is
   * not standardized, but is supported by many authorization servers.
   */
  String AUDIENCE = PREFIX + "audience";

  /** Prefix for extra parameters to include in token requests. */
  String EXTRA_PARAMS_PREFIX = PREFIX + "extra-params.";

  /**
   * The access token lifespan to assume when the server does not report one (ISO-8601 duration).
   */
  String TOKEN_REFRESH_ACCESS_TOKEN_LIFESPAN = PREFIX + "token-refresh.access-token-lifespan";

  /** How long before expiration a new token is fetched (ISO-8601 duration). */
  String TOKEN_REFRESH_PREFETCH = PREFIX + "token-refresh.prefetch";

  /** Random delay subtracted from the renewal time to spread out renewals (ISO-8601 duration). */
  String TOKEN_REFRESH_JITTER = PREFIX + "token-refresh.jitter";

  ClientAuthenticationMethod CLIENT_AUTH_DEFAULT = ClientAuthenticationMethod.CLIENT_SECRET_BASIC;

  Splitter LIST_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

  Duration TOKEN_REFRESH_ACCESS_TOKEN_LIFESPAN_DEFAULT = Duration.ofHours(1);
  Duration TOKEN_REFRESH_PREFETCH_DEFAULT = Duration.ofSeconds(10);
  Duration TOKEN_REFRESH_JITTER_DEFAULT = Duration.ofSeconds(5);

  List<ClientAuthenticationMethod> SUPPORTED_CLIENT_AUTH_METHODS =
      List.of(
          ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
          ClientAuthenticationMethod.CLIENT_SECRET_POST);

  @Nullable
  URI issuerUrl();

  @Nullable
  URI tokenEndpoint();

  ClientID clientId();

  @Value.Default
  default ClientAuthenticationMethod clientAuth() {
    return CLIENT_AUTH_DEFAULT;
  }

  @Value.Redacted
  Secret clientSecret();

  @Nullable
  Scope scope();

  List<URI> resources();

  List<String> audiences();

  Map<String, String> extraParams();

  @Value.Default
  default Duration accessTokenLifespan() {
    return TOKEN_REFRESH_ACCESS_TOKEN_LIFESPAN_DEFAULT;
  }

  @Value.Default
  default Duration prefetch() {
    return TOKEN_REFRESH_PREFETCH_DEFAULT;
  }

  @Value.Default
  default Duration jitter() {
    return TOKEN_REFRESH_JITTER_DEFAULT;
  }

  @Value.Check
  default void validate() {
    URI issuerUrl = issuerUrl();
    URI tokenEndpoint = tokenEndpoint();

    Preconditions.checkArgument(
        issuerUrl != null || tokenEndpoint != null,
        "Either the issuer URL (%s) or the token endpoint (%s) must be set",
        ISSUER_URL,
        TOKEN_ENDPOINT);

    if (issuerUrl != null) {
      checkEndpoint(issuerUrl, "Issuer URL", ISSUER_URL);
    }

    if (tokenEndpoint != null) {
      checkEndpoint(tokenEndpoint, "Token endpoint", TOKEN_ENDPOINT);
    }

    for (URI resource : resources()) {
      Preconditions.checkArgument(
          resource.isAbsolute() && resource.getFragment() == null,
          "Resource must be an absolute URI without a fragment part: %s (%s)",
          resource,
          RESOURCE);
    }

    Preconditions.checkArgument(
        SUPPORTED_CLIENT_AUTH_METHODS.contains(clientAuth()),
        "Client authentication method must be one of: %s (%s)",
        SUPPORTED_CLIENT_AUTH_METHODS,
        CLIENT_AUTH);
    Preconditions.checkArgument(
        !clientSecret().getValue().isEmpty(),
        "Client secret must not be empty (%s)",
        CLIENT_SECRET);
    Preconditions.checkArgument(
        accessTokenLifespan().compareTo(Duration.ZERO) > 0,
        "Access token lifespan must be positive (%s)",
        TOKEN_REFRESH_ACCESS_TOKEN_LIFESPAN);
    Preconditions.checkArgument(
        !prefetch().isNegative(),
        "Token refresh prefetch must not be negative (%s)",
        TOKEN_REFRESH_PREFETCH);
    Preconditions.checkArgument(
        !jitter().isNegative(),
        "Token refresh jitter must not be negative (%s)",
        TOKEN_REFRESH_JITTER);
    Preconditions.checkArgument(
        prefetch().plus(jitter()).compareTo(accessTokenLifespan()) < 0,
        "Token refresh prefetch plus jitter must be less than the access token lifespan (%s / %s / %s)",
        TOKEN_REFRESH_PREFETCH,
        TOKEN_REFRESH_JITTER,
        TOKEN_REFRESH_ACCESS_TOKEN_LIFESPAN);
  }

  static OAuth2Config fromProperties(Map<String, String> properties) {
    ImmutableOAuth2Config.Builder builder =
        ImmutableOAuth2Config.builder()
            .issuerUrl(parseOptional(properties, ISSUER_URL, URI::create))
            .tokenEndpoint(parseOptional(properties, TOKEN_ENDPOINT, URI::create))
            .clientId(parseRequired(properties, CLIENT_ID, "Client ID", ClientID::new))
            .clientSecret(parseRequired(properties, CLIENT_SECRET, "Client secret", Secret::new))
            .scope(parseOptional(properties, SCOPE, Scope::parse))
            .resources(parseList(properties, RESOURCE, URI::create))
            .audiences(parseList(properties, AUDIENCE, Function.identity()))
            .extraParams(RESTUtil.extractPrefixMap(properties, EXTRA_PARAMS_PREFIX));

    ClientAuthenticationMethod clientAuth =
        parseOptional(properties, CLIENT_AUTH, ClientAuthenticationMethod::parse);
    if (clientAuth != null) {
      builder.clientAuth(clientAuth);
    }

    Duration accessTokenLifespan =
        parseOptional(properties, TOKEN_REFRESH_ACCESS_TOKEN_LIFESPAN, Duration::parse);
    if (accessTokenLifespan != null) {
      builder.accessTokenLifespan(accessTokenLifespan);
    }

    Duration prefetch = parseOptional(properties, TOKEN_REFRESH_PREFETCH, Duration::parse);
    if (prefetch != null) {
      builder.prefetch(prefetch);
    }

    Duration jitter = parseOptional(properties, TOKEN_REFRESH_JITTER, Duration::parse);
    if (jitter != null) {
      builder.jitter(jitter);
    }

    return builder.build();
  }

  @Nullable
  private static <T> T parseOptional(
      Map<String, String> properties, String key, Function<String, T> parser) {
    String value = properties.get(key);
    return value == null ? null : parse(key, value, parser);
  }

  private static <T> T parseRequired(
      Map<String, String> properties, String key, String name, Function<String, T> parser) {
    String value = properties.get(key);
    Preconditions.checkArgument(value != null, "%s must be set (%s)", name, key);
    return parse(key, value, parser);
  }

  private static <T> List<T> parseList(
      Map<String, String> properties, String key, Function<String, T> parser) {
    String value = properties.get(key);
    if (value == null) {
      return List.of();
    }

    return LIST_SPLITTER.splitToStream(value).map(item -> parse(key, item, parser)).toList();
  }

  private static <T> T parse(String key, String value, Function<String, T> parser) {
    try {
      return parser.apply(value);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          String.format(Locale.ROOT, "Invalid value for property %s: '%s'", key, value), e);
    }
  }

  private static void checkEndpoint(URI endpoint, String name, String key) {
    Preconditions.checkArgument(endpoint.isAbsolute(), "%s must be absolute (%s)", name, key);
    Preconditions.checkArgument(
        endpoint.getUserInfo() == null, "%s must not have a user info part (%s)", name, key);
    Preconditions.checkArgument(
        endpoint.getQuery() == null, "%s must not have a query part (%s)", name, key);
    Preconditions.checkArgument(
        endpoint.getFragment() == null, "%s must not have a fragment part (%s)", name, key);
  }
}
