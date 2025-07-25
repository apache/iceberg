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
package org.apache.iceberg.rest.auth.oauth2.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.errorprone.annotations.MustBeClosed;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.TLSConfigurer;
import org.apache.iceberg.rest.auth.oauth2.ImmutableOAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.ImmutableOAuth2Runtime;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Manager;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Runtime;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2Client;
import org.apache.iceberg.rest.auth.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtil;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableBasicConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableTokenExchangeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.ImmutableTokenRefreshConfig;
import org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig;
import org.apache.iceberg.rest.auth.oauth2.config.TokenRefreshConfig;
import org.apache.iceberg.rest.auth.oauth2.flow.FlowFactory;
import org.apache.iceberg.rest.auth.oauth2.flow.TokensResult;
import org.apache.iceberg.rest.auth.oauth2.http.RESTClientAdapter;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableClientCredentialsExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableConfigEndpointExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableErrorExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableLoadTableEndpointExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableMetadataDiscoveryExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableRefreshTokenExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableTokenExchangeExpectation;
import org.apache.iceberg.util.ThreadPools;
import org.immutables.value.Value;

/**
 * A test environment for OAuth2.
 *
 * <p>The environment provides easy configuration of {@link OAuth2Config} and {@link OAuth2Runtime}
 * objects, as well as a MockServer instance for unit testing, with a set of pre-configured
 * expectations for the OAuth2 endpoints that match the desired client behavior.
 *
 * <p>It also provides a set of utility methods for creating and managing OAuth2 clients and REST
 * catalogs.
 */
@Value.Immutable(copy = false)
@SuppressWarnings("resource")
public abstract class TestEnvironment implements AutoCloseable {

  public static final Instant NOW = Instant.parse("2025-01-01T00:00:00Z");

  public static final int ACCESS_TOKEN_EXPIRES_IN_SECONDS = 3600;

  public static final ClientID CLIENT_ID1 = new ClientID("Client1");
  public static final ClientID CLIENT_ID2 = new ClientID("Client2");

  public static final Secret CLIENT_SECRET1 = new Secret("s3cr3t");
  public static final Secret CLIENT_SECRET2 = new Secret("sEcrEt");

  public static final String USERNAME = "Alice";
  public static final Secret PASSWORD = new Secret("s3cr3t");

  public static final Scope SCOPE1 = new Scope("catalog");
  public static final Scope SCOPE2 = new Scope("session");

  public static final String SUBJECT_TOKEN = "subject";
  public static final String ACTOR_TOKEN = "actor";

  public static final Audience AUDIENCE = new Audience("audience");
  public static final URI RESOURCE = URI.create("urn:iceberg-oauth2-client:test:resource");

  public static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("namespace1", "table1");

  private static final AtomicInteger ID_COUNTER = new AtomicInteger(0);

  public static ImmutableTestEnvironment.Builder builder() {
    return ImmutableTestEnvironment.builder();
  }

  // OAuth2 properties

  @Value.Default
  public GrantType grantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  @Value.Default
  public Optional<ClientID> clientId() {
    return Optional.of(CLIENT_ID1);
  }

  @Value.Default
  public Optional<Secret> clientSecret() {
    return Optional.of(CLIENT_SECRET1);
  }

  @Value.Default
  public ClientAuthenticationMethod clientAuthenticationMethod() {
    return ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
  }

  @Value.Default
  public Map<String, String> extraRequestParameters() {
    return Map.of("extra1", "value1");
  }

  public abstract Optional<TypelessAccessToken> token();

  @Value.Default
  public Scope scope() {
    return SCOPE1;
  }

  @Value.Default
  public Duration tokenAcquisitionTimeout() {
    return unitTest() ? Duration.ofSeconds(5) : BasicConfig.DEFAULT_TIMEOUT;
  }

  @Value.Default
  public boolean tokenRefreshEnabled() {
    return true;
  }

  @Value.Default
  public boolean tokenRefreshWithTokenExchangeEnabled() {
    return grantType().equals(GrantType.CLIENT_CREDENTIALS);
  }

  @Value.Default
  public Duration accessTokenLifespan() {
    return Duration.ofSeconds(ACCESS_TOKEN_EXPIRES_IN_SECONDS);
  }

  @Value.Default
  public Optional<String> subjectTokenString() {
    return Optional.of(SUBJECT_TOKEN);
  }

  public abstract Optional<TokenTypeURI> subjectTokenType();

  @Value.Default
  public Optional<String> actorTokenString() {
    return Optional.of(ACTOR_TOKEN);
  }

  public abstract Optional<TokenTypeURI> actorTokenType();

  public abstract Optional<TokenTypeURI> requestedTokenType();

  @Value.Default
  public List<Audience> audiences() {
    return List.of(AUDIENCE);
  }

  @Value.Default
  public List<URI> resources() {
    return List.of(RESOURCE);
  }

  public abstract Optional<JWSAlgorithm> jwsAlgorithm();

  public abstract Optional<Path> privateKey();

  // General configuration

  @Value.Default
  public boolean unitTest() {
    return true;
  }

  @Value.Default
  public String environmentId() {
    return "env" + ID_COUNTER.incrementAndGet();
  }

  @Value.Default
  public boolean discoveryEnabled() {
    return true;
  }

  @Value.Default
  public boolean returnRefreshTokens() {
    return !grantType().equals(GrantType.CLIENT_CREDENTIALS);
  }

  @Value.Default
  public boolean createDefaultExpectations() {
    return unitTest();
  }

  @Value.Default
  public boolean ssl() {
    return false;
  }

  // URLs, endpoints and paths

  @Value.Default
  public URI serverRootUrl() {
    if (!unitTest()) {
      throw new IllegalStateException(
          "serverRootUrl() must be provided explicitly for integration tests");
    }
    return URI.create(
        "http://localhost:%d/%s/".formatted(TestServer.instance().getLocalPort(), environmentId()));
  }

  @Value.Default
  public String authorizationServerContextPath() {
    return "auth-server/";
  }

  @Value.Default
  public String catalogServerContextPath() {
    return "catalog-server/";
  }

  @Value.Default
  public URI authorizationServerUrl() {
    return serverRootUrl().resolve(authorizationServerContextPath());
  }

  @Value.Default
  public URI catalogServerUrl() {
    return serverRootUrl().resolve(catalogServerContextPath());
  }

  @Value.Default
  public URI tokenEndpoint() {
    return authorizationServerUrl().resolve("token");
  }

  @Value.Default
  public String wellKnownPath() {
    return ".well-known/openid-configuration";
  }

  @Value.Default
  public URI discoveryEndpoint() {
    return authorizationServerUrl().resolve(wellKnownPath());
  }

  @Value.Default
  public URI configEndpoint() {
    return catalogServerUrl().resolve(ResourcePaths.config());
  }

  @Value.Default
  public URI loadTableEndpoint() {
    return catalogServerUrl()
        .resolve(ResourcePaths.forCatalogProperties(catalogProperties()).table(TABLE_IDENTIFIER));
  }

  // REST Catalog configuration

  @Value.Default
  public Map<String, String> catalogProperties() {

    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, catalogServerUrl().toString())
            .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO")
            .put(CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1", "catalog-default-key1")
            .put(CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2", "catalog-default-key2")
            .put(CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3", "catalog-default-key3")
            .put(CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3", "catalog-override-key3")
            .put(CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4", "catalog-override-key4")
            .put(AuthProperties.AUTH_TYPE, OAuth2Manager.class.getName());

    // Note: we don't include all possible OAuth2 properties here, only a few ones that are
    // relevant for catalog tests.

    builder
        .put(BasicConfig.GRANT_TYPE, grantType().toString())
        .put(BasicConfig.SCOPE, scope().toString())
        .put(BasicConfig.CLIENT_AUTH, clientAuthenticationMethod().toString());

    token().ifPresent(t -> builder.put(BasicConfig.TOKEN, t.getValue()));
    clientId().ifPresent(id -> builder.put(BasicConfig.CLIENT_ID, id.getValue()));

    if (ConfigUtil.requiresClientSecret(clientAuthenticationMethod())) {
      clientSecret().ifPresent(secret -> builder.put(BasicConfig.CLIENT_SECRET, secret.getValue()));
    }

    extraRequestParameters().forEach((k, v) -> builder.put(BasicConfig.EXTRA_PARAMS + '.' + k, v));

    if (discoveryEnabled()) {
      builder.put(BasicConfig.ISSUER_URL, authorizationServerUrl().toString());
    } else {
      builder.put(BasicConfig.TOKEN_ENDPOINT, tokenEndpoint().toString());
    }

    if (grantType().equals(GrantType.TOKEN_EXCHANGE)) {
      subjectTokenString().ifPresent(t -> builder.put(TokenExchangeConfig.SUBJECT_TOKEN, t));
      actorTokenString().ifPresent(t -> builder.put(TokenExchangeConfig.ACTOR_TOKEN, t));

      subjectTokenType()
          .ifPresent(t -> builder.put(TokenExchangeConfig.SUBJECT_TOKEN_TYPE, t.toString()));
      actorTokenType()
          .ifPresent(t -> builder.put(TokenExchangeConfig.ACTOR_TOKEN_TYPE, t.toString()));
      requestedTokenType()
          .ifPresent(t -> builder.put(TokenExchangeConfig.REQUESTED_TOKEN_TYPE, t.toString()));

      if (!resources().isEmpty()) {
        builder.put(
            TokenExchangeConfig.RESOURCES,
            resources().stream().map(URI::toString).collect(Collectors.joining(",")));
      }

      if (!audiences().isEmpty()) {
        builder.put(
            TokenExchangeConfig.AUDIENCES,
            audiences().stream().map(Audience::getValue).collect(Collectors.joining(",")));
      }
    }

    return builder.build();
  }

  @Value.Default
  public GrantType sessionContextGrantType() {
    return GrantType.CLIENT_CREDENTIALS;
  }

  public abstract Optional<String> sessionContextSubjectTokenString();

  public abstract Optional<String> sessionContextActorTokenString();

  @Value.Default
  public SessionContext sessionContext() {

    // Note: we don't include all possible OAuth2 properties here, only a few ones that are
    // relevant for catalog tests.

    Map<String, String> credentials =
        Map.of(
            BasicConfig.CLIENT_ID,
            TestEnvironment.CLIENT_ID1.getValue(),
            BasicConfig.CLIENT_SECRET,
            TestEnvironment.CLIENT_SECRET1.getValue());
    ImmutableMap.Builder<String, String> propertiesBuilder =
        ImmutableMap.<String, String>builder()
            .put(BasicConfig.ISSUER_URL, authorizationServerUrl().toString())
            .put(BasicConfig.GRANT_TYPE, sessionContextGrantType().getValue())
            .put(BasicConfig.SCOPE, TestEnvironment.SCOPE2.toString())
            .put(BasicConfig.EXTRA_PARAMS + ".extra2", "value2");
    if (sessionContextGrantType().equals(GrantType.TOKEN_EXCHANGE)) {
      sessionContextSubjectTokenString()
          .ifPresent(t -> propertiesBuilder.put(TokenExchangeConfig.SUBJECT_TOKEN, t));
      sessionContextActorTokenString()
          .ifPresent(t -> propertiesBuilder.put(TokenExchangeConfig.ACTOR_TOKEN, t));

      subjectTokenType()
          .ifPresent(
              t -> propertiesBuilder.put(TokenExchangeConfig.SUBJECT_TOKEN_TYPE, t.toString()));
      actorTokenType()
          .ifPresent(
              t -> propertiesBuilder.put(TokenExchangeConfig.ACTOR_TOKEN_TYPE, t.toString()));
      requestedTokenType()
          .ifPresent(
              t -> propertiesBuilder.put(TokenExchangeConfig.REQUESTED_TOKEN_TYPE, t.toString()));

      if (!resources().isEmpty()) {
        propertiesBuilder.put(
            TokenExchangeConfig.RESOURCES,
            resources().stream().map(URI::toString).collect(Collectors.joining(",")));
      }

      if (!audiences().isEmpty()) {
        propertiesBuilder.put(
            TokenExchangeConfig.AUDIENCES,
            audiences().stream().map(Audience::getValue).collect(Collectors.joining(",")));
      }
    }
    return new SessionCatalog.SessionContext(
        UUID.randomUUID().toString(), "user", credentials, propertiesBuilder.build());
  }

  @Value.Default
  public Map<String, String> tableProperties() {
    return Map.of(BasicConfig.TOKEN, "token"); // vended token
  }

  // OAuth2 Configuration objects

  @Value.Derived
  public BasicConfig basicConfig() {
    ImmutableBasicConfig.Builder builder =
        ImmutableBasicConfig.builder()
            .grantType(grantType())
            .token(token())
            .clientId(clientId())
            .clientAuthenticationMethod(clientAuthenticationMethod())
            .clientSecret(
                ConfigUtil.requiresClientSecret(clientAuthenticationMethod())
                    ? clientSecret()
                    : Optional.empty())
            .scope(scope())
            .extraRequestParameters(extraRequestParameters())
            .tokenAcquisitionTimeout(tokenAcquisitionTimeout())
            .minTokenAcquisitionTimeout(tokenAcquisitionTimeout());
    if (discoveryEnabled()) {
      builder.issuerUrl(authorizationServerUrl());
    } else {
      builder.tokenEndpoint(tokenEndpoint());
    }

    return builder.build();
  }

  @Value.Derived
  public TokenRefreshConfig tokenRefreshConfig() {
    return ImmutableTokenRefreshConfig.builder()
        .enabled(tokenRefreshEnabled())
        .tokenExchangeEnabled(tokenRefreshWithTokenExchangeEnabled())
        .accessTokenLifespan(accessTokenLifespan())
        .build();
  }

  @Value.Derived
  public TokenExchangeConfig tokenExchangeConfig() {
    return ImmutableTokenExchangeConfig.builder()
        .subjectTokenString(subjectTokenString())
        .subjectTokenType(subjectTokenType())
        .actorTokenString(actorTokenString())
        .actorTokenType(actorTokenType())
        .requestedTokenType(requestedTokenType())
        .audiences(audiences())
        .resources(resources())
        .build();
  }

  @Value.Derived
  public OAuth2Config config() {
    return ImmutableOAuth2Config.builder()
        .basicConfig(basicConfig())
        .tokenRefreshConfig(tokenRefreshConfig())
        .tokenExchangeConfig(tokenExchangeConfig())
        .build();
  }

  // User Emulation

  @Value.Default
  public String username() {
    return USERNAME;
  }

  @Value.Default
  public Secret password() {
    return PASSWORD;
  }

  @Value.Default
  public boolean forceInactiveUser() {
    return false;
  }

  @Value.Default
  public boolean emulateFailure() {
    return false;
  }

  // OAuth2 Runtime

  public abstract Optional<OAuth2Client> parentClient();

  @Value.Default
  public ScheduledExecutorService executor() {
    return ThreadPools.authRefreshPool();
  }

  public abstract Optional<Path> sslTrustStorePath();

  public abstract Optional<String> sslTrustStorePassword();

  @Value.Derived
  public SSLContext sslContext() {
    try {
      if (sslTrustStorePath().isEmpty()) {
        return SSLContext.getDefault();
      }

      return SSLContextBuilder.create()
          .loadTrustMaterial(
              sslTrustStorePath().get(),
              sslTrustStorePassword().map(String::toCharArray).orElse(null))
          .build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Value.Derived
  public HTTPClient.Builder httpClientBuilder() {
    return HTTPClient.builder(
            Map.of("rest.client.tls.configurer-impl", TestTLSConfigurer.class.getName()))
        .uri(catalogServerUrl())
        .withAuthSession(AuthSession.EMPTY);
  }

  @Value.Derived
  public HTTPClient httpClient() {
    HTTPClient.Builder builder = httpClientBuilder();
    TestTLSConfigurer.SSL_CONTEXT.set(sslContext());
    return builder.build();
  }

  @Value.Derived
  public RESTClientAdapter restClientAdapter() {
    return new RESTClientAdapter(this::httpClient);
  }

  @Value.Default
  public Clock clock() {
    return unitTest() ? Clock.fixed(NOW, ZoneOffset.UTC) : Clock.systemUTC();
  }

  @Value.Derived
  public OAuth2Runtime runtime() {
    return ImmutableOAuth2Runtime.builder()
        .parent(parentClient())
        .httpClient(restClientAdapter())
        .executor(executor())
        .clock(clock())
        .build();
  }

  // Lifecycle methods

  @Value.Check
  public void initialize() {
    if (createDefaultExpectations()) {
      createExpectations();
    }
  }

  public void reset() {
    if (unitTest()) {
      TestServer.clearExpectations(serverRootUrl().getPath() + ".*");
    }
  }

  @Override
  public void close() {
    if (executor() != ThreadPools.authRefreshPool()) {
      MoreExecutors.shutdownAndAwaitTermination(executor(), Duration.ofSeconds(10));
    }
    try {
      httpClient().close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      reset();
    }
  }

  // Factory methods

  @MustBeClosed
  public FlowFactory newFlowFactory() {
    return FlowFactory.of(config(), runtime());
  }

  @MustBeClosed
  public OAuth2Client newOAuth2Client() {
    return new OAuth2Client(config(), runtime());
  }

  @MustBeClosed
  public RESTCatalog newCatalog(Map<String, String> additionalProperties) {
    RESTCatalog catalog =
        new RESTCatalog(
            sessionContext(),
            config -> httpClientBuilder().withHeaders(RESTUtil.configHeaders(config)).build());
    catalog.initialize(
        "catalog-" + System.nanoTime(),
        ImmutableMap.<String, String>builder()
            .putAll(catalogProperties())
            .putAll(additionalProperties)
            .buildKeepingLast());
    return catalog;
  }

  // MockServer Expectations

  public void createExpectations() {
    createInitialGrantExpectations();
    createRefreshTokenExpectations();
    createCatalogExpectations();
    createMetadataDiscoveryExpectations();
    createErrorExpectations();
  }

  public void createInitialGrantExpectations() {
    Set<GrantType> grantTypes = ImmutableSet.of(grantType(), sessionContextGrantType());
    for (GrantType grantType : grantTypes) {
      if (grantType.equals(GrantType.CLIENT_CREDENTIALS)) {
        ImmutableClientCredentialsExpectation.of(this).create();
      } else if (grantType.equals(GrantType.TOKEN_EXCHANGE)) {
        ImmutableTokenExchangeExpectation.of(this).create();
      }
    }
  }

  public void createRefreshTokenExpectations() {
    if (tokenRefreshEnabled()) {
      ImmutableRefreshTokenExpectation.of(this).create();
    }
  }

  public void createCatalogExpectations() {
    ImmutableConfigEndpointExpectation.of(this).create();
    ImmutableLoadTableEndpointExpectation.of(this).create();
  }

  public void createMetadataDiscoveryExpectations() {
    if (discoveryEnabled()) {
      ImmutableMetadataDiscoveryExpectation.of(this).create();
    }
  }

  public void createErrorExpectations() {
    ImmutableErrorExpectation.of(this).create();
  }

  // Useful token assertions

  public void assertTokensResult(
      TokensResult result, String accessToken, @Nullable String refreshToken) {
    assertThat(result.accessTokenExpirationTime()).hasValue(NOW.plus(accessTokenLifespan()));
    assertAccessToken(result.tokens().getAccessToken(), accessToken, accessTokenLifespan());
    assertRefreshToken(result.tokens().getRefreshToken(), refreshToken);
  }

  public void assertAccessToken(AccessToken actual, String expected, Duration lifespan) {
    assertThat(actual.getValue()).as("Access token value").isEqualTo(expected);
    assertThat(actual.getLifetime()).as("Access token lifetime").isEqualTo(lifespan.getSeconds());
  }

  public void assertRefreshToken(RefreshToken actual, String expected) {
    if (expected == null) {
      assertThat(actual).as("Refresh token").isNull();
    } else {
      assertThat(actual).as("Refresh token").isNotNull();
      assertThat(actual.getValue()).as("Refresh token value").isEqualTo(expected);
    }
  }

  public static class TestTLSConfigurer implements TLSConfigurer {

    private static final ThreadLocal<SSLContext> SSL_CONTEXT = new ThreadLocal<>();

    @Override
    public SSLContext sslContext() {
      return SSL_CONTEXT.get();
    }
  }
}
