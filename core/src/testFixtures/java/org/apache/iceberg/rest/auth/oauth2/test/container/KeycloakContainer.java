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
package org.apache.iceberg.rest.auth.oauth2.test.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import dasniko.testcontainers.keycloak.ExtendableKeycloakContainer;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.text.ParseException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.ClientScopeRepresentation;
import org.keycloak.representations.idm.CredentialRepresentation;
import org.keycloak.representations.idm.ProtocolMapperRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.keycloak.representations.idm.UserRepresentation;
import org.keycloak.representations.idm.authorization.PolicyEnforcementMode;
import org.keycloak.representations.idm.authorization.ResourceServerRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class KeycloakContainer extends ExtendableKeycloakContainer<KeycloakContainer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeycloakContainer.class);

  public static final Duration ACCESS_TOKEN_LIFESPAN = Duration.ofMinutes(10);

  private static final String CONTEXT_PATH = "/realms/master";

  private final List<ClientScopeRepresentation> scopes = Lists.newArrayList();
  private final List<ClientRepresentation> clients = Lists.newArrayList();
  private final List<UserRepresentation> users = Lists.newArrayList();
  private final Map<String, List<String>> clientAudiences = Maps.newHashMap();

  private URI rootUrl;
  private URI issuerUrl;
  private URI tokenEndpoint;
  private URI authEndpoint;

  @SuppressWarnings("resource")
  public KeycloakContainer() {
    super("keycloak/keycloak:26.4");
    withNetworkAliases("keycloak");
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
    withEnv("KC_LOG_LEVEL", rootLoggerLevel() + ",org.keycloak:" + keycloakLoggerLevel());
    // Useful when debugging Keycloak REST endpoints:
    addExposedPorts(5005);
    withEnv(
        "JAVA_TOOL_OPTIONS",
        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005");
  }

  @CanIgnoreReturnValue
  public KeycloakContainer withScope(String scope) {
    scopes.add(newScope(scope));
    return this;
  }

  @CanIgnoreReturnValue
  public KeycloakContainer withClient(
      String clientId, String clientSecret, String authenticationMethod) {
    clients.add(newClient(clientId, clientSecret, authenticationMethod));
    return this;
  }

  @CanIgnoreReturnValue
  public KeycloakContainer withClientAudience(String clientId, String audience) {
    clientAudiences.computeIfAbsent(clientId, k -> Lists.newArrayList()).add(audience);
    return this;
  }

  @CanIgnoreReturnValue
  public KeycloakContainer withUser(String username, String password) {
    users.add(newUser(username, password));
    return this;
  }

  @Override
  public void start() {
    if (getContainerId() != null) {
      return;
    }

    super.start();
    rootUrl = URI.create(getAuthServerUrl());
    issuerUrl = rootUrl.resolve(CONTEXT_PATH);
    tokenEndpoint = rootUrl.resolve(CONTEXT_PATH + "/protocol/openid-connect/token");
    authEndpoint = rootUrl.resolve(CONTEXT_PATH + "/protocol/openid-connect/auth");
    try (Keycloak client = getKeycloakAdminClient()) {
      RealmResource master = client.realms().realm("master");
      updateMasterRealm(master);
      scopes.forEach(scope -> createScope(master, scope));
      users.forEach(user -> createUser(master, user));
      clients.forEach(cl -> createClient(master, cl));
    }
  }

  public URI rootUrl() {
    return rootUrl;
  }

  public URI issuerUrl() {
    return issuerUrl;
  }

  public URI tokenEndpoint() {
    return tokenEndpoint;
  }

  public URI authEndpoint() {
    return authEndpoint;
  }

  /**
   * Verifies a JWT token by parsing it and doing a basic validation of its claims. This method does
   * NOT verify the JWT signature.
   */
  public JWTClaimsSet verifyToken(String token) {
    assertThat(token).isNotNull();
    try {
      JWTClaimsSet claims = JWTParser.parse(token).getJWTClaimsSet();
      assertThat(claims.getIssuer()).isEqualTo(issuerUrl.toString());
      assertThat(claims.getExpirationTime()).isInTheFuture();
      return claims;
    } catch (ParseException e) {
      return fail("Failed to parse JWT token", e);
    }
  }

  protected void updateMasterRealm(RealmResource master) {
    RealmRepresentation masterRep = master.toRepresentation();
    masterRep.setAccessTokenLifespan((int) ACCESS_TOKEN_LIFESPAN.toSeconds());
    // Minimum polling interval for device auth flow
    masterRep.setOAuth2DevicePollingInterval(1);
    master.update(masterRep);
  }

  protected void createScope(RealmResource master, ClientScopeRepresentation scope) {
    try (Response response = master.clientScopes().create(scope)) {
      if (response.getStatus() != 201) {
        throw new IllegalStateException(
            "Failed to create scope: " + response.readEntity(String.class));
      }
    }
  }

  protected void createClient(RealmResource master, ClientRepresentation client) {
    client.setOptionalClientScopes(
        scopes.stream().map(ClientScopeRepresentation::getName).collect(Collectors.toList()));
    try (Response response = master.clients().create(client)) {
      if (response.getStatus() != 201) {
        throw new IllegalStateException(
            "Failed to create client: " + response.readEntity(String.class));
      }
    }

    // Required for Polaris
    addPrincipalIdClaimMapper(master, client.getId());
    addPrincipalRoleClaimMapper(master, client.getId());

    // Add audience mappers if configured
    List<String> audiences = clientAudiences.get(client.getClientId());
    if (audiences != null) {
      for (String audience : audiences) {
        addAudienceMapper(master, client.getId(), audience);
      }
    }
  }

  protected void createUser(RealmResource master, UserRepresentation user) {
    try (Response response = master.users().create(user)) {
      if (response.getStatus() != 201) {
        throw new IllegalStateException(
            "Failed to create user: " + response.readEntity(String.class));
      }
    }
  }

  private static ClientScopeRepresentation newScope(String scopeName) {
    ClientScopeRepresentation scope = new ClientScopeRepresentation();
    scope.setId(UUID.randomUUID().toString());
    scope.setName(scopeName);
    scope.setProtocol("openid-connect");
    scope.setAttributes(
        Map.of(
            "include.in.token.scope",
            "true",
            "consent.screen.text",
            "REST Catalog",
            "display.on.consent.screen",
            "true"));
    return scope;
  }

  private static ClientRepresentation newClient(
      String clientId, String clientSecret, String authenticationMethod) {
    ClientRepresentation client = new ClientRepresentation();
    String clientUuid = UUID.randomUUID().toString();
    client.setId(clientUuid);
    client.setClientId(clientId);
    boolean publicClient = authenticationMethod.equals("none");
    client.setPublicClient(publicClient);
    client.setServiceAccountsEnabled(!publicClient); // required for client credentials grant
    client.setStandardFlowEnabled(true); // required for authorization code grant
    client.setRedirectUris(List.of("http://localhost:*", "https://localhost:*"));
    ImmutableMap.Builder<String, String> attributes =
        ImmutableMap.<String, String>builder()
            .put("use.refresh.tokens", "true")
            .put("client_credentials.use_refresh_token", "false")
            .put("oauth2.device.authorization.grant.enabled", "true")
            .put("standard.token.exchange.enabled", "true")
            .put("standard.token.exchange.enableRefreshRequestedTokenType", "SAME_SESSION");
    switch (authenticationMethod) {
      case "client_secret_basic":
      case "client_secret_post":
        client.setSecret(clientSecret);
        break;
    }

    if (!publicClient) {
      ResourceServerRepresentation settings = new ResourceServerRepresentation();
      settings.setPolicyEnforcementMode(PolicyEnforcementMode.DISABLED);
      client.setAuthorizationSettings(settings);
    }

    client.setAttributes(attributes.build());
    return client;
  }

  private static UserRepresentation newUser(String username, String password) {
    UserRepresentation user = new UserRepresentation();
    user.setId(UUID.randomUUID().toString());
    user.setUsername(username);
    user.setFirstName(username);
    user.setLastName(username);
    CredentialRepresentation credential = new CredentialRepresentation();
    credential.setType(CredentialRepresentation.PASSWORD);
    credential.setValue(password);
    credential.setTemporary(false);
    user.setCredentials(ImmutableList.of(credential));
    user.setEnabled(true);
    user.setEmail(username.toLowerCase(Locale.ROOT) + "@example.com");
    user.setEmailVerified(true);
    user.setRequiredActions(Collections.emptyList());
    return user;
  }

  private void addPrincipalIdClaimMapper(RealmResource master, String clientUuid) {
    ProtocolMapperRepresentation mapper = new ProtocolMapperRepresentation();
    mapper.setId(UUID.randomUUID().toString());
    mapper.setName("principal-id-claim-mapper");
    mapper.setProtocol("openid-connect");
    mapper.setProtocolMapper("oidc-hardcoded-claim-mapper");
    mapper.setConfig(
        ImmutableMap.<String, String>builder()
            .put("claim.name", "principal_id")
            .put("claim.value", "1")
            .put("jsonType.label", "long")
            .put("id.token.claim", "true")
            .put("access.token.claim", "true")
            .put("userinfo.token.claim", "true")
            .build());
    try (Response response =
        master.clients().get(clientUuid).getProtocolMappers().createMapper(mapper)) {
      if (response.getStatus() != 201) {
        throw new IllegalStateException(
            "Failed to create mapper: " + response.readEntity(String.class));
      }
    }
  }

  private void addPrincipalRoleClaimMapper(RealmResource master, String clientUuid) {
    ProtocolMapperRepresentation mapper = new ProtocolMapperRepresentation();
    mapper.setId(UUID.randomUUID().toString());
    mapper.setName("principal-role-claim-mapper");
    mapper.setProtocol("openid-connect");
    mapper.setProtocolMapper("oidc-hardcoded-claim-mapper");
    mapper.setConfig(
        ImmutableMap.<String, String>builder()
            .put("claim.name", "groups")
            .put("claim.value", "[\"PRINCIPAL_ROLE:ALL\"]")
            .put("jsonType.label", "JSON")
            .put("id.token.claim", "true")
            .put("access.token.claim", "true")
            .put("userinfo.token.claim", "true")
            .build());
    try (Response response =
        master.clients().get(clientUuid).getProtocolMappers().createMapper(mapper)) {
      if (response.getStatus() != 201) {
        throw new IllegalStateException(
            "Failed to create role claim mapper: " + response.readEntity(String.class));
      }
    }
  }

  private void addAudienceMapper(RealmResource master, String clientUuid, String audience) {
    ProtocolMapperRepresentation mapper = new ProtocolMapperRepresentation();
    mapper.setId(UUID.randomUUID().toString());
    mapper.setName("audience-mapper-" + audience);
    mapper.setProtocol("openid-connect");
    mapper.setProtocolMapper("oidc-audience-mapper");
    mapper.setConfig(
        ImmutableMap.<String, String>builder()
            .put("included.client.audience", audience)
            .put("id.token.claim", "false")
            .put("access.token.claim", "true")
            .build());
    try (Response response =
        master.clients().get(clientUuid).getProtocolMappers().createMapper(mapper)) {
      if (response.getStatus() != 201) {
        throw new IllegalStateException(
            "Failed to create audience mapper: " + response.readEntity(String.class));
      }
    }
  }

  private static String rootLoggerLevel() {
    return LOGGER.isInfoEnabled() ? "INFO" : LOGGER.isWarnEnabled() ? "WARN" : "ERROR";
  }

  private static String keycloakLoggerLevel() {
    return LOGGER.isDebugEnabled() ? "DEBUG" : rootLoggerLevel();
  }
}
