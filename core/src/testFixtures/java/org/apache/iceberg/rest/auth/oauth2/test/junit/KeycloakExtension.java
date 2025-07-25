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
package org.apache.iceberg.rest.auth.oauth2.test.junit;

import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.id.Audience;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestCertificates;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.container.KeycloakContainer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class KeycloakExtension extends TestEnvironmentExtension
    implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

  // Client1 is used for client_secret_basic and client_secret_post authentication
  public static final String CLIENT_ID1 = TestEnvironment.CLIENT_ID1.getValue();
  public static final String CLIENT_SECRET1 = TestEnvironment.CLIENT_SECRET1.getValue();
  public static final String CLIENT_AUTH1 =
      ClientAuthenticationMethod.CLIENT_SECRET_BASIC.getValue();

  // Client2 is used for dynamic subject tokens
  public static final String CLIENT_ID2 = TestEnvironment.CLIENT_ID2.getValue();
  public static final String CLIENT_SECRET2 = TestEnvironment.CLIENT_SECRET2.getValue();
  public static final String CLIENT_AUTH2 =
      ClientAuthenticationMethod.CLIENT_SECRET_BASIC.getValue();

  // Client3 is used for dynamic actor tokens
  public static final String CLIENT_ID3 = "Client3";
  public static final String CLIENT_SECRET3 = "s€cr€t";
  public static final String CLIENT_AUTH3 =
      ClientAuthenticationMethod.CLIENT_SECRET_BASIC.getValue();

  // Client4 is used for "none" authentication (public client)
  public static final String CLIENT_ID4 = "Client4";
  public static final String CLIENT_AUTH4 = ClientAuthenticationMethod.NONE.getValue();

  public static final String USERNAME = TestEnvironment.USERNAME;
  public static final String PASSWORD = TestEnvironment.PASSWORD.getValue();

  public static final String SCOPE1 = TestEnvironment.SCOPE1.toString();
  public static final String SCOPE2 = TestEnvironment.SCOPE2.toString();

  // The intended audience for tokens issued by the clients is CLIENT_ID1 itself,
  // which will be used in token exchange scenarios.
  public static final String AUDIENCE = CLIENT_ID1;

  @Override
  public void beforeAll(ExtensionContext context) {
    context
        .getStore(ExtensionContext.Namespace.GLOBAL)
        .getOrComputeIfAbsent(KeycloakContainer.class.getName(), key -> createKeycloak());
  }

  @Override
  public void afterAll(ExtensionContext context) {
    KeycloakContainer keycloak =
        context
            .getStore(ExtensionContext.Namespace.GLOBAL)
            .remove(KeycloakContainer.class.getName(), KeycloakContainer.class);
    if (keycloak != null) {
      keycloak.close();
    }
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().equals(KeycloakContainer.class)
        || super.supportsParameter(parameterContext, extensionContext);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (parameterContext.getParameter().getType().equals(KeycloakContainer.class)) {
      return extensionContext
          .getStore(ExtensionContext.Namespace.GLOBAL)
          .getOrComputeIfAbsent(KeycloakContainer.class.getName(), key -> createKeycloak());
    }
    return super.resolveParameter(parameterContext, extensionContext);
  }

  @Override
  protected ImmutableTestEnvironment.Builder newTestEnvironmentBuilder(ExtensionContext context) {
    KeycloakContainer keycloak =
        context
            .getStore(ExtensionContext.Namespace.GLOBAL)
            .get(KeycloakContainer.class.getName(), KeycloakContainer.class);
    return TestEnvironment.builder()
        .unitTest(false)
        .accessTokenLifespan(KeycloakContainer.ACCESS_TOKEN_LIFESPAN)
        .serverRootUrl(keycloak.rootUrl())
        .authorizationServerUrl(keycloak.issuerUrl())
        .tokenEndpoint(keycloak.tokenEndpoint())
        // Do not use the default test tokens for Keycloak
        .subjectTokenString(Optional.empty())
        .actorTokenString(Optional.empty())
        .addAudiences(new Audience(AUDIENCE))
        // Keycloak does not yet support the "resource" parameter in token exchange
        .resources(List.of());
  }

  private KeycloakContainer createKeycloak() {
    TestCertificates certs = TestCertificates.instance();
    KeycloakContainer keycloak =
        new KeycloakContainer()
            .withScope(SCOPE1)
            .withScope(SCOPE2)
            .withUser(USERNAME, PASSWORD)
            .withClient(CLIENT_ID1, CLIENT_SECRET1, CLIENT_AUTH1)
            .withClient(CLIENT_ID2, CLIENT_SECRET2, CLIENT_AUTH2)
            .withClient(CLIENT_ID3, CLIENT_SECRET3, CLIENT_AUTH3)
            .withClient(CLIENT_ID4, null, CLIENT_AUTH4)
            // Include CLIENT_ID1 in the aud claim of tokens issued by these clients.
            // This will allow CLIENT_ID1 to exchange tokens obtained by all clients
            // (except CLIENT_ID4), since it's an authorized audience for them.
            // In Keycloak V2 standard token exchange, the audience parameter is restrictive:
            // it filters the resulting token's aud claim, so audiences must be declared in advance.
            .withClientAudience(CLIENT_ID1, AUDIENCE)
            .withClientAudience(CLIENT_ID2, AUDIENCE)
            .withClientAudience(CLIENT_ID3, AUDIENCE);
    keycloak.start();
    return keycloak;
  }
}
