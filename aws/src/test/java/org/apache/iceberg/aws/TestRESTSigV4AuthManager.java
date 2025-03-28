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
package org.apache.iceberg.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import java.util.Map;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthConfig;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.NoopAuthManager;
import org.apache.iceberg.rest.auth.OAuth2Manager;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

class TestRESTSigV4AuthManager {

  private final Map<String, String> catalogProperties =
      Map.of(
          // CI environment doesn't have credentials, but a value must be set for signing
          AwsProperties.REST_SIGNER_REGION,
          "us-west-2",
          AwsProperties.REST_ACCESS_KEY_ID,
          "id",
          AwsProperties.REST_SECRET_ACCESS_KEY,
          "secret",
          // OAuth2 properties
          OAuth2Properties.TOKEN,
          "token1",
          OAuth2Properties.SCOPE,
          "scope1");

  @Test
  void create() {
    AuthManager manager =
        AuthManagers.loadAuthManager(
            "test", Map.of(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_SIGV4));
    assertThat(manager)
        .isInstanceOf(RESTSigV4AuthManager.class)
        .extracting("delegate")
        .isInstanceOf(OAuth2Manager.class);
  }

  @Test
  void createLegacy() {
    AuthManager manager =
        AuthManagers.loadAuthManager("test", Map.of("rest.sigv4-enabled", "true"));
    assertThat(manager)
        .isInstanceOf(RESTSigV4AuthManager.class)
        .extracting("delegate")
        .isInstanceOf(OAuth2Manager.class);
  }

  @Test
  void createCustomDelegate() {
    AuthManager manager =
        AuthManagers.loadAuthManager(
            "test",
            Map.of(
                AuthProperties.AUTH_TYPE,
                AuthProperties.AUTH_TYPE_SIGV4,
                AuthProperties.SIGV4_DELEGATE_AUTH_TYPE,
                AuthProperties.AUTH_TYPE_NONE));
    assertThat(manager)
        .isInstanceOf(RESTSigV4AuthManager.class)
        .extracting("delegate")
        .isInstanceOf(NoopAuthManager.class);
  }

  @Test
  @SuppressWarnings("resource")
  void createInvalidCustomDelegate() {
    assertThatThrownBy(
            () ->
                AuthManagers.loadAuthManager(
                    "test",
                    Map.of(
                        AuthProperties.AUTH_TYPE,
                        AuthProperties.AUTH_TYPE_SIGV4,
                        AuthProperties.SIGV4_DELEGATE_AUTH_TYPE,
                        AuthProperties.AUTH_TYPE_SIGV4)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot delegate a SigV4 auth manager to another SigV4 auth manager");
  }

  @Test
  void initSession() {
    AuthManager delegate =
        AuthManagers.loadAuthManager(
            "test", Map.of(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2));
    RESTClient client = Mockito.mock(RESTClient.class);
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    AuthSession authSession = manager.initSession(client, catalogProperties);
    checkSession(authSession, "us-west-2", "id", "secret", false);
  }

  @Test
  void catalogSession() {
    AuthManager delegate =
        AuthManagers.loadAuthManager(
            "test", Map.of(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2));
    RESTClient client = Mockito.mock(RESTClient.class);
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    AuthSession authSession = manager.catalogSession(client, catalogProperties);
    checkSession(authSession, "us-west-2", "id", "secret", true);
  }

  @Test
  void contextualSession() {
    AuthManager delegate =
        AuthManagers.loadAuthManager(
            "test", Map.of(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2));
    RESTClient client = Mockito.mock(RESTClient.class);
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            "context1",
            "identity1",
            Map.of(
                AwsProperties.REST_ACCESS_KEY_ID,
                "id2",
                AwsProperties.REST_SECRET_ACCESS_KEY,
                "secret2"),
            Map.of(AwsProperties.REST_SIGNER_REGION, "us-east-1"));
    AuthSession authSession = manager.contextualSession(context, catalogSession);
    checkSession(authSession, "us-east-1", "id2", "secret2", true);
  }

  @Test
  void tableSession() {
    AuthManager delegate =
        AuthManagers.loadAuthManager(
            "test", Map.of(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2));
    RESTClient client = Mockito.mock(RESTClient.class);
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
    Map<String, String> tableProperties =
        Map.of(
            AwsProperties.REST_ACCESS_KEY_ID,
            "id2",
            AwsProperties.REST_SECRET_ACCESS_KEY,
            "secret2",
            AwsProperties.REST_SIGNER_REGION,
            "us-east-1");
    AuthSession authSession =
        manager.tableSession(TableIdentifier.of("table1"), tableProperties, catalogSession);
    checkSession(authSession, "us-east-1", "id2", "secret2", true);
  }

  @Test
  void close() {
    AuthManager delegate = Mockito.mock(AuthManager.class);
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    manager.close();
    Mockito.verify(delegate).close();
  }

  private static void checkSession(
      AuthSession authSession,
      String expectedRegion,
      String expectedAccessKeyId,
      String expectedSecretAccessKey,
      boolean expectedKeepRefreshed) {
    assertThat(authSession).isInstanceOf(RESTSigV4AuthSession.class);
    RESTSigV4AuthSession sigV4AuthSession = (RESTSigV4AuthSession) authSession;
    // Check AWS properties present
    assertThat(sigV4AuthSession).extracting("signingRegion").isEqualTo(Region.of(expectedRegion));
    assertThat(sigV4AuthSession)
        .extracting("credentialsProvider")
        .asInstanceOf(type(AwsCredentialsProvider.class))
        .extracting(AwsCredentialsProvider::resolveCredentials)
        .isEqualTo(AwsBasicCredentials.create(expectedAccessKeyId, expectedSecretAccessKey));
    // Check OAuth2 properties present in delegate
    assertThat(sigV4AuthSession.delegate())
        .asInstanceOf(type(OAuth2Util.AuthSession.class))
        .extracting("config")
        .asInstanceOf(type(AuthConfig.class))
        .isEqualTo(
            AuthConfig.builder()
                .token("token1")
                .tokenType(OAuth2Properties.ACCESS_TOKEN_TYPE)
                .scope("scope1")
                .keepRefreshed(expectedKeepRefreshed)
                .optionalOAuthParams(Map.of(OAuth2Properties.SCOPE, "scope1"))
                .build());
  }
}
