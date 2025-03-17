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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.ImmutableAuthScopes;
import org.apache.iceberg.rest.auth.NoopAuthManager;
import org.apache.iceberg.rest.auth.OAuth2Manager;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

class TestRESTSigV4AuthManager {

  private final Map<String, String> awsProperties =
      Map.of(
          // CI environment doesn't have credentials, but a value must be set for signing
          AwsProperties.REST_SIGNER_REGION,
          "us-west-2",
          AwsProperties.REST_ACCESS_KEY_ID,
          "id",
          AwsProperties.REST_SECRET_ACCESS_KEY,
          "secret");

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
    AuthManager delegate = Mockito.mock(AuthManager.class);
    when(delegate.authSession(any())).thenReturn(Mockito.mock(OAuth2Util.AuthSession.class));
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    AuthSession authSession = manager.authSession(ImmutableAuthScopes.Initial.of(awsProperties));
    assertAuthSession(authSession);
  }

  @Test
  void authSession() {
    AuthManager delegate = Mockito.mock(AuthManager.class);
    when(delegate.authSession(any())).thenReturn(Mockito.mock(OAuth2Util.AuthSession.class));
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    AuthSession authSession = manager.authSession(ImmutableAuthScopes.Catalog.of(awsProperties));
    assertAuthSession(authSession);
  }

  @Test
  void contextualSession() {
    AuthManager delegate = Mockito.mock(AuthManager.class);
    when(delegate.authSession(any())).thenReturn(Mockito.mock(OAuth2Util.AuthSession.class));
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    AuthSession catalogSession = manager.authSession(ImmutableAuthScopes.Catalog.of(awsProperties));
    AuthSession authSession =
        manager.authSession(
            ImmutableAuthScopes.Contextual.of(
                Mockito.mock(SessionCatalog.SessionContext.class), catalogSession));
    assertAuthSession(authSession);
  }

  @Test
  void tableSession() {
    AuthManager delegate = Mockito.mock(AuthManager.class);
    when(delegate.authSession(any())).thenReturn(Mockito.mock(OAuth2Util.AuthSession.class));
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    AuthSession catalogSession = manager.authSession(ImmutableAuthScopes.Catalog.of(awsProperties));
    AuthSession authSession =
        manager.authSession(
            ImmutableAuthScopes.Table.of(
                Mockito.mock(TableIdentifier.class), Map.of(), catalogSession));
    assertAuthSession(authSession);
  }

  private static void assertAuthSession(AuthSession authSession) {
    assertThat(authSession)
        .isInstanceOf(RESTSigV4AuthSession.class)
        .extracting("delegate")
        .isInstanceOf(OAuth2Util.AuthSession.class);
    assertThat(authSession).extracting("signer").isNotNull();
    assertThat(authSession).extracting("signingRegion").isEqualTo(Region.of("us-west-2"));
    assertThat(authSession)
        .extracting("credentialsProvider")
        .asInstanceOf(type(StaticCredentialsProvider.class))
        .extracting(StaticCredentialsProvider::resolveCredentials)
        .isEqualTo(AwsBasicCredentials.create("id", "secret"));
  }

  @Test
  void close() {
    AuthManager delegate = Mockito.mock(AuthManager.class);
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    manager.close();
    Mockito.verify(delegate).close();
  }
}
