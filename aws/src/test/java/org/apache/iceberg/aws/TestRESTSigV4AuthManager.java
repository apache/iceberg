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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.NoopAuthManager;
import org.apache.iceberg.rest.auth.OAuth2Manager;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
    when(delegate.initSession(any(), any())).thenReturn(Mockito.mock(OAuth2Util.AuthSession.class));
    RESTClient client = Mockito.mock(RESTClient.class);
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    AuthSession authSession = manager.initSession(client, awsProperties);
    assertThat(authSession)
        .isInstanceOf(RESTSigV4AuthSession.class)
        .extracting("delegate")
        .isInstanceOf(OAuth2Util.AuthSession.class);
  }

  @Test
  void catalogSession() {
    AuthManager delegate = Mockito.mock(AuthManager.class);
    when(delegate.catalogSession(any(), any()))
        .thenReturn(Mockito.mock(OAuth2Util.AuthSession.class));
    RESTClient client = Mockito.mock(RESTClient.class);
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    AuthSession authSession = manager.catalogSession(client, awsProperties);
    assertThat(authSession)
        .isInstanceOf(RESTSigV4AuthSession.class)
        .extracting("delegate")
        .isInstanceOf(OAuth2Util.AuthSession.class);
  }

  @Test
  void contextualSession() {
    AuthManager delegate = Mockito.mock(AuthManager.class);
    when(delegate.catalogSession(any(), any()))
        .thenReturn(Mockito.mock(OAuth2Util.AuthSession.class));
    when(delegate.contextualSession(any(), any()))
        .thenReturn(Mockito.mock(OAuth2Util.AuthSession.class));
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    manager.catalogSession(Mockito.mock(HTTPClient.class), awsProperties);
    AuthSession authSession =
        manager.contextualSession(
            Mockito.mock(SessionCatalog.SessionContext.class), Mockito.mock(AuthSession.class));
    assertThat(authSession)
        .isInstanceOf(RESTSigV4AuthSession.class)
        .extracting("delegate")
        .isInstanceOf(OAuth2Util.AuthSession.class);
  }

  @Test
  void tableSession() {
    AuthManager delegate = Mockito.mock(AuthManager.class);
    when(delegate.catalogSession(any(), any()))
        .thenReturn(Mockito.mock(OAuth2Util.AuthSession.class));
    when(delegate.tableSession(any(), any(), any()))
        .thenReturn(Mockito.mock(OAuth2Util.AuthSession.class));
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    manager.catalogSession(Mockito.mock(HTTPClient.class), awsProperties);
    AuthSession authSession =
        manager.tableSession(
            Mockito.mock(TableIdentifier.class), Map.of(), Mockito.mock(AuthSession.class));
    assertThat(authSession)
        .isInstanceOf(RESTSigV4AuthSession.class)
        .extracting("delegate")
        .isInstanceOf(OAuth2Util.AuthSession.class);
  }

  @Test
  void close() {
    AuthManager delegate = Mockito.mock(AuthManager.class);
    AuthManager manager = new RESTSigV4AuthManager("test", delegate);
    manager.close();
    Mockito.verify(delegate).close();
  }
}
