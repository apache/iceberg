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
package org.apache.iceberg.gcp.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.GoogleCredentials;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestGoogleAuthManager {

  private static final String MANAGER_NAME = "testManager";
  @Mock private RESTClient restClient;
  @Mock private GoogleCredentials credentials;
  @Mock private GoogleCredentials credentialsFromFile;

  private GoogleAuthManager authManager;
  private MockedStatic<GoogleCredentials> mockedStaticCredentials;

  @TempDir File tempDir;
  private File credentialFile;

  @BeforeEach
  public void beforeEach() throws IOException {
    authManager = new GoogleAuthManager(MANAGER_NAME);
    mockedStaticCredentials = Mockito.mockStatic(GoogleCredentials.class);
    credentialFile = new File(tempDir, "fake-creds.json");
    Files.write(credentialFile.toPath(), "{\"type\": \"service_account\"}".getBytes());
  }

  @AfterEach
  public void afterEach() {
    mockedStaticCredentials.close();
  }

  @Test
  public void providesCorrectManagerName() {
    assertThat(authManager.name()).isEqualTo(MANAGER_NAME);
  }

  @Test
  public void buildsCatalogSessionFromCredentialsFile() {
    String customScopes = "scope1,scope2";
    Map<String, String> properties =
        ImmutableMap.of(
            GoogleAuthManager.GCP_CREDENTIALS_PATH_PROPERTY,
            credentialFile.getAbsolutePath(),
            GoogleAuthManager.GCP_SCOPES_PROPERTY,
            customScopes);

    mockedStaticCredentials
        .when(() -> GoogleCredentials.fromStream(any(FileInputStream.class)))
        .thenReturn(credentialsFromFile);
    when(credentialsFromFile.createScoped(anyList())).thenReturn(credentials);

    AuthSession session = authManager.catalogSession(restClient, properties);

    assertThat(session).isInstanceOf(GoogleAuthSession.class);
    verify(credentialsFromFile).createScoped(ImmutableList.of("scope1", "scope2"));
  }

  @Test
  public void buildsCatalogSessionWithEmptyScopes() {
    Map<String, String> properties =
        ImmutableMap.of(
            GoogleAuthManager.GCP_CREDENTIALS_PATH_PROPERTY,
            credentialFile.getAbsolutePath(),
            GoogleAuthManager.GCP_SCOPES_PROPERTY,
            "");

    mockedStaticCredentials
        .when(() -> GoogleCredentials.fromStream(any(FileInputStream.class)))
        .thenReturn(credentialsFromFile);
    when(credentialsFromFile.createScoped(anyList())).thenReturn(credentials);

    AuthSession session = authManager.catalogSession(restClient, properties);

    assertThat(session).isInstanceOf(GoogleAuthSession.class);
    verify(credentialsFromFile).createScoped(ImmutableList.of());
  }

  @Test
  public void throwsUncheckedIOExceptionOnCredentialsFileError() {
    Map<String, String> properties =
        ImmutableMap.of(
            GoogleAuthManager.GCP_CREDENTIALS_PATH_PROPERTY, credentialFile.getAbsolutePath());

    mockedStaticCredentials
        .when(() -> GoogleCredentials.fromStream(any(FileInputStream.class)))
        .thenThrow(new IOException("Simulated stream loading failure"));

    assertThatThrownBy(() -> authManager.catalogSession(restClient, properties))
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("Failed to load Google credentials");
  }

  @Test
  public void buildsCatalogSessionUsingADC() {
    mockedStaticCredentials
        .when(GoogleCredentials::getApplicationDefault)
        .thenReturn(credentialsFromFile);

    when(credentialsFromFile.createScoped(anyList())).thenReturn(credentials);

    AuthSession session = authManager.catalogSession(restClient, Collections.emptyMap());

    assertThat(session).isInstanceOf(GoogleAuthSession.class);
    mockedStaticCredentials.verify(GoogleCredentials::getApplicationDefault, times(1));
  }

  @Test
  public void throwsUncheckedIOExceptionOnADCError() {
    mockedStaticCredentials
        .when(GoogleCredentials::getApplicationDefault)
        .thenThrow(new IOException("ADC unavailable"));

    assertThatThrownBy(() -> authManager.catalogSession(restClient, Collections.emptyMap()))
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("Failed to load Google credentials");
  }

  @Test
  public void initializationOccursOnlyOnce() {
    mockedStaticCredentials
        .when(GoogleCredentials::getApplicationDefault)
        .thenReturn(credentialsFromFile);

    when(credentialsFromFile.createScoped(anyList())).thenReturn(credentials);

    authManager.catalogSession(restClient, Collections.emptyMap());
    authManager.catalogSession(restClient, Collections.emptyMap());

    mockedStaticCredentials.verify(GoogleCredentials::getApplicationDefault, times(1));
  }

  @Test
  public void initSessionDelegatesToCatalogSession() {
    GoogleAuthManager spyManager = spy(authManager);
    AuthSession mockSession = mock(GoogleAuthSession.class);
    Map<String, String> props = Collections.emptyMap();

    doReturn(mockSession).when(spyManager).catalogSession(restClient, props);

    AuthSession resultSession = spyManager.initSession(restClient, props);
    assertThat(resultSession).isSameAs(mockSession);
    verify(spyManager).catalogSession(restClient, props);
  }

  @Test
  public void testLoadAuthManager() {
    AuthManager manager =
        AuthManagers.loadAuthManager(
            "test", Map.of(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_GOOGLE));
    assertThat(manager).isInstanceOf(GoogleAuthManager.class);
  }
}
