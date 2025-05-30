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
package org.apache.iceberg.spark.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.SparkConf;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Option;

@ExtendWith(MockitoExtension.class)
public class TestIcebergHiveConnectorDelegationTokenProvider {
  private IcebergHiveConnectorDelegationTokenProvider tokenProvider;

  @BeforeEach
  public void before() {
    tokenProvider = new IcebergHiveConnectorDelegationTokenProvider();
  }

  private SparkConf createSparkConf(
      String catalog, String type, String uri, String principal, String catalogImpl) {
    SparkConf conf = new SparkConf();
    if (type != null) {
      conf.set("spark.sql.catalog." + catalog + ".type", type);
    }

    if (uri != null) {
      conf.set("spark.sql.catalog." + catalog + ".uri", uri);
    }

    if (principal != null) {
      conf.set("spark.sql.catalog." + catalog + ".hive.metastore.kerberos.principal", principal);
    }

    if (catalogImpl != null) {
      conf.set("spark.sql.catalog." + catalog, catalogImpl);
    }

    return conf;
  }

  private static Stream<Arguments> parameterCases() {
    return Stream.of(
        Arguments.of("test_catalog", null, null, null, null),
        Arguments.of(
            "test_catalog",
            "non_hive",
            "thrift://localhost:9083",
            "test_principal",
            SparkCatalog.class.getName()),
        Arguments.of(
            "test_catalog",
            "hive",
            null,
            "test_principal",
            "org.apache.iceberg.spark.SparkCatalog"),
        Arguments.of(
            "test_catalog", "hive", "thrift://localhost:9083", null, SparkCatalog.class.getName()),
        Arguments.of("test_catalog", "hive", "thrift://localhost:9083", "test_principal", null));
  }

  @ParameterizedTest
  @MethodSource("parameterCases")
  public void delegationTokensRequired_ShouldReturnFalse_WhenIncompleteParameters(
      String catalog, String type, String uri, String principal, String catalogClass) {
    SparkConf conf = createSparkConf(catalog, type, uri, principal, catalogClass);

    assertFalse(tokenProvider.delegationTokensRequired(conf, new Configuration()));
  }

  @Test
  public void delegationTokensRequired_ShouldReturnFalse_WhenSecurityDisabled() {
    SparkConf conf =
        createSparkConf(
            "test_catalog",
            "hive",
            "thrift://localhost:9083",
            "test_principal",
            SparkSessionCatalog.class.getName());
    try (MockedStatic<UserGroupInformation> ugiStatic = mockStatic(UserGroupInformation.class)) {
      ugiStatic.when(UserGroupInformation::isSecurityEnabled).thenReturn(false);

      assertFalse(tokenProvider.delegationTokensRequired(conf, new Configuration()));
    }
  }

  @Test
  public void delegationTokensRequire_ShouldReturnTrue_WhenNoTokenExists() throws IOException {
    SparkConf conf =
        createSparkConf(
            "test_catalog",
            "hive",
            "thrift://localhost:9083",
            "test_principal",
            SparkCatalog.class.getName());
    try (MockedStatic<UserGroupInformation> ugiStatic = mockStatic(UserGroupInformation.class)) {
      ugiStatic.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);
      UserGroupInformation ugi = mock(UserGroupInformation.class);
      Credentials credentials = mock(Credentials.class);
      when(ugi.getCredentials()).thenReturn(credentials);
      when(credentials.getToken(new Text("thrift://localhost:9083"))).thenReturn(null);
      ugiStatic.when(UserGroupInformation::getCurrentUser).thenReturn(ugi);

      assertTrue(tokenProvider.delegationTokensRequired(conf, new Configuration()));
    }
  }

  @Test
  public void delegationTokensRequire_ShouldReturnFalse_WhenTokenExists() throws IOException {
    SparkConf conf =
        createSparkConf(
            "test_catalog",
            "hive",
            "thrift://localhost:9083",
            "test_principal",
            SparkCatalog.class.getName());
    try (MockedStatic<UserGroupInformation> ugiStatic = mockStatic(UserGroupInformation.class)) {
      ugiStatic.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);
      UserGroupInformation ugi = mock(UserGroupInformation.class);
      Credentials credentials = mock(Credentials.class);
      when(ugi.getCredentials()).thenReturn(credentials);
      Token<?> token = new Token<>();
      when(credentials.getToken(new Text("thrift://localhost:9083"))).thenReturn((Token) token);
      ugiStatic.when(UserGroupInformation::getCurrentUser).thenReturn(ugi);

      assertFalse(tokenProvider.delegationTokensRequired(conf, new Configuration()));
    }
  }

  @Test
  public void obtainDelegationTokens__ShouldReturnTrue_ForMultipleCatalogs() {
    SparkConf conf =
        new SparkConf()
            .set("spark.sql.catalog.test_catalog1.type", "hive")
            .set("spark.sql.catalog.test_catalog1.uri", "thrift://localhost:9083")
            .set("spark.sql.catalog.test_catalog1.hive.metastore.kerberos.principal", "principal1")
            .set("spark.sql.catalog.test_catalog1", SparkCatalog.class.getName())
            .set("spark.sql.catalog.test_catalog2.type", "hive")
            .set("spark.sql.catalog.test_catalog2.uri", "thrift://localhost:9084")
            .set("spark.sql.catalog.test_catalog2.hive.metastore.kerberos.principal", "principal2")
            .set("spark.sql.catalog.test_catalog2", SparkSessionCatalog.class.getName());
    Configuration hadoopConf = new Configuration();
    Credentials creds = new Credentials();

    // mock UGI
    UserGroupInformation mockCurrentUser = mock(UserGroupInformation.class);
    UserGroupInformation mockRealUser = mock(UserGroupInformation.class);
    when(mockCurrentUser.getRealUser()).thenReturn(mockRealUser);
    when(mockCurrentUser.getUserName()).thenReturn("testUser");
    when(mockCurrentUser.getCredentials()).thenReturn(creds);
    // Mock real user performing privileged actions
    try {
      when(mockRealUser.doAs(any(PrivilegedExceptionAction.class)))
          .then(
              invocation -> {
                PrivilegedExceptionAction<?> action = invocation.getArgument(0);
                return action.run();
              });
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    try (MockedStatic<UserGroupInformation> staticUGI = mockStatic(UserGroupInformation.class)) {
      staticUGI.when(UserGroupInformation::getCurrentUser).thenReturn(mockCurrentUser);
      staticUGI.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);

      // mock HiveMetaStoreClient
      HiveMetaStoreClient mockHmsClient = mock(HiveMetaStoreClient.class);
      try {
        when(mockHmsClient.getDelegationToken(anyString(), anyString())).thenReturn("tokenString");
      } catch (TException e) {
        throw new RuntimeException(e);
      }

      // mock provider、createHmsClient and buildHiveConf
      IcebergHiveConnectorDelegationTokenProvider provider =
          spy(new IcebergHiveConnectorDelegationTokenProvider());
      try {
        doReturn(mockHmsClient).when(provider).createHmsClient(any(HiveConf.class));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      doReturn(Optional.of(new HiveConf(hadoopConf, HiveConf.class)))
          .when(provider)
          .buildHiveConf(any(), any(), anyString());

      // mock Token
      try (MockedConstruction<Token> mockedToken =
          mockConstruction(
              Token.class,
              (mock, context) -> doNothing().when(mock).decodeFromUrlString(anyString()))) {
        // Obtain delegation tokens and verify the result
        Option<Object> result = provider.obtainDelegationTokens(hadoopConf, conf, creds);

        assertTrue(result.isEmpty());
        assertEquals(2, ((long) creds.getAllTokens().size()));
      }
    }
  }
}
