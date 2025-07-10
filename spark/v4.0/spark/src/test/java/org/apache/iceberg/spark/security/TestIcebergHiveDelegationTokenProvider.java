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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.SparkConf;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.mockito.Spy;

public class TestIcebergHiveDelegationTokenProvider {
  @Spy private IcebergHiveDelegationTokenProvider tokenProvider;
  private SparkConf sparkConf;
  private Configuration hadoopConf;

  @BeforeEach
  public void before() {
    tokenProvider = Mockito.spy(new IcebergHiveDelegationTokenProvider());
    sparkConf = new SparkConf();
    hadoopConf = new Configuration();
  }

  static class TestCase {
    String catalogType;
    String uri;
    String catalogClass;
    String auth;
    String sasl;
    boolean tokenExists;
    boolean renewalDisabled;
    boolean allMet;
    boolean expected;

    TestCase(
        String catalogType,
        String uri,
        String catalogClass,
        String auth,
        String sasl,
        boolean tokenExists,
        boolean renewalDisabled,
        boolean allMet,
        boolean expected) {
      this.catalogType = catalogType;
      this.uri = uri;
      this.catalogClass = catalogClass;
      this.auth = auth;
      this.sasl = sasl;
      this.tokenExists = tokenExists;
      this.renewalDisabled = renewalDisabled;
      this.allMet = allMet;
      this.expected = expected;
    }
  }

  /**
   * Provides test cases for delegation token requirements. Each case includes catalog type, URI,
   * catalog class, authentication method, SASL setting, whether a token exists, whether renewal is
   * disabled, and the expected result.
   */
  static Stream<TestCase> delegationTokenCases() {
    return Stream.of(
        // No catalog config
        new TestCase(null, null, null, null, null, false, false, false, false),
        // Catalog type not hive
        new TestCase(
            "custom",
            "thrift://localhost:9083",
            SparkCatalog.class.getName(),
            "kerberos",
            "true",
            false,
            false,
            false,
            false),
        // Missing metastore uri
        new TestCase(
            "hive",
            null,
            SparkCatalog.class.getName(),
            "kerberos",
            "true",
            false,
            false,
            false,
            false),
        // Delegation token renewal disabled
        new TestCase(
            "hive",
            "thrift://localhost:9083",
            SparkCatalog.class.getName(),
            "kerberos",
            "true",
            false,
            true,
            false,
            false),
        // Non-kerberos authentication
        new TestCase(
            "hive",
            "thrift://localhost:9083",
            SparkCatalog.class.getName(),
            "simple",
            "true",
            false,
            false,
            false,
            false),
        // SASL disabled
        new TestCase(
            "hive",
            "thrift://localhost:9083",
            SparkCatalog.class.getName(),
            "kerberos",
            "false",
            false,
            false,
            false,
            false),
        // Token already exists
        new TestCase(
            "hive",
            "thrift://localhost:9083",
            SparkCatalog.class.getName(),
            "kerberos",
            "true",
            true,
            false,
            false,
            false),
        // All conditions met
        new TestCase(
            "hive",
            "thrift://localhost:9083",
            SparkCatalog.class.getName(),
            "kerberos",
            "true",
            false,
            false,
            true,
            true));
  }

  @ParameterizedTest
  @MethodSource("delegationTokenCases")
  public void testDelegationTokensRequired(TestCase tc) throws Exception {

    if (tc.catalogType != null) {
      sparkConf.set("spark.sql.catalog.test.type", tc.catalogType);
    }

    if (tc.uri != null) {
      sparkConf.set("spark.sql.catalog.test.uri", tc.uri);
    }

    if (tc.catalogClass != null) {
      sparkConf.set("spark.sql.catalog.test", tc.catalogClass);
    }

    if (tc.renewalDisabled) {
      sparkConf.set("spark.sql.catalog.test.delegation.token.renewal.enabled", "false");
    }

    if (tc.auth != null) {
      hadoopConf.set("hadoop.security.authentication", tc.auth);
    }

    if (tc.sasl != null) {
      hadoopConf.set("hive.metastore.sasl.enabled", tc.sasl);
    }

    // Simulate UGI
    UserGroupInformation realUser = UserGroupInformation.createRemoteUser("testUser");
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("proxyUser", realUser);
    UserGroupInformation.setLoginUser(proxyUser);

    if (tc.tokenExists && tc.uri != null) {
      proxyUser.addToken(
          new org.apache.hadoop.io.Text(tc.uri), new org.apache.hadoop.security.token.Token<>());
    }

    boolean result = tokenProvider.delegationTokensRequired(sparkConf, hadoopConf);
    assertEquals(tc.expected, result);
  }

  @Test
  public void testDelegationTokensRequiredWithIllegalCatalogName_ShouldReturnFalse() {
    // Configure catalog with illegal name
    sparkConf.set("spark.sql.catalog.test.test1.type", "hive");
    sparkConf.set("spark.sql.catalog.test.test1", "thrift://localhost:9083");
    sparkConf.set("spark.sql.catalog.test.test1.uri", SparkCatalog.class.getName());

    hadoopConf.set("hadoop.security.authentication", "kerberos");
    hadoopConf.set("hive.metastore.sasl.enabled", String.valueOf(true));
    // Set up user info
    UserGroupInformation realUser = UserGroupInformation.createRemoteUser("testUser");
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("proxyUser", realUser);
    UserGroupInformation.setLoginUser(proxyUser);

    boolean result = tokenProvider.delegationTokensRequired(sparkConf, hadoopConf);
    assertFalse(result);
  }

  @Test
  public void obtainDelegationTokens_TwoCatalogs_OneFails_ShouldNotBlockOther()
      throws IOException, TException {
    // Configure two catalogs
    sparkConf.set("spark.sql.catalog.test1.type", "hive");
    sparkConf.set("spark.sql.catalog.test1.uri", "thrift://localhost:9083");
    sparkConf.set("spark.sql.catalog.test1", SparkCatalog.class.getName());

    sparkConf.set("spark.sql.catalog.test2.type", "hive");
    sparkConf.set("spark.sql.catalog.test2.uri", "thrift://localhost:9084");
    sparkConf.set("spark.sql.catalog.test2", SparkSessionCatalog.class.getName());

    hadoopConf.set("hadoop.security.authentication", "kerberos");
    hadoopConf.set("hive.metastore.sasl.enabled", String.valueOf(true));

    // Set up user info
    UserGroupInformation realUser = UserGroupInformation.createRemoteUser("testUser");
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("proxyUser", realUser);
    UserGroupInformation.setLoginUser(proxyUser);

    // Mock HMS client for test1 (failure case)
    HiveMetaStoreClient hmsClientTest1 = mock(HiveMetaStoreClient.class);
    when(hmsClientTest1.getDelegationToken(anyString(), anyString()))
        .thenThrow(new TException("Token fetch failed"));

    // Mock HMS client for test2 (success case)
    HiveMetaStoreClient hmsClientTest2 = mock(HiveMetaStoreClient.class);
    DelegationTokenIdentifier tokenId =
        new DelegationTokenIdentifier(
            new Text("testOwner"), new Text("testRenewer"), new Text("testRealUser"));
    String encodedToken;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos)) {
      out.writeLong(123456789L);
      tokenId.write(out);
      encodedToken = Base64.getEncoder().encodeToString(baos.toByteArray());
    }
    when(hmsClientTest2.getDelegationToken(anyString(), anyString())).thenReturn(encodedToken);

    // Return different clients based on metastore URI
    doAnswer(
            invocation -> {
              HiveConf conf = invocation.getArgument(0);
              String uri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname);
              if ("thrift://localhost:9083".equals(uri)) {
                return hmsClientTest1;
              } else if ("thrift://localhost:9084".equals(uri)) {
                return hmsClientTest2;
              }
              return null;
            })
        .when(tokenProvider)
        .createHmsClient(any(HiveConf.class));

    // Execute and verify
    Credentials credentials = new Credentials();
    tokenProvider.obtainDelegationTokens(hadoopConf, sparkConf, credentials);

    assertNotNull(credentials.getToken(new Text("thrift://localhost:9084")));
    assertNull(credentials.getToken(new Text("thrift://localhost:9083")));
  }
}
