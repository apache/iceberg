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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
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
import org.apache.spark.SparkConf;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.mockito.Spy;

public class TestIcebergHiveConnectorDelegationTokenProvider {
  @Spy private IcebergHiveConnectorDelegationTokenProvider tokenProvider;
  private SparkConf sparkConf;
  private Configuration hadoopConf;

  @BeforeEach
  public void before() {
    tokenProvider = Mockito.spy(new IcebergHiveConnectorDelegationTokenProvider());
    sparkConf = new SparkConf();
    hadoopConf = new Configuration();
  }

  static class TestCase {
    String catalogType, uri, principal, catalogClass, auth, sasl;
    boolean tokenExists, renewalDisabled, allMet, expected;

    TestCase(
        String catalogType,
        String uri,
        String principal,
        String catalogClass,
        String auth,
        String sasl,
        boolean tokenExists,
        boolean renewalDisabled,
        boolean allMet,
        boolean expected) {
      this.catalogType = catalogType;
      this.uri = uri;
      this.principal = principal;
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
   * principal, catalog class, authentication method, SASL setting, whether a token exists, whether
   * renewal is disabled, and the expected result.
   */
  static Stream<TestCase> delegationTokenCases() {
    return Stream.of(
        // No catalog config
        new TestCase(null, null,
                null, null, null, null,
                false, false, false, false),
        // Catalog type not hive
        new TestCase(
            "custom",
            "thrift://localhost:9083",
            "test_principal",
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
            "test_principal",
            SparkCatalog.class.getName(),
            "kerberos",
            "true",
            false,
            false,
            false,
            false),
        // Missing principal
        new TestCase(
            "hive",
            "thrift://localhost:9083",
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
            "test_principal",
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
            "test_principal",
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
            "test_principal",
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
            "test_principal",
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
            "test_principal",
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

    if (tc.principal != null) {
      sparkConf.set("spark.sql.catalog.test.hive.metastore.kerberos.principal", tc.principal);
    }

    if (tc.catalogClass != null) {
      sparkConf.set("spark.sql.catalog.test", tc.catalogClass);
    }

    if (tc.renewalDisabled) {
      sparkConf.set("spark.sql.catalog.test.delegation.token.renewal.enable", "false");
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
  public void obtainDelegationTokens_ShouldWork_WithRealTokenFlow() throws IOException, TException {

    // Set up Spark and Hadoop configuration for Hive catalog with Kerberos
    sparkConf.set("spark.sql.catalog.test.type", "hive");
    sparkConf.set("spark.sql.catalog.test.uri", "thrift://localhost:9083");
    sparkConf.set("spark.sql.catalog.test.hive.metastore.kerberos.principal", "test_principal");
    sparkConf.set("spark.sql.catalog.test", SparkCatalog.class.getName());
    sparkConf.set("spark.sql.catalog.test.delegation.token.renewal.enable", "true");

    sparkConf.set("spark.submit.deployMode", "test_yarn");
    hadoopConf.set("hadoop.security.authentication", "kerberos");
    hadoopConf.set("hive.metastore.sasl.enabled", String.valueOf(true));

    // Simulate a real user and proxy user for UGI
    UserGroupInformation realUser = UserGroupInformation.createRemoteUser("testUser");
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("proxyUser", realUser);
    UserGroupInformation.setLoginUser(proxyUser);

    // Create a valid DelegationTokenIdentifier and serialize it
    DelegationTokenIdentifier tokenId =
        new DelegationTokenIdentifier(
            new Text("testOwner"), new Text("testRenewer"), new Text("testRealUser"));
    String encodedToken;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos)) {
      out.writeLong(123456789L); // Add valid data to avoid EOF
      tokenId.write(out);
      encodedToken = Base64.getEncoder().encodeToString(baos.toByteArray());
    }
    // Mock HiveMetaStoreClient to return the encoded token
    HiveMetaStoreClient hmsClient = mock(HiveMetaStoreClient.class);
    when(hmsClient.getDelegationToken(anyString(), anyString())).thenReturn(encodedToken);
    // Ensure createHmsClient returns the mock client
    doReturn(hmsClient).when(tokenProvider).createHmsClient(any(HiveConf.class));

    // Obtain delegation tokens
    Credentials credentials = new Credentials();
    tokenProvider.obtainDelegationTokens(hadoopConf, sparkConf, credentials);

    // Verify the token is added to credentials
    Assertions.assertNotNull(credentials.getToken(new Text("thrift://localhost:9083")));
  }
}
