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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link IcebergHiveDelegationTokenProvider}. */
public class TestIcebergHiveDelegationTokenProvider {

  private static final String CATALOG1 = "catalog1";
  private static final String CATALOG2 = "catalog2";
  private static final String METASTORE_URI_1 = "thrift://localhost:9083";
  private static final String METASTORE_URI_2 = "thrift://localhost:9084";
  IcebergHiveDelegationTokenProvider provider = new IcebergHiveDelegationTokenProvider();

  @Test
  public void testServiceName() {
    assertThat(provider.serviceName()).isEqualTo("iceberg_hive");
  }

  @Test
  public void testExtractIcebergCatalogNames() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.sql.catalog.cat1", SparkCatalog.class.getName());
    sparkConf.set("spark.sql.catalog.cat2", SparkSessionCatalog.class.getName());
    sparkConf.set("spark.sql.catalog.custom", "com.example.CustomCatalog");
    sparkConf.set("spark.sql.catalog.invalid.name", SparkCatalog.class.getName());
    sparkConf.set("spark.sql.catalog.", SparkCatalog.class.getName());

    var catalogNames = provider.extractIcebergCatalogNames(sparkConf);
    // Verify only valid Iceberg catalog names are extracted
    // invalid.name contains dot and should be filtered by isValidCatalogName
    // custom uses non-Iceberg class and should be filtered
    // empty name should be filtered by isValidCatalogName
    assertThat(catalogNames).containsExactlyInAnyOrder("cat1", "cat2");
    assertThat(catalogNames).doesNotContain("invalid.name", "custom", "");

    // Boundary: no catalogs configured
    assertThat(provider.extractIcebergCatalogNames(new SparkConf())).isEmpty();
  }

  @Test
  public void testBuildHiveConf() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.sql.catalog.test.uri", METASTORE_URI_1);
    sparkConf.set("spark.sql.catalog.test.warehouse", "/tmp/wh");
    sparkConf.set("spark.sql.catalog.test.custom.prop", "value");

    var hiveConfOpt = provider.buildHiveConf(sparkConf, new Configuration(), "test");

    assertThat(hiveConfOpt).isPresent();
    HiveConf hiveConf = hiveConfOpt.get();
    // Verify URI is mapped to hive.metastore.uris
    assertThat(hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname)).isEqualTo(METASTORE_URI_1);
    // Verify other properties are passed through
    assertThat(hiveConf.get("warehouse")).isEqualTo("/tmp/wh");
    assertThat(hiveConf.get("custom.prop")).isEqualTo("value");
    // Verify METASTORE_FASTPATH is disabled to avoid blocking
    assertThat(hiveConf.get(HiveConf.ConfVars.METASTORE_FASTPATH.varname)).isEqualTo("false");

    // Boundary: empty URI should still create HiveConf
    sparkConf.set("spark.sql.catalog.test.uri", "");
    hiveConfOpt = provider.buildHiveConf(sparkConf, new Configuration(), "test");
    assertThat(hiveConfOpt).isPresent();
    assertThat(hiveConfOpt.get().get(HiveConf.ConfVars.METASTOREURIS.varname)).isEmpty();

    // Boundary: non-existent catalog should still create HiveConf
    hiveConfOpt = provider.buildHiveConf(new SparkConf(), new Configuration(), "nonexistent");
    assertThat(hiveConfOpt).isPresent();
  }

  /** Test scenarios where delegation token is NOT required */
  static Stream<Arguments> tokenNotRequiredScenarios() {
    return Stream.of(
        Arguments.of("empty URI", "", "hive", "kerberos", "true", "true", null),
        Arguments.of("blank URI", "   ", "hive", "kerberos", "true", "true", null),
        Arguments.of(
            "non-hive catalog", METASTORE_URI_1, "custom", "kerberos", "true", "true", null),
        Arguments.of(
            "renewal disabled", METASTORE_URI_1, "hive", "kerberos", "true", "false", null),
        Arguments.of("simple auth", METASTORE_URI_1, "hive", "simple", "true", "true", null),
        Arguments.of("sasl disabled", METASTORE_URI_1, "hive", "kerberos", "false", "true", null),
        Arguments.of(
            "token exists with URI signature",
            METASTORE_URI_1,
            "hive",
            "kerberos",
            "true",
            "true",
            METASTORE_URI_1),
        Arguments.of(
            "token exists with custom signature",
            METASTORE_URI_1,
            "hive",
            "kerberos",
            "true",
            "true",
            "custom_sig"),
        Arguments.of(
            "empty signature falls back to URI",
            METASTORE_URI_1,
            "hive",
            "kerberos",
            "true",
            "true",
            ""));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("tokenNotRequiredScenarios")
  public void testDelegationTokenNotRequired(
      String scenario,
      String uri,
      String catalogType,
      String authType,
      String saslEnabled,
      String renewalEnabled,
      String tokenSignature) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.sql.catalog.test", SparkCatalog.class.getName());
    sparkConf.set("spark.sql.catalog.test.type", catalogType);
    sparkConf.set("spark.sql.catalog.test.uri", uri);
    if (renewalEnabled != null) {
      sparkConf.set("spark.sql.catalog.test.delegation.token.renewal.enabled", renewalEnabled);
    }

    // Handle custom token signature configuration
    if (tokenSignature != null && !tokenSignature.equals(uri)) {
      sparkConf.set("spark.sql.catalog.test.hive.metastore.token.signature", tokenSignature);
    }

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("hadoop.security.authentication", authType);
    hadoopConf.set("hive.metastore.sasl.enabled", saslEnabled);

    // Setup proxy user with token if tokenSignature is provided
    if (tokenSignature != null) {
      UserGroupInformation testUser = UserGroupInformation.createRemoteUser("testUser");
      UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("proxyUser", testUser);
      UserGroupInformation.setLoginUser(proxyUser);
      // Empty signature falls back to URI
      String effectiveSignature = tokenSignature.isEmpty() ? uri : tokenSignature;
      proxyUser.addToken(
          new Text(effectiveSignature), new org.apache.hadoop.security.token.Token<>());
    }

    assertThat(provider.delegationTokensRequired(sparkConf, hadoopConf))
        .as("Token should not be required for scenario: %s", scenario)
        .isFalse();
  }

  /** Test scenarios for token requirement based on deploy mode and user type */
  static Stream<Arguments> tokenRequiredByDeployModeScenarios() {
    return Stream.of(
        Arguments.of("proxy user", null, null, true),
        Arguments.of("cluster mode without keytab", "cluster", null, true),
        Arguments.of("cluster mode with keytab", "cluster", "/path/to/keytab", false),
        Arguments.of("client mode", "client", null, false));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("tokenRequiredByDeployModeScenarios")
  public void testDelegationTokenRequiredByDeployMode(
      String scenario, String deployMode, String keytabPath, boolean expectedRequired) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.sql.catalog.test", SparkCatalog.class.getName());
    sparkConf.set("spark.sql.catalog.test.type", "hive");
    sparkConf.set("spark.sql.catalog.test.uri", METASTORE_URI_1);
    if (deployMode != null) {
      sparkConf.set("spark.submit.deployMode", deployMode);
    }

    if (keytabPath != null) {
      sparkConf.set("spark.kerberos.keytab", keytabPath);
    }

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("hadoop.security.authentication", "kerberos");
    hadoopConf.set("hive.metastore.sasl.enabled", "true");

    UserGroupInformation testUser = UserGroupInformation.createRemoteUser("testUser");
    // Proxy user scenario
    if ("proxy user".equals(scenario)) {
      UserGroupInformation proxyUser = UserGroupInformation.createProxyUser("proxyUser", testUser);
      UserGroupInformation.setLoginUser(proxyUser);
    } else {
      UserGroupInformation.setLoginUser(testUser);
    }

    assertThat(provider.delegationTokensRequired(sparkConf, hadoopConf))
        .as("Token requirement for scenario: %s", scenario)
        .isEqualTo(expectedRequired);
  }

  @Test
  public void testObtainDelegationTokensWithNoCatalogs() {
    Credentials creds = new Credentials();
    provider.obtainDelegationTokens(new Configuration(), new SparkConf(), creds);
    assertThat(creds.numberOfTokens()).isZero();
  }

  @Test
  public void testObtainDelegationTokensWithNonRequiredCatalogs() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.sql.catalog." + CATALOG1, SparkCatalog.class.getName());
    sparkConf.set("spark.sql.catalog." + CATALOG1 + ".type", "hive");
    sparkConf.set("spark.sql.catalog." + CATALOG1 + ".uri", METASTORE_URI_1);

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("hadoop.security.authentication", "simple");

    Credentials creds = new Credentials();
    provider.obtainDelegationTokens(hadoopConf, sparkConf, creds);
    assertThat(creds.numberOfTokens()).isZero();
  }

  @Test
  public void testObtainDelegationTokensHandlesExceptions() {
    // Verify failure isolation: one catalog failure should not block other catalogs
    IcebergHiveDelegationTokenProvider testProvider =
        new IcebergHiveDelegationTokenProvider() {
          @Override
          Optional<HiveConf> buildHiveConf(
              SparkConf sparkConf, Configuration hadoopConf, String catalogName) {
            if (CATALOG1.equals(catalogName)) {
              return Optional.empty(); // Simulate buildHiveConf failure for CATALOG1
            }

            return super.buildHiveConf(sparkConf, hadoopConf, catalogName);
          }

          @Override
          IMetaStoreClient createHmsClient(HiveConf conf)
              throws org.apache.hadoop.hive.metastore.api.MetaException {
            if (METASTORE_URI_2.equals(conf.get(HiveConf.ConfVars.METASTOREURIS.varname))) {
              throw new org.apache.hadoop.hive.metastore.api.MetaException(
                  "Simulated HMS client failure for CATALOG2");
            }

            return super.createHmsClient(conf);
          }
        };

    SparkConf sparkConf = new SparkConf();
    // CATALOG1: buildHiveConf will fail
    sparkConf.set("spark.sql.catalog." + CATALOG1, SparkCatalog.class.getName());
    sparkConf.set("spark.sql.catalog." + CATALOG1 + ".type", "hive");
    sparkConf.set("spark.sql.catalog." + CATALOG1 + ".uri", METASTORE_URI_1);
    // CATALOG2: HMS client creation will fail
    sparkConf.set("spark.sql.catalog." + CATALOG2, SparkSessionCatalog.class.getName());
    sparkConf.set("spark.sql.catalog." + CATALOG2 + ".type", "hive");
    sparkConf.set("spark.sql.catalog." + CATALOG2 + ".uri", METASTORE_URI_2);

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("hadoop.security.authentication", "kerberos");
    hadoopConf.set("hive.metastore.sasl.enabled", "true");

    Credentials creds = new Credentials();
    // Should not throw exception when failures occur - verifying failure isolation
    testProvider.obtainDelegationTokens(hadoopConf, sparkConf, creds);
    assertThat(creds.numberOfTokens()).as("Both catalogs failed, should have zero tokens").isZero();
  }

  @Test
  public void testObtainTokenSignature() throws Exception {
    var method =
        IcebergHiveDelegationTokenProvider.class.getDeclaredMethod(
            "obtainTokenSignature", HiveConf.class);
    method.setAccessible(true);

    HiveConf conf = new HiveConf();
    conf.set("hive.metastore.uris", METASTORE_URI_1);
    conf.set("hive.metastore.token.signature", "custom_sig");

    // Custom signature takes precedence
    assertThat((String) method.invoke(provider, conf)).isEqualTo("custom_sig");

    // Fallback to metastore URI when signature is not set
    conf.unset("hive.metastore.token.signature");
    assertThat((String) method.invoke(provider, conf)).isEqualTo(METASTORE_URI_1);
  }
}
