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

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.security.HadoopDelegationTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * Delegation token provider for Iceberg Hive catalogs in Kerberos environments.
 *
 * <p>This provider automatically obtains and renews Hive Metastore delegation tokens for Iceberg
 * catalogs configured in Spark. It supports:
 *
 * <ul>
 *   <li>Multiple Hive Metastore instances
 *   <li>Custom token signatures
 *   <li>Failure isolation (one HMS failure doesn't block others)
 *   <li>Proxy user support
 * </ul>
 *
 * <p><b>Important Limitations:</b>
 *
 * <ul>
 *   <li>Catalogs must be configured before Spark application starts (via spark-defaults.conf or
 *       --conf)
 *   <li>Dynamic catalog registration (via SQL SET) is NOT supported
 *   <li>spark-sql command may not work properly due to Spark internal implementation
 * </ul>
 *
 * <p><b>Configuration Example:</b>
 *
 * <pre>{@code
 * spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkSessionCatalog
 * spark.sql.catalog.hive_prod.type=hive
 * spark.sql.catalog.hive_prod.uri=thrift://metastore-host:port
 * spark.sql.catalog.hive_prod.hive.metastore.token.signature=hive_prod
 * spark.sql.catalog.hive_prod.delegation.token.renewal.enabled=true
 * }</pre>
 *
 * @see <a href="https://github.com/apache/kyuubi/pull/4560">Kyuubi PR #4560</a> for reference
 *     implementation
 */
public class IcebergHiveDelegationTokenProvider implements HadoopDelegationTokenProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergHiveDelegationTokenProvider.class);

  private static final String SERVICE_NAME = "iceberg_hive";
  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";
  private static final String CATALOG_TYPE = "hive";
  private static final String URI_KEY = "uri";
  private static final String TYPE_KEY = "type";
  private static final String DOT = ".";
  private static final String DELEGATION_TOKEN_RENEWAL_ENABLE = "delegation.token.renewal.enabled";
  private static final String SPARK_KERBEROS_KEYTAB = "spark.kerberos.keytab";
  private static final String DEPLOY_MODE_CLIENT = "client";

  @Override
  public String serviceName() {
    return SERVICE_NAME;
  }

  /**
   * Builds a HiveConf object for the specified catalog by merging the provided Hadoop configuration
   * with catalog-specific configurations from the Spark configuration.
   *
   * @param sparkConf The SparkConf object containing Spark configurations.
   * @param hadoopConf The Hadoop Configuration object to be used as the base for the HiveConf.
   * @param catalogName The name of the catalog.
   * @return An Optional containing the constructed HiveConf if successful, or an empty Optional if
   *     an error occurs.
   */
  Optional<HiveConf> buildHiveConf(
      SparkConf sparkConf, Configuration hadoopConf, String catalogName) {
    String targetPrefix = SPARK_SQL_CATALOG_PREFIX + catalogName + DOT;
    try {
      HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);
      Arrays.stream(sparkConf.getAllWithPrefix(targetPrefix))
          .forEach(
              entry -> {
                if (entry._1.equals(URI_KEY)) {
                  hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, entry._2);
                } else {
                  hiveConf.set(entry._1, entry._2);
                }
              });
      // The `RetryingHiveMetaStoreClient` may block the subsequent token obtaining,
      // and `obtainDelegationTokens` is scheduled frequently, it's fine to disable
      // the Hive retry mechanism.
      hiveConf.set(HiveConf.ConfVars.METASTORE_FASTPATH.varname, "false");

      return Optional.of(hiveConf);
    } catch (Exception e) {
      LOG.warn("Failed to create Hive Configuration for catalog: {}", catalogName, e);
      return Optional.empty();
    }
  }

  /**
   * Extracts the names of Iceberg catalogs from the provided Spark configuration. This method
   * filters the Spark configuration entries to identify those that correspond to Iceberg catalogs.
   * It checks if the catalog type matches either `SparkSessionCatalog` or `SparkCatalog` and
   * collects their names.
   *
   * @param sparkConf The SparkConf object containing Spark configurations.
   * @return A Set of Strings representing the names of Iceberg catalogs.
   */
  Set<String> extractIcebergCatalogNames(SparkConf sparkConf) {
    return Arrays.stream(sparkConf.getAllWithPrefix(SPARK_SQL_CATALOG_PREFIX))
        .filter(
            entry -> {
              return entry._2.equals(SparkSessionCatalog.class.getName())
                  || entry._2.equals(SparkCatalog.class.getName());
            })
        .map(entry -> entry._1)
        // Filter out illegal catalog names (e.g. containing dots)
        .filter(catalogName -> isValidCatalogName(catalogName))
        .collect(Collectors.toSet());
  }

  /**
   * Checks if a catalog name is valid according to Spark's naming rules.
   *
   * @param catalogName The catalog name to validate
   * @return true if the catalog name is valid, false otherwise
   */
  private boolean isValidCatalogName(String catalogName) {
    // A valid catalog name should not contain dots (to avoid nested catalogs)
    // and should not be empty
    return !catalogName.contains(DOT) && !catalogName.isEmpty();
  }

  /**
   * Determines if delegation tokens are required for any configured Iceberg Hive catalogs.
   *
   * <p>This method is called by Spark to check if the delegation token provider should be invoked.
   * It returns {@code true} if at least one catalog requires delegation token renewal.
   *
   * @param sparkConf Spark configuration
   * @param hadoopConf Hadoop configuration from SparkContext
   * @return true if delegation tokens are required for any catalog
   */
  @Override
  public boolean delegationTokensRequired(SparkConf sparkConf, Configuration hadoopConf) {
    return !obtainRequireTokenCatalogs(sparkConf, hadoopConf).isEmpty();
  }

  private Set<String> obtainRequireTokenCatalogs(SparkConf sparkConf, Configuration hadoopConf) {
    return extractIcebergCatalogNames(sparkConf).stream()
        .filter(
            catalogName ->
                buildHiveConf(sparkConf, hadoopConf, catalogName)
                    .map(
                        hiveConf -> checkDelegationTokensRequired(sparkConf, hiveConf, catalogName))
                    .orElse(false))
        .collect(Collectors.toSet());
  }

  private boolean checkDelegationTokensRequired(
      SparkConf sparkConf, HiveConf hiveConf, String catalogName) {
    String targetPrefix = SPARK_SQL_CATALOG_PREFIX + catalogName + DOT;

    boolean isNeedRenewal =
        sparkConf.getBoolean(targetPrefix + DELEGATION_TOKEN_RENEWAL_ENABLE, true);
    boolean isHiveType = CATALOG_TYPE.equalsIgnoreCase(sparkConf.get(targetPrefix + TYPE_KEY, ""));
    String metastoreUri = hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
    if (!isNeedRenewal || !isHiveType || StringUtils.isBlank(metastoreUri)) {
      LOG.debug(
          "Delegation token not required for catalog: {}, isNeedRenewal: {}, isHiveType: {}, metastoreUri: {}",
          catalogName,
          isNeedRenewal,
          isHiveType,
          metastoreUri);

      return false;
    }

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
      if (currentUser == null) {
        throw new RuntimeException("Failed to get current user for delegation token check");
      }
    } catch (IOException e) {
      throw new RuntimeException("Error retrieving current user", e);
    }

    boolean authenticated =
        SecurityUtil.getAuthenticationMethod(hiveConf)
            != UserGroupInformation.AuthenticationMethod.SIMPLE;
    boolean saslEnabled =
        hiveConf.getBoolean(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, false);

    // Use token signature instead of metastore URI as service
    String tokenSignature = hiveConf.get(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE.varname, null);
    if (StringUtils.isBlank(tokenSignature)) {
      tokenSignature = metastoreUri;
    }
    Token<?> currentToken = currentUser.getCredentials().getToken(new Text(tokenSignature));
    // Check if current user is a proxy user (has a real user behind it)
    boolean isProxyUser = currentUser.getRealUser() != null;
    boolean keytabExists = sparkConf.contains(SPARK_KERBEROS_KEYTAB);

    boolean isClientMode =
        DEPLOY_MODE_CLIENT.equals(sparkConf.get(SparkLauncher.DEPLOY_MODE, DEPLOY_MODE_CLIENT));

    LOG.debug(
        "Delegation token check for catalog: {}, authenticated: {}, saslEnabled: {}, currentTokenExists: {}, isProxyUser: {}, isClientMode: {}, keytabExists: {}",
        catalogName,
        authenticated,
        saslEnabled,
        currentToken != null,
        isProxyUser,
        isClientMode,
        keytabExists);

    return authenticated
        && saslEnabled
        && currentToken == null
        && (isProxyUser || (!isClientMode && !keytabExists));
  }

  IMetaStoreClient createHmsClient(HiveConf conf) throws MetaException {
    return new HiveMetaStoreClient(conf, null, false);
  }

  /**
   * Obtains delegation tokens for all configured Iceberg Hive catalogs.
   *
   * <p>This method is called periodically by Spark to renew delegation tokens. It iterates through
   * all catalogs that require tokens and attempts to obtain them. Failures for individual catalogs
   * are isolated and logged, but do not prevent token acquisition for other catalogs.
   *
   * @param hadoopConf Hadoop configuration from SparkContext
   * @param sparkConf Spark configuration
   * @param creds Credentials object to store obtained tokens
   * @return Empty Option (required by Spark interface)
   */
  @Override
  public Option<Object> obtainDelegationTokens(
      Configuration hadoopConf, SparkConf sparkConf, Credentials creds) {
    Set<String> requireTokenCatalogs = obtainRequireTokenCatalogs(sparkConf, hadoopConf);

    for (String catalogName : requireTokenCatalogs) {
      LOG.debug("Processing delegation token for catalog: {}", catalogName);

      Optional<HiveConf> hiveConfOpt = buildHiveConf(sparkConf, hadoopConf, catalogName);
      if (!hiveConfOpt.isPresent()) {
        LOG.warn("Skipping catalog {}: failed to build HiveConf", catalogName);
        continue;
      }

      try {
        HiveConf remoteHmsConf = hiveConfOpt.get();
        obtainTokenForCatalog(remoteHmsConf, creds, catalogName);
      } catch (Exception e) {
        // Failure isolation: one HMS failure should not block token acquisition for other HMS
        LOG.warn("Failed to obtain delegation token for catalog: {}", catalogName, e);
      }
    }

    return Option.empty();
  }

  private void obtainTokenForCatalog(
      HiveConf remoteHmsConf, Credentials creds, String catalogName) {
    String metastoreUri = remoteHmsConf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
    String principal =
        remoteHmsConf.get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, "");
    if (StringUtils.isBlank(metastoreUri)) {
      throw new RuntimeException("Metastore URI is empty for catalog: " + catalogName);
    }

    if (StringUtils.isBlank(principal)) {
      throw new RuntimeException("Hive principal is empty for catalog: " + catalogName);
    }

    doAsRealUser(
        () -> {
          IMetaStoreClient hmsClient = null;
          try {
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            LOG.info(
                "Obtaining delegation token for catalog {}: user={}, principal={}, metastoreUri={}",
                catalogName,
                currentUser.getUserName(),
                principal,
                metastoreUri);

            hmsClient = createHmsClient(remoteHmsConf);
            String tokenStr = hmsClient.getDelegationToken(currentUser.getUserName(), principal);
            Token<DelegationTokenIdentifier> hive2Token = new Token<>();
            hive2Token.decodeFromUrlString(tokenStr);

            // Use token signature instead of metastore URI as service
            String tokenSignature = obtainTokenSignature(remoteHmsConf);
            hive2Token.setService(new Text(tokenSignature));
            LOG.info("Successfully obtained delegation token for catalog: {}", catalogName);
            creds.addToken(new Text(tokenSignature), hive2Token);

            return null;
          } finally {
            if (hmsClient != null) {
              try {
                hmsClient.close();
              } catch (Exception e) {
                LOG.warn("Failed to close HiveMetaStoreClient for catalog: {}", catalogName, e);
              }
            }
          }
        });
  }

  /**
   * Get token signature for the given catalog. Fallback to hive.metastore.uris if
   * hive.metastore.token.signature is absent
   *
   * @param conf Hive configuration
   * @return token signature
   */
  private String obtainTokenSignature(HiveConf conf) {
    String tokenSignature = conf.get(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE.varname, null);
    if (StringUtils.isNotBlank(tokenSignature)) {
      return tokenSignature;
    }

    // Fallback to metastore URI
    return conf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
  }

  private <T> void doAsRealUser(PrivilegedExceptionAction<T> action) {
    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      UserGroupInformation realUser =
          Optional.ofNullable(currentUser.getRealUser()).orElse(currentUser);
      realUser.doAs(action);
    } catch (IOException | InterruptedException | UndeclaredThrowableException e) {
      throw new RuntimeException(e);
    }
  }
}
