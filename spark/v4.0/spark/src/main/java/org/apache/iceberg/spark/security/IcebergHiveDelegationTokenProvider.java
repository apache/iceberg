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
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.security.HadoopDelegationTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

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
      LOG.warn("Fail to create Hive Configuration for catalog {}", catalogName, e);
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
  private Set<String> extractIcebergCatalogNames(SparkConf sparkConf) {
    return Arrays.stream(sparkConf.getAllWithPrefix(SPARK_SQL_CATALOG_PREFIX))
        .filter(
            entry -> {
              return entry._2.contains(SparkSessionCatalog.class.getName())
                  || entry._2.contains(SparkCatalog.class.getName());
            })
        .map(entry -> entry._1)
        .filter(catalogName -> !catalogName.contains(DOT))
        .collect(Collectors.toSet());
  }

  @Override
  public boolean delegationTokensRequired(SparkConf sparkConf, Configuration hadoopConf) {
    return !getRequireTokenCatalogs(sparkConf, hadoopConf).isEmpty();
  }

  private Set<String> getRequireTokenCatalogs(SparkConf sparkConf, Configuration hadoopConf) {
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

    Token<?> currentToken = currentUser.getCredentials().getToken(new Text(metastoreUri));
    boolean securityEnabled = SparkHadoopUtil.get().isProxyUser(currentUser);
    boolean keytabExists = sparkConf.contains(SPARK_KERBEROS_KEYTAB);

    boolean isClientMode =
        DEPLOY_MODE_CLIENT.equals(sparkConf.get(SparkLauncher.DEPLOY_MODE, DEPLOY_MODE_CLIENT));

    LOG.debug(
        "Delegation token check for catalog: {}, authenticated: {}, saslEnable: {}, currentTokenExists: {}, securityEnabled: {}, isClientMode: {}, keytabExists: {}",
        catalogName,
        authenticated,
        saslEnabled,
        currentToken != null,
        securityEnabled,
        isClientMode,
        keytabExists);

    return authenticated
        && saslEnabled
        && currentToken == null
        && (securityEnabled || (!isClientMode && !keytabExists));
  }

  IMetaStoreClient createHmsClient(HiveConf conf) throws MetaException {
    return new HiveMetaStoreClient(conf, null, false);
  }

  @Override
  public Option<Object> obtainDelegationTokens(
      Configuration hadoopConf, SparkConf sparkConf, Credentials creds) {
    Set<String> requireTokenCatalogs = getRequireTokenCatalogs(sparkConf, hadoopConf);

    for (String catalogName : requireTokenCatalogs) {
      LOG.debug("Require token Hive catalog: {}", catalogName);

      Optional<HiveConf> hiveConfOpt = buildHiveConf(sparkConf, hadoopConf, catalogName);
      // one token request failure should not block the subsequent token obtaining
      if (!hiveConfOpt.isPresent()) {
        LOG.warn("Failed to create HiveConf for listed catalog: {}", catalogName);
        continue;
      }

      try {
        HiveConf remoteHmsConf = hiveConfOpt.get();
        obtainTokenForCatalog(remoteHmsConf, creds);
      } catch (Exception e) {
        // one token request failure should not block the subsequent token obtaining
        LOG.warn("Failed to obtain token for catalog {}", catalogName, e);
      }
    }

    return Option.empty();
  }

  private void obtainTokenForCatalog(HiveConf remoteHmsConf, Credentials creds) throws Exception {
    String metastoreUri = remoteHmsConf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
    String principal =
        remoteHmsConf.get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, "");
    if (StringUtils.isBlank(metastoreUri)) {
      throw new RuntimeException("Metastore URI is empty");
    }

    if (StringUtils.isBlank(principal)) {
      throw new RuntimeException("Hive principal is empty");
    }

    doAsRealUser(
        () -> {
          IMetaStoreClient hmsClient = null;
          try {
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            LOG.debug(
                "Acquiring Hive delegation token - User: {}, Principal: {}, Metastore URI: {}",
                currentUser.getUserName(),
                principal,
                metastoreUri);

            hmsClient = createHmsClient(remoteHmsConf);
            String tokenStr = hmsClient.getDelegationToken(currentUser.getUserName(), principal);
            Token<DelegationTokenIdentifier> hive2Token = new Token<>();
            hive2Token.decodeFromUrlString(tokenStr);
            hive2Token.setService(new Text(metastoreUri));
            LOG.debug("Get Token from hive metastore: {}", metastoreUri);
            creds.addToken(new Text(metastoreUri), hive2Token);

            return null;
          } finally {
            if (hmsClient != null) {
              try {
                hmsClient.close();
              } catch (Exception e) {
                LOG.warn("Failed to close HiveMetaStoreClient", e);
              }
            }
          }
        });
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
