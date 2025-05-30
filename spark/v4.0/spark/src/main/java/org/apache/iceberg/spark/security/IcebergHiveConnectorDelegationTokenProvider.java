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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.security.HadoopDelegationTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

public class IcebergHiveConnectorDelegationTokenProvider implements HadoopDelegationTokenProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergHiveConnectorDelegationTokenProvider.class);

  private static final String SERVICE_NAME = "iceberg_hive";
  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";
  private static final String CATALOG_TYPE = "hive";
  private static final String URI_KEY = ".uri";
  private static final String PRINCIPAL_KEY = ".hive.metastore.kerberos.principal";
  private static final String TYPE_KEY = ".type";

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
    try {
      HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);
      Arrays.stream(sparkConf.getAllWithPrefix(SPARK_SQL_CATALOG_PREFIX + catalogName))
          .forEach(x -> hiveConf.set(x._1, x._2));
      return Optional.of(hiveConf);
    } catch (Exception e) {
      LOG.warn("Fail to create Hive Configuration for catalog {}: {}", catalogName, e.getMessage());
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
              String val = entry._2();
              return val.contains(SparkSessionCatalog.class.getName())
                  || val.contains(SparkCatalog.class.getName());
            })
        .map(Tuple2::_1)
        .collect(Collectors.toSet());
  }

  @Override
  public boolean delegationTokensRequired(SparkConf sparkConf, Configuration hadoopConf) {
    return !getRequireTokenCatalogs(sparkConf).isEmpty();
  }

  private Set<String> getRequireTokenCatalogs(SparkConf sparkConf) {
    return extractIcebergCatalogNames(sparkConf).stream()
        .filter(catalog -> checkDelegationTokensRequired(sparkConf, catalog))
        .collect(Collectors.toSet());
  }

  private boolean checkDelegationTokensRequired(SparkConf sparkConf, String catalogName) {
    String metastoreUri = sparkConf.get(SPARK_SQL_CATALOG_PREFIX + catalogName + URI_KEY, "");
    String principal = sparkConf.get(SPARK_SQL_CATALOG_PREFIX + catalogName + PRINCIPAL_KEY, "");
    boolean isHiveType =
        CATALOG_TYPE.equalsIgnoreCase(
            sparkConf.get(SPARK_SQL_CATALOG_PREFIX + catalogName + TYPE_KEY, ""));
    if (metastoreUri.isEmpty()
        || principal.isEmpty()
        || !isHiveType
        || !UserGroupInformation.isSecurityEnabled()) {
      return false;
    }

    try {
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      Token<?> currentToken = credentials.getToken(new Text(metastoreUri));
      return currentToken == null;
    } catch (IOException ex) {
      LOG.error(
          "Failed to get current user credentials for catalog {}: {}",
          catalogName,
          ex.getMessage(),
          ex);
      throw new RuntimeException(ex);
    }
  }

  IMetaStoreClient createHmsClient(HiveConf conf) throws MetaException {
    return new HiveMetaStoreClient(conf, null, false);
  }

  @Override
  public Option<Object> obtainDelegationTokens(
      Configuration hadoopConf, SparkConf sparkConf, Credentials creds) {
    Set<String> requireTokenCatalogs = getRequireTokenCatalogs(sparkConf);

    for (String catalogName : requireTokenCatalogs) {
      Optional<HiveConf> hiveConfOpt = buildHiveConf(sparkConf, hadoopConf, catalogName);
      if (!hiveConfOpt.isPresent()) {
        throw new RuntimeException("Failed to create HiveConf for listed catalog: " + catalogName);
      }

      LOG.debug("Require token Hive catalog: {}", catalogName);
      HiveConf remoteHmsConf = hiveConfOpt.get();
      String metastoreUri = sparkConf.get(SPARK_SQL_CATALOG_PREFIX + catalogName + URI_KEY);
      String principal = sparkConf.get(SPARK_SQL_CATALOG_PREFIX + catalogName + PRINCIPAL_KEY);

      doAsRealUser(
          () -> {
            IMetaStoreClient hmsClient = null;
            try {
              UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
              LOG.debug(
                  "Getting Hive delegation token for {} against {} at {}",
                  currentUser.getUserName(),
                  principal,
                  metastoreUri);
              hmsClient = createHmsClient(remoteHmsConf);
              String tokenStr = hmsClient.getDelegationToken(currentUser.getUserName(), principal);
              Token<DelegationTokenIdentifier> hive2Token = new Token<>();
              hive2Token.decodeFromUrlString(tokenStr);
              hive2Token.setService(new Text(metastoreUri));
              LOG.debug("Get Token from hive metastore: {}", hive2Token);
              creds.addToken(new Text(metastoreUri), hive2Token);
              return null;
            } finally {
              if (hmsClient != null) {
                try {
                  hmsClient.close();
                } catch (Exception e) {
                  LOG.warn("Failed to close HiveMetaStoreClient: {}", e.getMessage(), e);
                }
              }
            }
          });
    }

    return Option.empty();
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
