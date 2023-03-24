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

import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_OPTIONS;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_OPTIONS_DEFAULT;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_SORT_ORDER;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_SORT_ORDER_DEFAULT;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_SPARK_CONFS;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_SPARK_CONFS_DEFAULT;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_STRATEGY;
import static org.apache.iceberg.TableProperties.AUTO_OPTIMIZE_REWRITE_DATA_FILES_STRATEGY_DEFAULT;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.dynamodb.DynamoDbTableOperations;
import org.apache.iceberg.aws.glue.GlueTableOperations;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.metrics.Rewrite;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to hold utilities to optimize table */
public class OptimizeTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(OptimizeTableUtil.class);

  public static final String COMMAND_RUNNER_JAR = "command-runner.jar";

  public static final String EMR_STEP_CONTINUE = "CONTINUE";

  public static final int EMR_STEP_INDEX = 0;

  public static final int SLEEP_WAIT_DURATION_MS = 2000;

  public static final String BLANK = " ";

  public static final String DOT = ".";

  public static final String OPEN_BRACKET = "(";

  public static final String CLOSE_BRACKET = ")";

  public static final String COMMA = ",";

  public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

  public static final String PERSISTENT_APP_UI_ENABLED = "ENABLED";

  public static final String EMR_CONTAINERS_LOG_GROUP_NAME = "/aws/emr-containers";

  public static final String EMR_CONTAINERS_LOG_PREFIX = "iceberg";

  /**
   * Amazon Athena supports bin packing with optimize rewrite data command.
   *
   * <p>For more details, refer:
   * https://docs.aws.amazon.com/athena/latest/ug/optimize-statement.html
   */
  public static final String ATHENA_QUERY_FORMAT = "OPTIMIZE %s REWRITE DATA USING BIN_PACK;";

  private OptimizeTableUtil() {}

  /**
   * Load a custom {@link Rewrite} implementation.
   *
   * <p>The implementation must have a no-arg constructor.
   *
   * @param impl full class name of a custom {@link Rewrite} implementation
   * @return An initialized {@link Rewrite}.
   * @throws IllegalArgumentException if class path not found or right constructor not found or the
   *     loaded class cannot be cast to the given interface type
   */
  public static Rewrite loadRewrite(String impl) {
    LOG.info("Loading custom Rewrite implementation: {}", impl);
    DynConstructors.Ctor<Rewrite> ctor;
    try {
      ctor =
          DynConstructors.builder(Rewrite.class)
              .loader(OptimizeTableUtil.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize Rewrite, missing no-arg constructor: %s", impl), e);
    }

    Rewrite rewrite;
    try {
      rewrite = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize Rewrite, %s does not implement MetricsReporter.", impl),
          e);
    }

    return rewrite;
  }

  /**
   * Get catalog properties from {@link TableOperations} instance
   *
   * @param tableOperations {@link TableOperations} instance
   * @return Catalog properties
   */
  public static Map<String, String> catalogProperties(TableOperations tableOperations) {
    switch (tableOperations.getClass().getSimpleName()) {
      case "GlueTableOperations":
        return ((GlueTableOperations) tableOperations).tableCatalogProperties();
      case "DynamoDbTableOperations":
        return ((DynamoDbTableOperations) tableOperations).tableCatalogProperties();
      case "HiveTableOperations":
        return ((HiveTableOperations) tableOperations).tableCatalogProperties();
      case "HadoopTableOperations":
        return ((HadoopTableOperations) tableOperations).tableCatalogProperties();
      default:
        return Maps.newHashMap();
    }
  }

  /**
   * Get property as boolean from table properties or catalog properties. Table property is given
   * more preference in case both table property and catalog property is provided by the user.
   *
   * @param tableOperations {@link TableOperations} instance
   * @param property property to be fetched
   * @param defaultValue default value of the property
   * @return Property as boolean
   */
  public static boolean propertyAsBoolean(
      TableOperations tableOperations, String property, boolean defaultValue) {
    boolean tableProperty = tableOperations.current().propertyAsBoolean(property, defaultValue);
    if (tableProperty == defaultValue) {
      return PropertyUtil.propertyAsBoolean(
          catalogProperties(tableOperations), property, defaultValue);
    } else {
      return tableProperty;
    }
  }

  /**
   * Get property as string from table properties or catalog properties. Table property is given
   * more preference in case both table property and catalog property is provided by the user.
   *
   * @param tableOperations {@link TableOperations} instance
   * @param property property to be fetched
   * @param defaultValue default value of the property
   * @return Property as string
   */
  public static String propertyAsString(
      TableOperations tableOperations, String property, String defaultValue) {
    String tableProperty = tableOperations.current().property(property, defaultValue);
    if (Objects.equals(tableProperty, defaultValue)) {
      return PropertyUtil.propertyAsString(
          catalogProperties(tableOperations), property, defaultValue);
    } else {
      return tableProperty;
    }
  }

  /**
   * Get property as int from table properties or catalog properties. Table property is given more
   * preference in case both table property and catalog property is provided by the user.
   *
   * @param tableOperations {@link TableOperations} instance
   * @param property property to be fetched
   * @param defaultValue default value of the property
   * @return Property as int
   */
  public static int propertyAsInt(
      TableOperations tableOperations, String property, int defaultValue) {
    int tableProperty = tableOperations.current().propertyAsInt(property, defaultValue);
    if (Objects.equals(tableProperty, defaultValue)) {
      return PropertyUtil.propertyAsInt(catalogProperties(tableOperations), property, defaultValue);
    } else {
      return tableProperty;
    }
  }

  /**
   * Build Spark SQL launch command
   *
   * @return Spark SQL launch command
   */
  public static List<String> buildSparkSqlCommand() {
    return ImmutableList.of("spark-sql");
  }

  /**
   * Build Spark SQL extensions configuration
   *
   * @return Spark SQL extensions configuration
   */
  public static List<String> buildSparkSqlExtensions() {
    return ImmutableList.of(
        "--conf",
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
  }

  /**
   * Build Spark SQL catalog configuration
   *
   * @param tableOperations {@link TableOperations} instance
   * @return Spark SQL Catalog configuration
   */
  public static List<String> buildSparkSqlCatalog(TableOperations tableOperations) {
    return ImmutableList.of(
        "--conf",
        "spark.sql.catalog.catalog_name=org.apache.iceberg.spark.SparkCatalog",
        "--conf",
        "spark.sql.catalog.catalog_name.io-impl=" + tableOperations.io().getClass().getName(),
        "--conf",
        "spark.sql.catalog.catalog_name.warehouse="
            + propertyAsString(
                tableOperations,
                CatalogProperties.WAREHOUSE_LOCATION,
                tableOperations.current().location()));
  }

  /**
   * Build Spark SQL catalog implementation configuration
   *
   * @param tableOperations {@link TableOperations} instance
   * @return Spark SQL Catalog implementation configuration
   */
  public static List<String> buildSparkSqlCatalogImplementation(TableOperations tableOperations) {
    switch (tableOperations.getClass().getSimpleName()) {
      case "GlueTableOperations":
        return ImmutableList.of(
            "--conf",
            "spark.sql.catalog.catalog_name.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog");
      case "DynamoDbTableOperations":
        return ImmutableList.of(
            "--conf",
            "spark.sql.catalog.catalog_name.catalog-impl=org.apache.iceberg.aws.dynamodb.DynamoDbCatalog");
      case "HiveTableOperations":
        return ImmutableList.of(
            "--conf",
            "spark.sql.catalog.catalog_name.catalog-impl=org.apache.iceberg.hive.HiveCatalog",
            "--conf",
            "spark.sql.catalog.catalog_name.uri="
                + ((HiveTableOperations) tableOperations).configuration().get(HIVE_METASTORE_URIS));
      case "HadoopTableOperations":
        return ImmutableList.of(
            "--conf",
            "spark.sql.catalog.catalog_name.catalog-impl=org.apache.iceberg.hadoop.HadoopCatalog");
      default:
        return ImmutableList.of();
    }
  }

  /**
   * Build Default Spark SQL job configurations. Users can override the Spark SQL job configurations
   * using {@link org.apache.iceberg.TableProperties#AUTO_OPTIMIZE_REWRITE_DATA_FILES_SPARK_CONFS}
   * property.
   *
   * @param tableOperations {@link TableOperations} instance
   * @return Default Spark SQL job configurations
   */
  public static List<String> buildDefaultSparkConfigurations(TableOperations tableOperations) {
    return Arrays.asList(
        OptimizeTableUtil.propertyAsString(
                tableOperations,
                AUTO_OPTIMIZE_REWRITE_DATA_FILES_SPARK_CONFS,
                AUTO_OPTIMIZE_REWRITE_DATA_FILES_SPARK_CONFS_DEFAULT)
            .split(OptimizeTableUtil.BLANK));
  }

  /**
   * Removes the catalog name, i.e. my_catalog.db.table gets converted to db.table
   *
   * @param tableNameWithCatalog Table with catalog name
   * @return Table with database name
   */
  public static String tableName(String tableNameWithCatalog) {
    return tableNameWithCatalog.substring(tableNameWithCatalog.indexOf(DOT) + 1);
  }

  /**
   * Build Spark SQL Execute flag
   *
   * @return Spark SQL Execute flag
   */
  public static List<String> buildSparkSqlExecuteFlag() {
    return ImmutableList.of("-e");
  }

  /**
   * Build Spark SQL rewrite data files command. Users can configure the command using {@link
   * org.apache.iceberg.TableProperties#AUTO_OPTIMIZE_REWRITE_DATA_FILES_OPTIONS}, {@link
   * org.apache.iceberg.TableProperties#AUTO_OPTIMIZE_REWRITE_DATA_FILES_STRATEGY}, {@link
   * org.apache.iceberg.TableProperties#AUTO_OPTIMIZE_REWRITE_DATA_FILES_SORT_ORDER} properties. For
   * example, the resulting Spark SQL rewrite data files command will be like CALL
   * catalog_name.system.rewrite_data_files(table => 'db_name.table_name', options =>
   * map('partial-progress.enabled','true'), strategy => 'binpack')
   *
   * @param tableNameWithCatalog Fully qualified name table with catalog. For example:
   *     catalog_name.db_name.table_name
   * @param tableOperations {@link TableOperations} instance
   * @return Spark SQL rewrite data files command
   */
  public static List<String> buildSparkSqlRewriteDataFilesCommand(
      String tableNameWithCatalog, TableOperations tableOperations) {
    String tableNameWithDatabase = tableName(tableNameWithCatalog);
    String optionsMap =
        propertyAsString(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_OPTIONS,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_OPTIONS_DEFAULT);
    String strategy =
        propertyAsString(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_STRATEGY,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_STRATEGY_DEFAULT);
    String sortOrder =
        propertyAsString(
            tableOperations,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_SORT_ORDER,
            AUTO_OPTIMIZE_REWRITE_DATA_FILES_SORT_ORDER_DEFAULT);

    StringBuilder rewriteDataFilesCommand = new StringBuilder();
    rewriteDataFilesCommand.append("CALL catalog_name.system.rewrite_data_files");
    rewriteDataFilesCommand.append(OPEN_BRACKET);
    rewriteDataFilesCommand.append(String.format("table => '%s'", tableNameWithDatabase));
    if (optionsMap != null && !optionsMap.isEmpty()) {
      rewriteDataFilesCommand.append(COMMA);
      rewriteDataFilesCommand.append(BLANK);
      rewriteDataFilesCommand.append(String.format("options => %s", optionsMap));
    }
    if (strategy != null && !strategy.isEmpty()) {
      rewriteDataFilesCommand.append(COMMA);
      rewriteDataFilesCommand.append(BLANK);
      rewriteDataFilesCommand.append(String.format("strategy => '%s'", strategy));
    }
    if (sortOrder != null && !sortOrder.isEmpty()) {
      rewriteDataFilesCommand.append(COMMA);
      rewriteDataFilesCommand.append(BLANK);
      rewriteDataFilesCommand.append(String.format("sort_order => '%s'", sortOrder));
    }
    rewriteDataFilesCommand.append(CLOSE_BRACKET);

    return ImmutableList.of(rewriteDataFilesCommand.toString());
  }

  /**
   * Build Spark Kubernetes file upload path configuration
   *
   * @param path Path where the files are to be uploaded
   * @return Spark Kubernetes file upload path configuration
   */
  public static List<String> buildSparkKubernetesFileUploadPath(String path) {
    return ImmutableList.of("--conf", String.format("spark.kubernetes.file.upload.path=%s", path));
  }

  /**
   * Build Iceberg jar path
   *
   * @return Iceberg jar path
   */
  public static List<String> buildIcebergJarPath() {
    return ImmutableList.of("--jars", "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar");
  }
}
