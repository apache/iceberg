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
package org.apache.iceberg.connect;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkConfig extends AbstractConfig {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkConfig.class.getName());

  public static final String INTERNAL_TRANSACTIONAL_SUFFIX_PROP =
      "iceberg.coordinator.transactional.suffix";
  private static final String ROUTE_REGEX = "route-regex";
  private static final String ID_COLUMNS = "id-columns";
  private static final String PARTITION_BY = "partition-by";
  private static final String COMMIT_BRANCH = "commit-branch";

  private static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";
  private static final String HADOOP_PROP_PREFIX = "iceberg.hadoop.";
  private static final String KAFKA_PROP_PREFIX = "iceberg.kafka.";
  private static final String TABLE_PROP_PREFIX = "iceberg.table.";
  private static final String AUTO_CREATE_PROP_PREFIX = "iceberg.tables.auto-create-props.";
  private static final String WRITE_PROP_PREFIX = "iceberg.tables.write-props.";

  private static final String CATALOG_NAME_PROP = "iceberg.catalog";
  private static final String TABLES_PROP = "iceberg.tables";
  private static final String TABLES_DYNAMIC_PROP = "iceberg.tables.dynamic-enabled";
  private static final String TABLES_ROUTE_FIELD_PROP = "iceberg.tables.route-field";
  private static final String TABLES_DEFAULT_COMMIT_BRANCH = "iceberg.tables.default-commit-branch";
  private static final String TABLES_DEFAULT_ID_COLUMNS = "iceberg.tables.default-id-columns";
  private static final String TABLES_DEFAULT_PARTITION_BY = "iceberg.tables.default-partition-by";
  private static final String TABLES_AUTO_CREATE_ENABLED_PROP =
      "iceberg.tables.auto-create-enabled";
  private static final String TABLES_EVOLVE_SCHEMA_ENABLED_PROP =
      "iceberg.tables.evolve-schema-enabled";
  private static final String TABLES_SCHEMA_FORCE_OPTIONAL_PROP =
      "iceberg.tables.schema-force-optional";
  private static final String TABLES_SCHEMA_CASE_INSENSITIVE_PROP =
      "iceberg.tables.schema-case-insensitive";
  private static final String CONTROL_TOPIC_PROP = "iceberg.control.topic";
  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.control.commit.interval-ms";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 300_000;
  private static final String COMMIT_TIMEOUT_MS_PROP = "iceberg.control.commit.timeout-ms";
  private static final int COMMIT_TIMEOUT_MS_DEFAULT = 30_000;
  private static final String COMMIT_THREADS_PROP = "iceberg.control.commit.threads";
  private static final String CONNECT_GROUP_ID_PROP = "iceberg.connect.group-id";
  private static final String HADOOP_CONF_DIR_PROP = "iceberg.hadoop-conf-dir";

  private static final String NAME_PROP = "name";
  private static final String BOOTSTRAP_SERVERS_PROP = "bootstrap.servers";

  private static final String DEFAULT_CATALOG_NAME = "iceberg";
  private static final String DEFAULT_CONTROL_TOPIC = "control-iceberg";
  public static final String DEFAULT_CONTROL_GROUP_PREFIX = "cg-control-";

  public static final int SCHEMA_UPDATE_RETRIES = 2; // 3 total attempts
  public static final int CREATE_TABLE_RETRIES = 2; // 3 total attempts

  @VisibleForTesting static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";

  public static final ConfigDef CONFIG_DEF = newConfigDef();

  public static String version() {
    return IcebergBuild.version();
  }

  private static ConfigDef newConfigDef() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(
        TABLES_PROP,
        ConfigDef.Type.LIST,
        null,
        Importance.HIGH,
        "Comma-delimited list of destination tables");
    configDef.define(
        TABLES_DYNAMIC_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Enable dynamic routing to tables based on a record value");
    configDef.define(
        TABLES_ROUTE_FIELD_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Source record field for routing records to tables");
    configDef.define(
        TABLES_DEFAULT_COMMIT_BRANCH,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Default branch for commits");
    configDef.define(
        TABLES_DEFAULT_ID_COLUMNS,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Default ID columns for tables, comma-separated");
    configDef.define(
        TABLES_DEFAULT_PARTITION_BY,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "Default partition spec to use when creating tables, comma-separated");
    configDef.define(
        TABLES_AUTO_CREATE_ENABLED_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to automatically create destination tables, false otherwise");
    configDef.define(
        TABLES_SCHEMA_FORCE_OPTIONAL_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to set columns as optional during table create and evolution, false to respect schema");
    configDef.define(
        TABLES_SCHEMA_CASE_INSENSITIVE_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to look up table columns by case-insensitive name, false for case-sensitive");
    configDef.define(
        TABLES_EVOLVE_SCHEMA_ENABLED_PROP,
        ConfigDef.Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to add any missing record fields to the table schema, false otherwise");
    configDef.define(
        CATALOG_NAME_PROP,
        ConfigDef.Type.STRING,
        DEFAULT_CATALOG_NAME,
        Importance.MEDIUM,
        "Iceberg catalog name");
    configDef.define(
        CONTROL_TOPIC_PROP,
        ConfigDef.Type.STRING,
        DEFAULT_CONTROL_TOPIC,
        Importance.MEDIUM,
        "Name of the control topic");
    configDef.define(
        CONNECT_GROUP_ID_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.LOW,
        "Name of the Connect consumer group, should not be set under normal conditions");
    configDef.define(
        COMMIT_INTERVAL_MS_PROP,
        ConfigDef.Type.INT,
        COMMIT_INTERVAL_MS_DEFAULT,
        Importance.MEDIUM,
        "Coordinator interval for performing Iceberg table commits, in millis");
    configDef.define(
        COMMIT_TIMEOUT_MS_PROP,
        ConfigDef.Type.INT,
        COMMIT_TIMEOUT_MS_DEFAULT,
        Importance.MEDIUM,
        "Coordinator time to wait for worker responses before committing, in millis");
    configDef.define(
        COMMIT_THREADS_PROP,
        ConfigDef.Type.INT,
        Runtime.getRuntime().availableProcessors() * 2,
        Importance.MEDIUM,
        "Coordinator threads to use for table commits, default is (cores * 2)");
    configDef.define(
        HADOOP_CONF_DIR_PROP,
        ConfigDef.Type.STRING,
        null,
        Importance.MEDIUM,
        "If specified, Hadoop config files in this directory will be loaded");
    return configDef;
  }

  private final Map<String, String> originalProps;
  private final Map<String, String> catalogProps;
  private final Map<String, String> hadoopProps;
  private final Map<String, String> kafkaProps;
  private final Map<String, String> autoCreateProps;
  private final Map<String, String> writeProps;
  private final Map<String, TableSinkConfig> tableConfigMap = Maps.newHashMap();
  private final JsonConverter jsonConverter;

  public IcebergSinkConfig(Map<String, String> originalProps) {
    super(CONFIG_DEF, originalProps);
    this.originalProps = originalProps;

    this.catalogProps = PropertyUtil.propertiesWithPrefix(originalProps, CATALOG_PROP_PREFIX);
    this.hadoopProps = PropertyUtil.propertiesWithPrefix(originalProps, HADOOP_PROP_PREFIX);

    this.kafkaProps = Maps.newHashMap(loadWorkerProps());
    kafkaProps.putAll(PropertyUtil.propertiesWithPrefix(originalProps, KAFKA_PROP_PREFIX));

    this.autoCreateProps =
        PropertyUtil.propertiesWithPrefix(originalProps, AUTO_CREATE_PROP_PREFIX);
    this.writeProps = PropertyUtil.propertiesWithPrefix(originalProps, WRITE_PROP_PREFIX);

    this.jsonConverter = new JsonConverter();
    jsonConverter.configure(
        ImmutableMap.of(
            JsonConverterConfig.SCHEMAS_ENABLE_CONFIG,
            false,
            ConverterConfig.TYPE_CONFIG,
            ConverterType.VALUE.getName()));

    validate();
  }

  private void validate() {
    checkState(!catalogProps().isEmpty(), "Must specify Iceberg catalog properties");
    if (tables() != null) {
      checkState(!dynamicTablesEnabled(), "Cannot specify both static and dynamic table names");
    } else if (dynamicTablesEnabled()) {
      checkState(
          tablesRouteField() != null, "Must specify a route field if using dynamic table names");
    } else {
      throw new ConfigException("Must specify table name(s)");
    }
  }

  private void checkState(boolean condition, String msg) {
    if (!condition) {
      throw new ConfigException(msg);
    }
  }

  public String connectorName() {
    return originalProps.get(NAME_PROP);
  }

  public String transactionalSuffix() {
    // this is for internal use and is not part of the config definition...
    return originalProps.get(INTERNAL_TRANSACTIONAL_SUFFIX_PROP);
  }

  public Map<String, String> catalogProps() {
    return catalogProps;
  }

  public Map<String, String> hadoopProps() {
    return hadoopProps;
  }

  public Map<String, String> kafkaProps() {
    return kafkaProps;
  }

  public Map<String, String> autoCreateProps() {
    return autoCreateProps;
  }

  public Map<String, String> writeProps() {
    return writeProps;
  }

  public String catalogName() {
    return getString(CATALOG_NAME_PROP);
  }

  public List<String> tables() {
    return getList(TABLES_PROP);
  }

  public boolean dynamicTablesEnabled() {
    return getBoolean(TABLES_DYNAMIC_PROP);
  }

  public String tablesRouteField() {
    return getString(TABLES_ROUTE_FIELD_PROP);
  }

  public String tablesDefaultCommitBranch() {
    return getString(TABLES_DEFAULT_COMMIT_BRANCH);
  }

  public String tablesDefaultIdColumns() {
    return getString(TABLES_DEFAULT_ID_COLUMNS);
  }

  public String tablesDefaultPartitionBy() {
    return getString(TABLES_DEFAULT_PARTITION_BY);
  }

  public TableSinkConfig tableConfig(String tableName) {
    return tableConfigMap.computeIfAbsent(
        tableName,
        notUsed -> {
          Map<String, String> tableConfig =
              PropertyUtil.propertiesWithPrefix(originalProps, TABLE_PROP_PREFIX + tableName + ".");

          String routeRegexStr = tableConfig.get(ROUTE_REGEX);
          Pattern routeRegex = routeRegexStr == null ? null : Pattern.compile(routeRegexStr);

          String idColumnsStr = tableConfig.getOrDefault(ID_COLUMNS, tablesDefaultIdColumns());
          List<String> idColumns = stringToList(idColumnsStr, ",");

          String partitionByStr =
              tableConfig.getOrDefault(PARTITION_BY, tablesDefaultPartitionBy());
          List<String> partitionBy = stringToList(partitionByStr, COMMA_NO_PARENS_REGEX);

          String commitBranch =
              tableConfig.getOrDefault(COMMIT_BRANCH, tablesDefaultCommitBranch());

          return new TableSinkConfig(routeRegex, idColumns, partitionBy, commitBranch);
        });
  }

  @VisibleForTesting
  static List<String> stringToList(String value, String regex) {
    if (value == null || value.isEmpty()) {
      return ImmutableList.of();
    }

    return Arrays.stream(value.split(regex)).map(String::trim).collect(Collectors.toList());
  }

  public String controlTopic() {
    return getString(CONTROL_TOPIC_PROP);
  }

  public String connectGroupId() {
    String result = getString(CONNECT_GROUP_ID_PROP);
    if (result != null) {
      return result;
    }

    String connectorName = connectorName();
    Preconditions.checkNotNull(connectorName, "Connector name cannot be null");
    return "connect-" + connectorName;
  }

  public int commitIntervalMs() {
    return getInt(COMMIT_INTERVAL_MS_PROP);
  }

  public int commitTimeoutMs() {
    return getInt(COMMIT_TIMEOUT_MS_PROP);
  }

  public int commitThreads() {
    return getInt(COMMIT_THREADS_PROP);
  }

  public String hadoopConfDir() {
    return getString(HADOOP_CONF_DIR_PROP);
  }

  public boolean autoCreateEnabled() {
    return getBoolean(TABLES_AUTO_CREATE_ENABLED_PROP);
  }

  public boolean evolveSchemaEnabled() {
    return getBoolean(TABLES_EVOLVE_SCHEMA_ENABLED_PROP);
  }

  public boolean schemaForceOptional() {
    return getBoolean(TABLES_SCHEMA_FORCE_OPTIONAL_PROP);
  }

  public boolean schemaCaseInsensitive() {
    return getBoolean(TABLES_SCHEMA_CASE_INSENSITIVE_PROP);
  }

  public JsonConverter jsonConverter() {
    return jsonConverter;
  }

  /**
   * This method attempts to load the Kafka Connect worker properties, which are not exposed to
   * connectors. It does this by parsing the Java command used to launch the worker, extracting the
   * name of the properties file, and then loading the file. <br>
   * The sink uses these properties, if available, when initializing its internal Kafka clients. By
   * doing this, Kafka-related properties only need to be set in the worker properties and do not
   * need to be duplicated in the sink config. <br>
   * If the worker properties cannot be loaded, then Kafka-related properties must be set via the
   * `iceberg.kafka.*` sink configs.
   *
   * @return The Kafka Connect worker properties
   */
  private Map<String, String> loadWorkerProps() {
    String javaCmd = System.getProperty("sun.java.command");
    if (javaCmd != null && !javaCmd.isEmpty()) {
      List<String> args = Splitter.on(' ').splitToList(javaCmd);
      if (args.size() > 1
          && (args.get(0).endsWith(".ConnectDistributed")
              || args.get(0).endsWith(".ConnectStandalone"))) {
        Properties result = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get(args.get(1)))) {
          result.load(in);
          // sanity check that this is the config we want
          if (result.containsKey(BOOTSTRAP_SERVERS_PROP)) {
            return Maps.fromProperties(result);
          }
        } catch (Exception e) {
          // NO-OP
        }
      }
    }
    LOG.info(
        "Worker properties not loaded, using only {}* properties for Kafka clients",
        KAFKA_PROP_PREFIX);
    return ImmutableMap.of();
  }
}
