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
package io.tabular.iceberg.connect;

import static java.util.stream.Collectors.toList;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkConfig extends AbstractConfig {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkConfig.class.getName());

  public static final String INTERNAL_TRANSACTIONAL_SUFFIX_PROP =
      "iceberg.coordinator.transactional.suffix";
  private static final String ROUTE_REGEX = "routeRegex";
  private static final String ID_COLUMNS = "idColumns";
  private static final String COMMIT_BRANCH = "commitBranch";

  private static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";
  private static final String HADOOP_PROP_PREFIX = "iceberg.hadoop.";
  private static final String KAFKA_PROP_PREFIX = "iceberg.kafka.";
  private static final String TABLE_PROP_PREFIX = "iceberg.table.";

  private static final String CATALOG_NAME_PROP = "iceberg.catalog";
  private static final String TABLES_PROP = "iceberg.tables";
  private static final String TABLES_DYNAMIC_PROP = "iceberg.tables.dynamic.enabled";
  private static final String TABLES_ROUTE_FIELD_PROP = "iceberg.tables.routeField";
  private static final String TABLES_DEFAULT_COMMIT_BRANCH = "iceberg.tables.defaultCommitBranch";
  private static final String TABLES_CDC_FIELD_PROP = "iceberg.tables.cdcField";
  private static final String TABLES_UPSERT_MODE_ENABLED_PROP = "iceberg.tables.upsertModeEnabled";
  private static final String TABLES_AUTO_CREATE_ENABLED_PROP = "iceberg.tables.autoCreateEnabled";
  private static final String TABLES_EVOLVE_SCHEMA_ENABLED_PROP =
      "iceberg.tables.evolveSchemaEnabled";
  private static final String CONTROL_TOPIC_PROP = "iceberg.control.topic";
  private static final String CONTROL_GROUP_ID_PROP = "iceberg.control.group.id";
  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.control.commitIntervalMs";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 300_000;
  private static final String COMMIT_TIMEOUT_MS_PROP = "iceberg.control.commitTimeoutMs";
  private static final int COMMIT_TIMEOUT_MS_DEFAULT = 30_000;
  private static final String COMMIT_THREADS_PROP = "iceberg.control.commitThreads";
  private static final String HADDOP_CONF_DIR_PROP = "iceberg.hadoop-conf-dir";

  private static final String NAME_PROP = "name";
  private static final String BOOTSTRAP_SERVERS_PROP = "bootstrap.servers";

  private static final String DEFAULT_CATALOG_NAME = "iceberg";
  private static final String DEFAULT_CONTROL_TOPIC = "control-iceberg";
  public static final String DEFAULT_CONTROL_GROUP_PREFIX = "cg-control-";

  public static final ConfigDef CONFIG_DEF = newConfigDef();

  public static String getVersion() {
    String kcVersion = IcebergSinkConfig.class.getPackage().getImplementationVersion();
    if (kcVersion == null) {
      kcVersion = "unknown";
    }
    return IcebergBuild.version() + "-kc-" + kcVersion;
  }

  private static ConfigDef newConfigDef() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(
        SinkConnector.TOPICS_CONFIG,
        Type.LIST,
        Importance.HIGH,
        "Comma-delimited list of source topics");
    configDef.define(
        TABLES_PROP,
        Type.LIST,
        null,
        Importance.HIGH,
        "Comma-delimited list of destination tables");
    configDef.define(
        TABLES_DYNAMIC_PROP,
        Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Enable dynamic routing to tables based on a record value");
    configDef.define(
        TABLES_ROUTE_FIELD_PROP,
        Type.STRING,
        null,
        Importance.MEDIUM,
        "Source record field for routing records to tables");
    configDef.define(
        TABLES_DEFAULT_COMMIT_BRANCH,
        Type.STRING,
        null,
        Importance.MEDIUM,
        "Default branch for commits");
    configDef.define(
        TABLES_CDC_FIELD_PROP,
        Type.STRING,
        null,
        Importance.MEDIUM,
        "Source record field that identifies the type of operation (insert, update, or delete)");
    configDef.define(
        TABLES_UPSERT_MODE_ENABLED_PROP,
        Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to treat all appends as upserts, false otherwise");
    configDef.define(
        TABLES_AUTO_CREATE_ENABLED_PROP,
        Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to automatically create destination tables, false otherwise");
    configDef.define(
        TABLES_EVOLVE_SCHEMA_ENABLED_PROP,
        Type.BOOLEAN,
        false,
        Importance.MEDIUM,
        "Set to true to add any missing record fields to the table schema, false otherwise");
    configDef.define(
        CATALOG_NAME_PROP,
        Type.STRING,
        DEFAULT_CATALOG_NAME,
        Importance.MEDIUM,
        "Iceberg catalog name");
    configDef.define(
        CONTROL_TOPIC_PROP,
        Type.STRING,
        DEFAULT_CONTROL_TOPIC,
        Importance.MEDIUM,
        "Name of the control topic");
    configDef.define(
        CONTROL_GROUP_ID_PROP,
        Type.STRING,
        null,
        Importance.MEDIUM,
        "Name of the consumer group to store offsets");
    configDef.define(
        COMMIT_INTERVAL_MS_PROP,
        Type.INT,
        COMMIT_INTERVAL_MS_DEFAULT,
        Importance.MEDIUM,
        "Coordinator interval for performing Iceberg table commits, in millis");
    configDef.define(
        COMMIT_TIMEOUT_MS_PROP,
        Type.INT,
        COMMIT_TIMEOUT_MS_DEFAULT,
        Importance.MEDIUM,
        "Coordinator time to wait for worker responses before committing, in millis");
    configDef.define(
        COMMIT_THREADS_PROP,
        Type.INT,
        Runtime.getRuntime().availableProcessors() * 2,
        Importance.MEDIUM,
        "Coordinator threads to use for table commits, default is (cores * 2)");
    configDef.define(
        HADDOP_CONF_DIR_PROP,
        Type.STRING,
        null,
        Importance.MEDIUM,
        "Coordinator threads to use for table commits, default is (cores * 2)");
    return configDef;
  }

  private final Map<String, String> originalProps;
  private final Map<String, String> catalogProps;
  private final Map<String, String> hadoopProps;
  private final Map<String, String> kafkaProps;
  private final Map<String, TableSinkConfig> tableConfigMap = Maps.newHashMap();
  private final JsonConverter jsonConverter;

  public IcebergSinkConfig(Map<String, String> originalProps) {
    super(CONFIG_DEF, originalProps);
    this.originalProps = originalProps;

    this.catalogProps = PropertyUtil.propertiesWithPrefix(originalProps, CATALOG_PROP_PREFIX);
    this.hadoopProps = PropertyUtil.propertiesWithPrefix(originalProps, HADOOP_PROP_PREFIX);

    this.kafkaProps = Maps.newHashMap(loadWorkerProps());
    kafkaProps.putAll(PropertyUtil.propertiesWithPrefix(originalProps, KAFKA_PROP_PREFIX));

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
    checkState(!getCatalogProps().isEmpty(), "Must specify Iceberg catalog properties");
    if (getTables() != null) {
      checkState(!getDynamicTablesEnabled(), "Cannot specify both static and dynamic table names");
    } else if (getDynamicTablesEnabled()) {
      checkState(
          getTablesRouteField() != null, "Must specify a route field if using dynamic table names");
    } else {
      throw new ConfigException("Must specify table name(s)");
    }
  }

  private void checkState(boolean condition, String msg) {
    if (!condition) {
      throw new ConfigException(msg);
    }
  }

  public String getConnectorName() {
    return originalProps.get(NAME_PROP);
  }

  public String getTransactionalSuffix() {
    // this is for internal use and is not part of the config definition...
    return originalProps.get(INTERNAL_TRANSACTIONAL_SUFFIX_PROP);
  }

  public SortedSet<String> getTopics() {
    return new TreeSet<>(getList(SinkConnector.TOPICS_CONFIG));
  }

  public Map<String, String> getCatalogProps() {
    return catalogProps;
  }

  public Map<String, String> getHadoopProps() {
    return hadoopProps;
  }

  public Map<String, String> getKafkaProps() {
    return kafkaProps;
  }

  public String getCatalogName() {
    return getString(CATALOG_NAME_PROP);
  }

  public List<String> getTables() {
    return getList(TABLES_PROP);
  }

  public boolean getDynamicTablesEnabled() {
    return getBoolean(TABLES_DYNAMIC_PROP);
  }

  public String getTablesRouteField() {
    return getString(TABLES_ROUTE_FIELD_PROP);
  }

  public String getTablesDefaultCommitBranch() {
    return getString(TABLES_DEFAULT_COMMIT_BRANCH);
  }

  public TableSinkConfig getTableConfig(String tableName) {
    return tableConfigMap.computeIfAbsent(
        tableName,
        notUsed -> {
          Map<String, String> tableProps =
              PropertyUtil.propertiesWithPrefix(originalProps, TABLE_PROP_PREFIX + tableName + ".");
          String routeRegexStr = tableProps.get(ROUTE_REGEX);
          Pattern routeRegex = routeRegexStr == null ? null : Pattern.compile(routeRegexStr);

          String idColumnsStr = tableProps.get(ID_COLUMNS);
          List<String> idColumns =
              idColumnsStr == null || idColumnsStr.isEmpty()
                  ? ImmutableList.of()
                  : Arrays.stream(idColumnsStr.split(",")).map(String::trim).collect(toList());

          String commitBranch = tableProps.get(COMMIT_BRANCH);
          if (commitBranch == null) {
            commitBranch = getTablesDefaultCommitBranch();
          }

          return new TableSinkConfig(routeRegex, idColumns, commitBranch);
        });
  }

  public String getTablesCdcField() {
    return getString(TABLES_CDC_FIELD_PROP);
  }

  public String getControlTopic() {
    return getString(CONTROL_TOPIC_PROP);
  }

  public String getControlGroupId() {
    String result = getString(CONTROL_GROUP_ID_PROP);
    if (result != null) {
      return result;
    }
    String connectorName = getConnectorName();
    Preconditions.checkNotNull(connectorName, "Connector name cannot be null");
    return DEFAULT_CONTROL_GROUP_PREFIX + connectorName;
  }

  public int getCommitIntervalMs() {
    return getInt(COMMIT_INTERVAL_MS_PROP);
  }

  public int getCommitTimeoutMs() {
    return getInt(COMMIT_TIMEOUT_MS_PROP);
  }

  public int getCommitThreads() {
    return getInt(COMMIT_THREADS_PROP);
  }

  public String getHadoopConfDir() {
    return getString(HADDOP_CONF_DIR_PROP);
  }

  public boolean isUpsertMode() {
    return getBoolean(TABLES_UPSERT_MODE_ENABLED_PROP);
  }

  public boolean isAutoCreate() {
    return getBoolean(TABLES_AUTO_CREATE_ENABLED_PROP);
  }

  public boolean isEvolveSchema() {
    return getBoolean(TABLES_EVOLVE_SCHEMA_ENABLED_PROP);
  }

  public JsonConverter getJsonConverter() {
    return jsonConverter;
  }

  private Map<String, String> loadWorkerProps() {
    String javaCmd = System.getProperty("sun.java.command");
    if (javaCmd != null && !javaCmd.isEmpty()) {
      String[] args = javaCmd.split(" ");
      if (args.length > 1
          && (args[0].endsWith(".ConnectDistributed") || args[0].endsWith(".ConnectStandalone"))) {
        Properties result = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get(args[1]))) {
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
