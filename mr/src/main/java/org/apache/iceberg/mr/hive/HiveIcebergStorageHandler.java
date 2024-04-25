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
package org.apache.iceberg.mr.hive;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SerializationUtil;

public class HiveIcebergStorageHandler implements HiveStoragePredicateHandler, HiveStorageHandler {
  private static final Splitter TABLE_NAME_SPLITTER = Splitter.on("..");
  private static final String TABLE_NAME_SEPARATOR = "..";

  static final String WRITE_KEY = "HiveIcebergStorageHandler_write";

  private Configuration conf;

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveIcebergInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveIcebergOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return HiveIcebergSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return new HiveIcebergMetaHook(conf);
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> map) {
    overlayTableProperties(conf, tableDesc, map);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> map) {
    overlayTableProperties(conf, tableDesc, map);
    // For Tez, setting the committer here is enough to make sure it'll be part of the jobConf
    map.put("mapred.output.committer.class", HiveIcebergOutputCommitter.class.getName());
    // For MR, the jobConf is set only in configureJobConf, so we're setting the write key here to
    // detect it over there
    map.put(WRITE_KEY, "true");
    // Putting the key into the table props as well, so that projection pushdown can be determined
    // on a
    // table-level and skipped only for output tables in HiveIcebergSerde. Properties from the map
    // will be present in
    // the serde config for all tables in the query, not just the output tables, so we can't rely on
    // that in the serde.
    tableDesc.getProperties().put(WRITE_KEY, "true");
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> map) {}

  // Override annotation commented out, since this interface method has been introduced only in Hive
  // 3
  // @Override
  public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> secrets) {}

  private void setCommonJobConf(JobConf jobConf) {
    String customValue = jobConf.get("tez.mrreader.config.update.properties", null);
    String value;
    if (null == customValue) {
      value = "hive.io.file.readcolumn.names,hive.io.file.readcolumn.ids";
    } else {
      List<String> valueList = Arrays.asList(customValue.split(","));
      valueList.add("hive.io.file.readcolumn.names");
      valueList.add("hive.io.file.readcolumn.ids");
      List<String> uniqueValues = Lists.newArrayList(Sets.newHashSet(valueList));
      value = String.join(",", uniqueValues);
    }
    jobConf.set("tez.mrreader.config.update.properties", value);
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    setCommonJobConf(jobConf);
    if (tableDesc != null
        && tableDesc.getProperties() != null
        && tableDesc.getProperties().get(WRITE_KEY) != null) {
      String tableName = tableDesc.getTableName();
      Preconditions.checkArgument(
          !tableName.contains(TABLE_NAME_SEPARATOR),
          "Can not handle table "
              + tableName
              + ". Its name contains '"
              + TABLE_NAME_SEPARATOR
              + "'");
      String tables = jobConf.get(InputFormatConfig.OUTPUT_TABLES);
      tables = tables == null ? tableName : tables + TABLE_NAME_SEPARATOR + tableName;
      jobConf.set("mapred.output.committer.class", HiveIcebergOutputCommitter.class.getName());
      jobConf.set(InputFormatConfig.OUTPUT_TABLES, tables);

      String catalogName = tableDesc.getProperties().getProperty(InputFormatConfig.CATALOG_NAME);
      if (catalogName != null) {
        jobConf.set(InputFormatConfig.TABLE_CATALOG_PREFIX + tableName, catalogName);
      }
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }

  /**
   * @param jobConf Job configuration for InputFormat to access
   * @param deserializer Deserializer
   * @param exprNodeDesc Filter expression extracted by Hive
   * @return Entire filter to take advantage of Hive's pruning as well as Iceberg's pruning.
   */
  @Override
  public DecomposedPredicate decomposePredicate(
      JobConf jobConf, Deserializer deserializer, ExprNodeDesc exprNodeDesc) {
    DecomposedPredicate predicate = new DecomposedPredicate();
    predicate.residualPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    predicate.pushedPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    return predicate;
  }

  /**
   * Returns the Table serialized to the configuration based on the table name. If configuration is
   * missing from the FileIO of the table, it will be populated with the input config.
   *
   * @param config The configuration used to get the data from
   * @param name The name of the table we need as returned by TableDesc.getTableName()
   * @return The Table
   */
  public static Table table(Configuration config, String name) {
    Table table =
        SerializationUtil.deserializeFromBase64(
            config.get(InputFormatConfig.SERIALIZED_TABLE_PREFIX + name));
    checkAndSetIoConfig(config, table);
    return table;
  }

  /**
   * If enabled, it populates the FileIO's hadoop configuration with the input config object. This
   * might be necessary when the table object was serialized without the FileIO config.
   *
   * @param config Configuration to set for FileIO, if enabled
   * @param table The Iceberg table object
   */
  public static void checkAndSetIoConfig(Configuration config, Table table) {
    if (table != null
        && config.getBoolean(
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT)
        && table.io() instanceof HadoopConfigurable) {
      ((HadoopConfigurable) table.io()).setConf(config);
    }
  }

  /**
   * If enabled, it ensures that the FileIO's hadoop configuration will not be serialized. This
   * might be desirable for decreasing the overall size of serialized table objects.
   *
   * <p>Note: Skipping FileIO config serialization in this fashion might in turn necessitate calling
   * {@link #checkAndSetIoConfig(Configuration, Table)} on the deserializer-side to enable
   * subsequent use of the FileIO.
   *
   * @param config Configuration to set for FileIO in a transient manner, if enabled
   * @param table The Iceberg table object
   */
  public static void checkAndSkipIoConfigSerialization(Configuration config, Table table) {
    if (table != null
        && config.getBoolean(
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT)
        && table.io() instanceof HadoopConfigurable) {
      ((HadoopConfigurable) table.io())
          .serializeConfWith(conf -> new NonSerializingConfig(config)::get);
    }
  }

  /**
   * Returns the names of the output tables stored in the configuration.
   *
   * @param config The configuration used to get the data from
   * @return The collection of the table names as returned by TableDesc.getTableName()
   */
  public static Collection<String> outputTables(Configuration config) {
    return TABLE_NAME_SPLITTER.splitToList(config.get(InputFormatConfig.OUTPUT_TABLES));
  }

  /**
   * Returns the catalog name serialized to the configuration.
   *
   * @param config The configuration used to get the data from
   * @param name The name of the table we neeed as returned by TableDesc.getTableName()
   * @return catalog name
   */
  public static String catalogName(Configuration config, String name) {
    return config.get(InputFormatConfig.TABLE_CATALOG_PREFIX + name);
  }

  /**
   * Returns the Table Schema serialized to the configuration.
   *
   * @param config The configuration used to get the data from
   * @return The Table Schema object
   */
  public static Schema schema(Configuration config) {
    return SchemaParser.fromJson(config.get(InputFormatConfig.TABLE_SCHEMA));
  }

  /**
   * Stores the serializable table data in the configuration. Currently the following is handled:
   *
   * <ul>
   *   <li>- Table - in case the table is serializable
   *   <li>- Location
   *   <li>- Schema
   *   <li>- Partition specification
   *   <li>- FileIO for handling table files
   *   <li>- Location provider used for file generation
   *   <li>- Encryption manager for encryption handling
   * </ul>
   *
   * @param configuration The configuration storing the catalog information
   * @param tableDesc The table which we want to store to the configuration
   * @param map The map of the configuration properties which we append with the serialized data
   */
  @VisibleForTesting
  static void overlayTableProperties(
      Configuration configuration, TableDesc tableDesc, Map<String, String> map) {
    Properties props = tableDesc.getProperties();
    Table table = Catalogs.loadTable(configuration, props);
    String schemaJson = SchemaParser.toJson(table.schema());

    Maps.fromProperties(props).entrySet().stream()
        .filter(entry -> !map.containsKey(entry.getKey())) // map overrides tableDesc properties
        .forEach(entry -> map.put(entry.getKey(), entry.getValue()));

    map.put(InputFormatConfig.TABLE_IDENTIFIER, props.getProperty(Catalogs.NAME));
    map.put(InputFormatConfig.TABLE_LOCATION, table.location());
    map.put(InputFormatConfig.TABLE_SCHEMA, schemaJson);

    // serialize table object into config
    Table serializableTable = SerializableTable.copyOf(table);
    checkAndSkipIoConfigSerialization(configuration, serializableTable);
    map.put(
        InputFormatConfig.SERIALIZED_TABLE_PREFIX + tableDesc.getTableName(),
        SerializationUtil.serializeToBase64(serializableTable));

    // We need to remove this otherwise the job.xml will be invalid as column comments are separated
    // with '\0' and
    // the serialization utils fail to serialize this character
    map.remove("columns.comments");

    // save schema into table props as well to avoid repeatedly hitting the HMS during serde
    // initializations
    // this is an exception to the interface documentation, but it's a safe operation to add this
    // property
    props.put(InputFormatConfig.TABLE_SCHEMA, schemaJson);
  }

  private static class NonSerializingConfig implements Serializable {

    private final transient Configuration conf;

    NonSerializingConfig(Configuration conf) {
      this.conf = conf;
    }

    public Configuration get() {
      if (conf == null) {
        throw new IllegalStateException(
            "Configuration was not serialized on purpose but was not set manually either");
      }

      return conf;
    }
  }
}
