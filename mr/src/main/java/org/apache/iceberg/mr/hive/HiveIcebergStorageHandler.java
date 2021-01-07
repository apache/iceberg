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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.SerializationUtil;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;

public class HiveIcebergStorageHandler implements HiveStoragePredicateHandler, HiveStorageHandler {

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
    Properties props = tableDesc.getProperties();
    Table table = Catalogs.loadTable(conf, props);
    String schemaJson = SchemaParser.toJson(table.schema());

    map.put(InputFormatConfig.TABLE_IDENTIFIER, props.getProperty(Catalogs.NAME));
    map.put(InputFormatConfig.TABLE_LOCATION, table.location());
    map.put(InputFormatConfig.TABLE_SCHEMA, schemaJson);
    if (table instanceof Serializable) {
      map.put(InputFormatConfig.SERIALIZED_TABLE, SerializationUtil.serializeToBase64(table));
    }

    map.put(InputFormatConfig.FILE_IO, SerializationUtil.serializeToBase64(table.io()));
    // save schema into table props as well to avoid repeatedly hitting the HMS during serde initializations
    // this is an exception to the interface documentation, but it's a safe operation to add this property
    props.put(InputFormatConfig.TABLE_SCHEMA, schemaJson);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> map) {

  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> map) {

  }

  // Override annotation commented out, since this interface method has been introduced only in Hive 3
  // @Override
  public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> secrets) {

  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {

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
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc exprNodeDesc) {
    DecomposedPredicate predicate = new DecomposedPredicate();
    predicate.residualPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    predicate.pushedPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    return predicate;
  }

  /**
   * Returns the Table FileIO serialized to the configuration.
   * @param config The configuration used to get the data from
   * @return The Table FileIO object
   */
  public static FileIO io(Configuration config) {
    return SerializationUtil.deserializeFromBase64(config.get(InputFormatConfig.FILE_IO));
  }

  /**
   * Returns the Table LocationProvider serialized to the configuration.
   * @param config The configuration used to get the data from
   * @return The Table LocationProvider object
   */
  public static LocationProvider location(Configuration config) {
    return SerializationUtil.deserializeFromBase64(config.get(InputFormatConfig.LOCATION_PROVIDER));
  }

  /**
   * Returns the Table EncryptionManager serialized to the configuration.
   * @param config The configuration used to get the data from
   * @return The Table EncryptionManager object
   */
  public static EncryptionManager encryption(Configuration config) {
    return SerializationUtil.deserializeFromBase64(config.get(InputFormatConfig.ENCRYPTION_MANAGER));
  }

  /**
   * Returns the Table Schema serialized to the configuration.
   * @param config The configuration used to get the data from
   * @return The Table Schema object
   */
  public static Schema schema(Configuration config) {
    return SchemaParser.fromJson(config.get(InputFormatConfig.TABLE_SCHEMA));
  }

  /**
   * Returns the Table PartitionSpec serialized to the configuration.
   * @param config The configuration used to get the data from
   * @return The Table PartitionSpec object
   */
  public static PartitionSpec spec(Configuration config) {
    return PartitionSpecParser.fromJson(schema(config), config.get(InputFormatConfig.PARTITION_SPEC));
  }

  /**
   * Stores the serializable table data in the configuration.
   * Currently the following is handled:
   * <ul>
   *   <li>- Table - in case the table is serializable</li>
   *   <li>- Location</li>
   *   <li>- Schema</li>
   *   <li>- Partition specification</li>
   *   <li>- FileIO for handling table files</li>
   *   <li>- Location provider used for file generation</li>
   *   <li>- Encryption manager for encryption handling</li>
   * </ul>
   * @param config The target configuration to store to
   * @param table The table which we want to store to the configuration
   */
  @VisibleForTesting
  static void put(Configuration config, Table table) {
    // The Table contains a FileIO and the FileIO serializes the configuration so we might end up recursively
    // serializing the objects. To avoid this unset the values for now before serializing.
    config.unset(InputFormatConfig.SERIALIZED_TABLE);
    config.unset(InputFormatConfig.FILE_IO);
    config.unset(InputFormatConfig.LOCATION_PROVIDER);
    config.unset(InputFormatConfig.ENCRYPTION_MANAGER);
    config.unset(InputFormatConfig.TABLE_LOCATION);
    config.unset(InputFormatConfig.TABLE_SCHEMA);
    config.unset(InputFormatConfig.PARTITION_SPEC);

    String base64Table = table instanceof Serializable ? SerializationUtil.serializeToBase64(table) : null;
    String base64Io = SerializationUtil.serializeToBase64(table.io());
    String base64LocationProvider = SerializationUtil.serializeToBase64(table.locationProvider());
    String base64EncryptionManager = SerializationUtil.serializeToBase64(table.encryption());

    if (base64Table != null) {
      config.set(InputFormatConfig.SERIALIZED_TABLE, base64Table);
    }

    config.set(InputFormatConfig.FILE_IO, base64Io);
    config.set(InputFormatConfig.LOCATION_PROVIDER, base64LocationProvider);
    config.set(InputFormatConfig.ENCRYPTION_MANAGER, base64EncryptionManager);

    config.set(InputFormatConfig.TABLE_LOCATION, table.location());
    config.set(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(table.schema()));
    config.set(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(table.spec()));
  }
}
