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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class HiveIcebergStorageHandler implements HiveStoragePredicateHandler, HiveStorageHandler {

  private static final String NAME = "name";
  private static final String WRITE_KEY = "HiveIcebergStorageHandler_write";

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
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> map) {
    Properties props = tableDesc.getProperties();
    Table table = Catalogs.loadTable(conf, props);

    map.put(InputFormatConfig.TABLE_IDENTIFIER, props.getProperty(Catalogs.NAME));
    map.put(InputFormatConfig.TABLE_LOCATION, table.location());
    map.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(table.schema()));
    overlayTableProperties(tableDesc, map);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> map) {
    overlayTableProperties(tableDesc, map);
    map.put(WRITE_KEY, "true");
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
    if (tableDesc != null && tableDesc.getJobProperties() != null &&
        tableDesc.getJobProperties().get(WRITE_KEY) != null) {
      jobConf.set("mapred.output.committer.class", HiveIcebergOutputCommitter.class.getName());
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
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
      ExprNodeDesc exprNodeDesc) {
    DecomposedPredicate predicate = new DecomposedPredicate();
    predicate.residualPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    predicate.pushedPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    return predicate;
  }

  private void overlayTableProperties(TableDesc tableDesc, Map<String, String> map) {
    Properties props = tableDesc.getProperties();
    Table table = Catalogs.loadTable(conf, props);

    Map<String, String> original = new HashMap<>(map);
    map.clear();

    map.putAll(Maps.fromProperties(props));
    map.putAll(original);

    map.put(InputFormatConfig.TABLE_IDENTIFIER, props.getProperty(NAME));
    map.put(InputFormatConfig.TABLE_LOCATION, table.location());
    map.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(table.schema()));
    map.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(table.spec()));
    // We need to remove this otherwise the job.xml will be invalid
    map.remove("columns.comments");
  }
}
