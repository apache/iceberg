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
package org.apache.iceberg.spark.source;

import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

public class ManualSource implements WriteSupport, DataSourceRegister, DataSourceV2 {
  public static final String SHORT_NAME = "manual_source";
  public static final String TABLE_NAME = "table_name";
  private static final Map<String, Table> tableMap = Maps.newHashMap();

  private SparkSession lazySpark = null;
  private Configuration lazyConf = null;

  public static void setTable(String name, Table table) {
    Preconditions.checkArgument(
        !tableMap.containsKey(name), "Cannot set " + name + ". It is already set");
    tableMap.put(name, table);
  }

  public static void clearTables() {
    tableMap.clear();
  }

  @Override
  public String shortName() {
    return SHORT_NAME;
  }

  @Override
  public Optional<DataSourceWriter> createWriter(
      String writeUUID, StructType dsStruct, SaveMode mode, DataSourceOptions options) {

    Map<String, String> properties = options.asMap();
    Preconditions.checkArgument(
        properties.containsKey(TABLE_NAME), "Missing property " + TABLE_NAME);
    String tableName = properties.get(TABLE_NAME);
    Preconditions.checkArgument(tableMap.containsKey(tableName), "Table missing " + tableName);
    Table table = tableMap.get(tableName);

    SparkWriteConf writeConf = new SparkWriteConf(lazySparkSession(), table, options.asMap());
    Schema writeSchema = SparkSchemaUtil.convert(table.schema(), dsStruct);
    TypeUtil.validateWriteSchema(
        table.schema(), writeSchema, writeConf.checkNullability(), writeConf.checkOrdering());
    SparkUtil.validatePartitionTransforms(table.spec());
    String appId = lazySparkSession().sparkContext().applicationId();
    String wapId = writeConf.wapId();
    boolean replacePartitions = mode == SaveMode.Overwrite;

    return Optional.of(
        new Writer(
            lazySparkSession(),
            table,
            writeConf,
            replacePartitions,
            appId,
            wapId,
            writeSchema,
            dsStruct));
  }

  private SparkSession lazySparkSession() {
    if (lazySpark == null) {
      this.lazySpark = SparkSession.builder().getOrCreate();
    }
    return lazySpark;
  }

  private Configuration lazyBaseConf() {
    if (lazyConf == null) {
      this.lazyConf = lazySparkSession().sessionState().newHadoopConf();
    }
    return lazyConf;
  }
}
