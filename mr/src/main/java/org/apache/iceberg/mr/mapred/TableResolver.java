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

package org.apache.iceberg.mr.mapred;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

final class TableResolver {

  private TableResolver() {
  }

  static Table resolveTableFromJob(JobConf conf) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(InputFormatConfig.CATALOG_NAME,
        conf.get(InputFormatConfig.CATALOG_NAME, InputFormatConfig.HADOOP_TABLES)); //Default to HadoopTables
    properties.setProperty(InputFormatConfig.TABLE_LOCATION, extractProperty(conf, InputFormatConfig.TABLE_LOCATION));
    properties.setProperty(InputFormatConfig.TABLE_NAME, extractProperty(conf, InputFormatConfig.TABLE_NAME));
    return resolveTableFromConfiguration(conf, properties);
  }

  static Table resolveTableFromConfiguration(Configuration conf, Properties properties) throws IOException {
    String catalogName = properties.getProperty(InputFormatConfig.CATALOG_NAME, InputFormatConfig.HADOOP_TABLES);
    String tableLocation = properties.getProperty(InputFormatConfig.TABLE_LOCATION);
    String tableName = properties.getProperty(InputFormatConfig.TABLE_NAME);
    Preconditions.checkNotNull(tableLocation, "Table location is not set.");
    Preconditions.checkNotNull(tableName, "Table name is not set.");
    switch (catalogName) {
      case InputFormatConfig.HADOOP_TABLES:
        HadoopTables tables = new HadoopTables(conf);
        return tables.load(tableLocation);
      case InputFormatConfig.HIVE_CATALOG:
        //TODO Implement HiveCatalog
        return null;
      default:
        throw new NoSuchTableException("Table does not exist at location: " + tableLocation);
    }
  }

  protected static String extractProperty(JobConf conf, String key) {
    String value = conf.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Property not set in JobConf: " + key);
    }
    return value;
  }
}
