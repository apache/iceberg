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
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public final class TableResolver {

  private TableResolver() {
  }

  static Table resolveTableFromConfiguration(Configuration conf, Properties properties) throws IOException {
    Configuration configuration = new Configuration(conf);
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      configuration.set(entry.getKey().toString(), entry.getValue().toString());
    }
    return resolveTableFromConfiguration(configuration);
  }

  public static Table resolveTableFromConfiguration(Configuration conf) throws IOException {
    //Default to HadoopTables
    String catalogName = conf.get(InputFormatConfig.CATALOG_NAME, InputFormatConfig.HADOOP_TABLES);

    switch (catalogName) {
      case InputFormatConfig.HADOOP_TABLES:
        String tableLocation = conf.get(InputFormatConfig.TABLE_LOCATION);
        Preconditions.checkNotNull(tableLocation, InputFormatConfig.TABLE_LOCATION + " is not set.");
        HadoopTables tables = new HadoopTables(conf);
        return tables.load(tableLocation);

      case InputFormatConfig.HIVE_CATALOG:
        String tableName = conf.get(InputFormatConfig.TABLE_NAME);
        Preconditions.checkNotNull(tableName, InputFormatConfig.TABLE_NAME + " is not set.");
        throw new UnsupportedOperationException(InputFormatConfig.HIVE_CATALOG + " is not supported yet");

      default:
        throw new NoSuchNamespaceException("Catalog " + catalogName + " not supported.");
    }
  }

}
