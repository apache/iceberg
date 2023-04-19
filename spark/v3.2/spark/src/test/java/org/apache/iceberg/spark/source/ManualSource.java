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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class ManualSource implements TableProvider, DataSourceRegister {
  public static final String SHORT_NAME = "manual_source";
  public static final String TABLE_NAME = "TABLE_NAME";
  private static final Map<String, Table> tableMap = Maps.newHashMap();

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
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return getTable(null, null, options).schema();
  }

  @Override
  public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return getTable(null, null, options).partitioning();
  }

  @Override
  public org.apache.spark.sql.connector.catalog.Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    Preconditions.checkArgument(
        properties.containsKey(TABLE_NAME), "Missing property " + TABLE_NAME);
    String tableName = properties.get(TABLE_NAME);
    Preconditions.checkArgument(tableMap.containsKey(tableName), "Table missing " + tableName);
    return tableMap.get(tableName);
  }

  @Override
  public boolean supportsExternalMetadata() {
    return false;
  }
}
