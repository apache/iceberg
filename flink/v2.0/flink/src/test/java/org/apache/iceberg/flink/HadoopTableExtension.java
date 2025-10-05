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
package org.apache.iceberg.flink;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.extension.ExtensionContext;

public class HadoopTableExtension extends HadoopCatalogExtension {
  private final Schema schema;
  private final PartitionSpec partitionSpec;
  private final Map<String, String> properties;

  private Table table;

  public HadoopTableExtension(String database, String tableName, Schema schema) {
    this(database, tableName, schema, null, null);
  }

  public HadoopTableExtension(
      String database, String tableName, Schema schema, PartitionSpec partitionSpec) {
    this(database, tableName, schema, partitionSpec, null);
  }

  public HadoopTableExtension(
      String database, String tableName, Schema schema, Map<String, String> properties) {
    this(database, tableName, schema, null, properties);
  }

  public HadoopTableExtension(
      String database, String tableName, Schema schema, PartitionSpec partitionSpec, Map<String, String> properties) {
    super(database, tableName);
    this.schema = schema;
    this.partitionSpec = partitionSpec;
    this.properties = properties;
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    super.beforeEach(context);
    if (partitionSpec == null) {
      if (properties == null) {
        this.table = catalog.createTable(TableIdentifier.of(database, tableName), schema);
      } else {
        this.table = catalog.createTable(TableIdentifier.of(database, tableName), schema, 
            PartitionSpec.unpartitioned(), null, properties);
      }
    } else {
      if (properties == null) {
        this.table = catalog.createTable(TableIdentifier.of(database, tableName), schema, partitionSpec);
      } else {
        this.table = catalog.createTable(TableIdentifier.of(database, tableName), schema, partitionSpec, null, properties);
      }
    }
    tableLoader.open();
  }

  public Table table() {
    return table;
  }

  /**
   * Create a HadoopTableExtension with format version 3 for nanosecond timestamp support.
   */
  public static HadoopTableExtension withFormatVersion3(String database, String tableName, Schema schema) {
    return new HadoopTableExtension(database, tableName, schema, 
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
  }

  /**
   * Create a HadoopTableExtension with format version 3 for nanosecond timestamp support.
   */
  public static HadoopTableExtension withFormatVersion3(String database, String tableName, Schema schema, PartitionSpec partitionSpec) {
    return new HadoopTableExtension(database, tableName, schema, partitionSpec,
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
  }
}
