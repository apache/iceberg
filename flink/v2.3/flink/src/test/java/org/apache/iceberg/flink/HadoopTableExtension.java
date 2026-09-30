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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.extension.ExtensionContext;

public class HadoopTableExtension extends HadoopCatalogExtension {
  private final Schema schema;
  private final PartitionSpec partitionSpec;

  private Table table;

  public HadoopTableExtension(String database, String tableName, Schema schema) {
    this(database, tableName, schema, null);
  }

  public HadoopTableExtension(
      String database, String tableName, Schema schema, PartitionSpec partitionSpec) {
    super(database, tableName);
    this.schema = schema;
    this.partitionSpec = partitionSpec;
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    super.beforeEach(context);
    if (partitionSpec == null) {
      this.table = catalog.createTable(TableIdentifier.of(database, tableName), schema);
    } else {
      this.table =
          catalog.createTable(TableIdentifier.of(database, tableName), schema, partitionSpec);
    }
    tableLoader.open();
  }

  public Table table() {
    return table;
  }
}
