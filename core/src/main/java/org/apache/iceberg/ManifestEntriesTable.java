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

package org.apache.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

class ManifestEntriesTable extends BaseMetadataTable {
  private final TableOperations ops;
  private final Table table;

  ManifestEntriesTable(TableOperations ops, Table table) {
    this.ops = ops;
    this.table = table;
  }

  @Override
  Table table() {
    return table;
  }

  @Override
  String metadataTableName() {
    return "entries";
  }

  @Override
  public TableScan newScan() {
    return new EntriesTableScan(ops, table);
  }

  @Override
  public Schema schema() {
    return ManifestEntry.getSchema(table.spec().partitionType());
  }

  @Override
  public PartitionSpec spec() {
    return PartitionSpec.unpartitioned();
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of();
  }

  @Override
  public String location() {
    return table.currentSnapshot().manifestListLocation();
  }
}
