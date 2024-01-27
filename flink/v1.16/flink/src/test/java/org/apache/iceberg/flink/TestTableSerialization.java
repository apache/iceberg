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

import static org.apache.iceberg.flink.TestHelpers.roundTripKryoSerialize;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTableSerialization {
  private static final HadoopTables TABLES = new HadoopTables();

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "date", Types.StringType.get()),
          optional(4, "double", Types.DoubleType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("date").build();

  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();

  @TempDir private Path temp;
  private Table table;

  @BeforeEach
  public void initTable() throws IOException {
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    File tableLocation = temp.resolve("table").toFile();
    assertThat(tableLocation).doesNotExist();

    this.table = TABLES.create(SCHEMA, SPEC, SORT_ORDER, props, tableLocation.toString());
  }

  @Test
  public void testSerializableTableKryoSerialization() throws IOException {
    SerializableTable serializableTable = (SerializableTable) SerializableTable.copyOf(table);
    TestHelpers.assertSerializedAndLoadedMetadata(
        table, roundTripKryoSerialize(SerializableTable.class, serializableTable));
  }

  @Test
  public void testSerializableMetadataTableKryoSerialization() throws IOException {
    for (MetadataTableType type : MetadataTableType.values()) {
      TableOperations ops = ((HasTableOperations) table).operations();
      Table metadataTable =
          MetadataTableUtils.createMetadataTableInstance(ops, table.name(), "meta", type);
      SerializableTable serializableMetadataTable =
          (SerializableTable) SerializableTable.copyOf(metadataTable);

      TestHelpers.assertSerializedAndLoadedMetadata(
          metadataTable,
          roundTripKryoSerialize(SerializableTable.class, serializableMetadataTable));
    }
  }

  @Test
  public void testSerializableTransactionTableKryoSerialization() throws IOException {
    Transaction txn = table.newTransaction();

    txn.updateProperties().set("k1", "v1").commit();

    Table txnTable = txn.table();
    SerializableTable serializableTxnTable = (SerializableTable) SerializableTable.copyOf(txnTable);

    TestHelpers.assertSerializedMetadata(
        txnTable, roundTripKryoSerialize(SerializableTable.class, serializableTxnTable));
  }
}
