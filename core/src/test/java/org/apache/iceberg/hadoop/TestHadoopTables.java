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
package org.apache.iceberg.hadoop;

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.SortDirection.ASC;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestHadoopTables {

  private static final HadoopTables TABLES = new HadoopTables();
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()));

  @TempDir private File tableDir;
  @TempDir private File dataDir;

  @Test
  public void testTableExists() {
    assertThat(TABLES.exists(tableDir.toURI().toString())).isFalse();
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();
    TABLES.create(SCHEMA, spec, tableDir.toURI().toString());
    assertThat(TABLES.exists(tableDir.toURI().toString())).isTrue();
  }

  @Test
  public void testDropTable() {
    TABLES.create(SCHEMA, tableDir.toURI().toString());
    TABLES.dropTable(tableDir.toURI().toString());

    assertThatThrownBy(() -> TABLES.load(tableDir.toURI().toString()))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageStartingWith("Table does not exist");
  }

  @Test
  public void testDropTableWithPurge() throws IOException {

    createDummyTable(tableDir, dataDir);

    TABLES.dropTable(tableDir.toURI().toString(), true);
    assertThatThrownBy(() -> TABLES.load(tableDir.toURI().toString()))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageStartingWith("Table does not exist");

    assertThat(dataDir.listFiles()).hasSize(0);
    assertThat(tableDir).doesNotExist();
    assertThat(TABLES.dropTable(tableDir.toURI().toString())).isFalse();
  }

  @Test
  public void testDropTableWithoutPurge() throws IOException {
    createDummyTable(tableDir, dataDir);

    TABLES.dropTable(tableDir.toURI().toString(), false);
    assertThatThrownBy(() -> TABLES.load(tableDir.toURI().toString()))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageStartingWith("Table does not exist");

    assertThat(dataDir.listFiles()).hasSize(1);
    assertThat(tableDir).doesNotExist();
    assertThat(TABLES.dropTable(tableDir.toURI().toString())).isFalse();
  }

  @Test
  public void testDefaultSortOrder() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();
    Table table = TABLES.create(SCHEMA, spec, tableDir.toURI().toString());

    SortOrder sortOrder = table.sortOrder();
    assertThat(sortOrder.orderId()).as("Order ID must match").isEqualTo(0);
    assertThat(sortOrder.isUnsorted()).as("Order must be unsorted").isTrue();
  }

  @Test
  public void testCustomSortOrder() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();
    SortOrder order = SortOrder.builderFor(SCHEMA).asc("id", NULLS_FIRST).build();
    Table table =
        TABLES.create(SCHEMA, spec, order, Maps.newHashMap(), tableDir.toURI().toString());

    SortOrder sortOrder = table.sortOrder();
    assertThat(sortOrder.orderId()).as("Order ID must match").isEqualTo(1);
    assertThat(sortOrder.fields()).as("Order must have 1 field").hasSize(1);
    assertThat(sortOrder.fields().get(0).direction()).as("Direction must match").isEqualTo(ASC);
    assertThat(sortOrder.fields().get(0).nullOrder())
        .as("Null order must match")
        .isEqualTo(NULLS_FIRST);
    Transform<?, ?> transform = Transforms.identity();
    assertThat(sortOrder.fields().get(0).transform())
        .as("Transform must match")
        .isEqualTo(transform);
  }

  @Test
  public void testTableName() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();
    String location = tableDir.toURI().toString();
    TABLES.create(SCHEMA, spec, location);

    Table table = TABLES.load(location);
    assertThat(table.name()).as("Name must match").isEqualTo(location);

    Table snapshotsTable = TABLES.load(location + "#snapshots");
    assertThat(snapshotsTable.name()).as("Name must match").isEqualTo(location + "#snapshots");
  }

  private static void createDummyTable(File tableDir, File dataDir) throws IOException {
    Table table = TABLES.create(SCHEMA, tableDir.toURI().toString());
    AppendFiles append = table.newAppend();
    String data = dataDir.getPath() + "/data.parquet";
    Files.write(Paths.get(data), Lists.newArrayList(), StandardCharsets.UTF_8);
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(data)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    append.appendFile(dataFile);
    append.commit();

    // Make sure that the data file and the manifest dir is created
    assertThat(dataDir.listFiles()).hasSize(1);
    assertThat(tableDir.listFiles()).hasSize(1);
  }
}
