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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

public class TestStaticTable extends HadoopTableTestBase {

  private Table getStaticTable() {
    return TABLES.load(((HasTableOperations) table).operations().current().metadataFileLocation());
  }

  private Table getStaticTable(MetadataTableType type) {
    return TABLES.load(
        ((HasTableOperations) table).operations().current().metadataFileLocation() + "#" + type);
  }

  @Test
  public void testLoadFromMetadata() {
    Table staticTable = getStaticTable();
    assertThat(((HasTableOperations) staticTable).operations())
        .as("Loading a metadata file based table should return StaticTableOperations")
        .isInstanceOf(StaticTableOperations.class);
  }

  @Test
  public void testCannotBeAddedTo() {
    Table staticTable = getStaticTable();
    assertThatThrownBy(() -> staticTable.newOverwrite().addFile(FILE_A).commit())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");
  }

  @Test
  public void testCannotBeDeletedFrom() {
    table.newAppend().appendFile(FILE_A).commit();
    Table staticTable = getStaticTable();
    assertThatThrownBy(() -> staticTable.newDelete().deleteFile(FILE_A).commit())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot modify a static table");
  }

  @Test
  public void testCannotDoIncrementalScanOnMetadataTable() {
    table.newAppend().appendFile(FILE_A).commit();

    for (MetadataTableType type : MetadataTableType.values()) {
      Table staticTable = getStaticTable(type);

      if (type.equals(MetadataTableType.POSITION_DELETES)) {
        assertThatThrownBy(staticTable::newScan)
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot create TableScan from table of type POSITION_DELETES");
      } else {
        assertThatThrownBy(() -> staticTable.newScan().appendsAfter(1))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage(String.format("Cannot incrementally scan table of type %s", type));
      }
    }
  }

  @Test
  public void testHasSameProperties() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    table.newOverwrite().deleteFile(FILE_B).addFile(FILE_C).commit();
    Table staticTable = getStaticTable();
    assertThat(table.history()).as("Same history?").containsAll(staticTable.history());
    assertThat(table.currentSnapshot().snapshotId())
        .as("Same snapshot?")
        .isEqualTo(staticTable.currentSnapshot().snapshotId());
    assertThat(table.properties()).as("Same properties?").isEqualTo(staticTable.properties());
  }

  @Test
  public void testImmutable() {
    table.newAppend().appendFile(FILE_A).commit();
    Table staticTable = getStaticTable();
    long originalSnapshot = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(FILE_B).commit();
    table.newOverwrite().deleteFile(FILE_B).addFile(FILE_C).commit();
    staticTable.refresh();

    assertThat(staticTable.currentSnapshot().snapshotId())
        .as("Snapshot unchanged after table modified")
        .isEqualTo(originalSnapshot);
  }

  @Test
  public void testMetadataTables() {
    for (MetadataTableType type : MetadataTableType.values()) {
      String enumName = type.name().replace("_", "").toLowerCase();
      assertThat(getStaticTable(type).getClass().getName().toLowerCase())
          .as("Should be able to get MetadataTable of type : " + type)
          .contains(enumName);
    }
  }
}
