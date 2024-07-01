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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

public class TestMetadataTableReadDerivedColumns extends TestBaseWithCatalog {

  @TempDir private Path temp;

  private Long appendSnapshotId0;
  private Long appendSnapshotId1;
  private Long eqDeleteSnapshotId;
  private Long posDeleteSnapshotId;

  private static final Schema SCHEMA =
      new Schema(
          required(1, "booleanCol", Types.BooleanType.get()),
          required(2, "intCol", Types.IntegerType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("intCol").build();

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        // only SparkCatalog supports metadata table sql queries
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties()
      },
    };
  }

  protected String tableName() {
    return tableName.split("\\.")[2];
  }

  protected String database() {
    return tableName.split("\\.")[1];
  }

  private Table createPrimitiveTable() throws IOException {
    Table table =
        catalog.createTable(
            TableIdentifier.of(Namespace.of(database()), tableName()),
            SCHEMA,
            SPEC,
            Collections.singletonMap(TableProperties.FORMAT_VERSION, "2"));

    // write 2 data files
    List<Record> records0 = Lists.newArrayList(createRecord(true, 0), createRecord(false, 0));
    List<Record> records1 = Lists.newArrayList(createRecord(true, 1), createRecord(false, 1));
    DataFile dataFile0 =
        FileHelpers.writeDataFile(
            table, Files.localOutput(temp.toFile()), TestHelpers.Row.of(0), records0);
    DataFile dataFile1 =
        FileHelpers.writeDataFile(
            table, Files.localOutput(temp.toFile()), TestHelpers.Row.of(1), records1);
    table.newAppend().appendFile(dataFile0).commit();
    table.refresh();
    appendSnapshotId0 = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(dataFile1).commit();
    table.refresh();
    appendSnapshotId1 = table.currentSnapshot().snapshotId();

    // write equality deletes
    List<Record> eqDeletes = Lists.newArrayList();
    Schema deleteRowSchema = SCHEMA.select("intCol");
    Record delete = GenericRecord.create(deleteRowSchema);
    eqDeletes.add(delete.copy("intCol", 1));
    DeleteFile equalityDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(temp.toFile()),
            TestHelpers.Row.of(1),
            eqDeletes,
            deleteRowSchema);
    table.newRowDelta().addDeletes(equalityDeletes).commit();
    table.refresh();
    eqDeleteSnapshotId = table.currentSnapshot().snapshotId();

    // write position deletes
    List<PositionDelete<?>> posDeletes =
        Lists.newArrayList(positionDelete(table.schema(), dataFile0.path(), 0L, true, 0));

    DeleteFile posDeleteFiles =
        FileHelpers.writePosDeleteFile(
            table, Files.localOutput(temp.toFile()), TestHelpers.Row.of(0), posDeletes);
    table.newRowDelta().addDeletes(posDeleteFiles).commit();
    table.refresh();
    posDeleteSnapshotId = table.currentSnapshot().snapshotId();

    return table;
  }

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE %s", tableName);
  }

  protected GenericRecord createRecord(boolean booleanCol, int intCol) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.set(0, booleanCol);
    record.set(1, intCol);
    return record;
  }

  private PositionDelete<GenericRecord> positionDelete(
      Schema tableSchema, CharSequence path, Long position, boolean boolValue, int intValue) {
    PositionDelete<GenericRecord> posDelete = PositionDelete.create();
    GenericRecord nested = GenericRecord.create(tableSchema);
    nested.set(0, boolValue);
    nested.set(1, intValue);
    posDelete.set(path, position, nested);
    return posDelete;
  }

  @TestTemplate
  public void testDataSequenceNumberOnDeleteFiles() throws Exception {
    Table table = createPrimitiveTable();
    DeleteFile equalityDelete =
        table.snapshot(eqDeleteSnapshotId).addedDeleteFiles(table.io()).iterator().next();
    DeleteFile positionDelete =
        table.snapshot(posDeleteSnapshotId).addedDeleteFiles(table.io()).iterator().next();

    List<Object[]> expected =
        ImmutableList.of(
            row(equalityDelete.dataSequenceNumber()), row(positionDelete.dataSequenceNumber()));
    String deleteSql = "SELECT  data_sequence_number FROM %s.%s order by data_sequence_number";
    List<Object[]> actual = sql(String.format(deleteSql, tableName, "delete_files"));
    assertEquals(
        "Select of data sequence number only should match record for delete_files table",
        expected,
        actual);
  }

  @TestTemplate
  public void testMixedColumnsOnFiles() throws Exception {
    Table table = createPrimitiveTable();
    DataFile dataFileFirst =
        table.snapshot(appendSnapshotId0).addedDataFiles(table.io()).iterator().next();
    DataFile dataFileSecond =
        table.snapshot(appendSnapshotId1).addedDataFiles(table.io()).iterator().next();
    DeleteFile equalityDelete =
        table.snapshot(eqDeleteSnapshotId).addedDeleteFiles(table.io()).iterator().next();
    DeleteFile positionDelete =
        table.snapshot(posDeleteSnapshotId).addedDeleteFiles(table.io()).iterator().next();

    List<Object[]> expected =
        ImmutableList.of(
            row(dataFileFirst.path(), dataFileFirst.dataSequenceNumber()),
            row(dataFileSecond.path(), dataFileSecond.dataSequenceNumber()),
            row(equalityDelete.path(), equalityDelete.dataSequenceNumber()),
            row(positionDelete.path(), positionDelete.dataSequenceNumber()));
    String sql = "SELECT file_path, data_sequence_number FROM %s.%s order by data_sequence_number";
    List<Object[]> actual = sql(String.format(sql, tableName, "files"));
    assertEquals(
        "Select of derived and non-derived column should match record for files table",
        expected,
        actual);
  }

  @TestTemplate
  public void testVirtualColumnsOnDataFiles() throws Exception {
    Table table = createPrimitiveTable();
    DataFile dataFileFirst =
        table.snapshot(appendSnapshotId0).addedDataFiles(table.io()).iterator().next();
    DataFile dataFileSecond =
        table.snapshot(appendSnapshotId1).addedDataFiles(table.io()).iterator().next();

    List<Object[]> expected =
        ImmutableList.of(
            row(dataFileFirst.dataSequenceNumber(), 0, true, dataFileFirst.recordCount()),
            row(dataFileSecond.dataSequenceNumber(), 1, true, dataFileSecond.recordCount()));

    String dataFileSql =
        "SELECT data_sequence_number,"
            + "readable_metrics.intCol.lower_bound, readable_metrics.booleanCol.upper_bound,"
            + "record_count FROM %s.%s order by data_sequence_number";
    List<Object[]> result = sql(String.format(dataFileSql, tableName, "data_files"));
    assertEquals(
        "Select of data sequence number, readable metrcics and record count should match record for data_files table",
        expected,
        result);
  }
}
