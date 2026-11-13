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
package org.apache.iceberg.flink.sink.dynamic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.RegistryBasedFileWriterFactory;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestDynamicWriter extends TestFlinkIcebergSinkBase {

  private static final TableIdentifier TABLE1 = TableIdentifier.of("myTable1");
  private static final TableIdentifier TABLE2 = TableIdentifier.of("myTable2");

  @Test
  void testDynamicWriter() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table1 = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);
    Table table2 = catalog.createTable(TABLE2, SimpleDataUtil.SCHEMA);

    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    DynamicRecordInternal record1 = getDynamicRecordInternal(table1);
    DynamicRecordInternal record2 = getDynamicRecordInternal(table2);

    assertThat(getNumDataFiles(table1)).isEqualTo(0);

    dynamicWriter.write(record1, null);
    dynamicWriter.write(record2, null);
    Collection<DynamicWriteResult> writeResults = dynamicWriter.prepareCommit();

    assertThat(writeResults).hasSize(2);
    assertThat(getNumDataFiles(table1)).isEqualTo(1);
    assertThat(
            dynamicWriter
                .getMetrics()
                .writerMetrics(TABLE1.name())
                .getFlushedDataFiles()
                .getCount())
        .isEqualTo(1);
    assertThat(
            dynamicWriter
                .getMetrics()
                .writerMetrics(TABLE2.name())
                .getFlushedDataFiles()
                .getCount())
        .isEqualTo(1);

    WriteResult wr1 = writeResults.iterator().next().writeResult();
    assertThat(wr1.dataFiles().length).isEqualTo(1);
    assertThat(wr1.dataFiles()[0].format()).isEqualTo(FileFormat.PARQUET);
    assertThat(wr1.deleteFiles()).isEmpty();

    dynamicWriter.write(record1, null);
    dynamicWriter.write(record2, null);
    writeResults = dynamicWriter.prepareCommit();

    assertThat(writeResults).hasSize(2);
    assertThat(getNumDataFiles(table1)).isEqualTo(2);
    assertThat(
            dynamicWriter
                .getMetrics()
                .writerMetrics(TABLE1.name())
                .getFlushedDataFiles()
                .getCount())
        .isEqualTo(2);
    assertThat(
            dynamicWriter
                .getMetrics()
                .writerMetrics(TABLE2.name())
                .getFlushedDataFiles()
                .getCount())
        .isEqualTo(2);

    WriteResult wr2 = writeResults.iterator().next().writeResult();
    assertThat(wr2.dataFiles().length).isEqualTo(1);
    assertThat(wr2.dataFiles()[0].format()).isEqualTo(FileFormat.PARQUET);
    assertThat(wr2.deleteFiles()).isEmpty();

    dynamicWriter.close();
  }

  @Test
  void testDynamicWriterPropertiesDefault() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table1 =
        catalog.createTable(
            TABLE1,
            SimpleDataUtil.SCHEMA,
            null,
            ImmutableMap.of("write.parquet.compression-codec", "zstd"));

    DynamicWriter dynamicWriter = createDynamicWriter(catalog);
    DynamicRecordInternal record1 = getDynamicRecordInternal(table1);

    assertThat(getNumDataFiles(table1)).isEqualTo(0);

    dynamicWriter.write(record1, null);
    Map<String, String> properties = properties(dynamicWriter);
    assertThat(properties).containsEntry("write.parquet.compression-codec", "zstd");

    dynamicWriter.close();
  }

  @Test
  void testFlinkConfigOverridesTableProperties() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table1 =
        catalog.createTable(
            TABLE1,
            SimpleDataUtil.SCHEMA,
            null,
            ImmutableMap.of("write.parquet.compression-codec", "zstd"));

    Configuration flinkConfig = new Configuration();
    flinkConfig.set(FlinkWriteOptions.COMPRESSION_CODEC, "snappy");

    DynamicWriter dynamicWriter =
        new DynamicWriter(
            catalog,
            Map.of(),
            flinkConfig,
            100,
            new DynamicWriterMetrics(UnregisteredMetricsGroup.createSinkWriterMetricGroup()),
            0,
            0);
    DynamicRecordInternal record1 = getDynamicRecordInternal(table1);

    dynamicWriter.write(record1, null);
    Map<String, String> properties = properties(dynamicWriter);
    assertThat(properties).containsEntry("write.parquet.compression-codec", "snappy");

    dynamicWriter.close();
  }

  @Test
  void testWritePropertiesOverrideFlinkConfig() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table1 = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);

    Configuration flinkConfig = new Configuration();
    flinkConfig.set(FlinkWriteOptions.COMPRESSION_CODEC, "snappy");

    DynamicWriter dynamicWriter =
        new DynamicWriter(
            catalog,
            ImmutableMap.of("compression-codec", "gzip"),
            flinkConfig,
            100,
            new DynamicWriterMetrics(UnregisteredMetricsGroup.createSinkWriterMetricGroup()),
            0,
            0);
    DynamicRecordInternal record1 = getDynamicRecordInternal(table1);

    dynamicWriter.write(record1, null);
    Map<String, String> properties = properties(dynamicWriter);
    assertThat(properties).containsEntry("write.parquet.compression-codec", "gzip");

    dynamicWriter.close();
  }

  @Test
  void testFlinkConfigFileFormat() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table1 = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);

    Configuration flinkConfig = new Configuration();
    flinkConfig.set(FlinkWriteOptions.WRITE_FORMAT, "orc");

    DynamicWriter dynamicWriter =
        new DynamicWriter(
            catalog,
            Map.of(),
            flinkConfig,
            100,
            new DynamicWriterMetrics(UnregisteredMetricsGroup.createSinkWriterMetricGroup()),
            0,
            0);
    DynamicRecordInternal record1 = getDynamicRecordInternal(table1);

    dynamicWriter.write(record1, null);
    dynamicWriter.prepareCommit();

    File dataDir = new File(URI.create(table1.location()).getPath(), "data");
    File[] files = dataDir.listFiles((dir, name) -> name.endsWith(".orc"));
    assertThat(files).isNotNull().hasSize(1);

    dynamicWriter.close();
  }

  @Test
  void testFlinkConfigTargetFileSize() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table1 = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);

    Configuration flinkConfig = new Configuration();
    flinkConfig.set(FlinkWriteOptions.TARGET_FILE_SIZE_BYTES, 2048L);

    DynamicWriter dynamicWriter =
        new DynamicWriter(
            catalog,
            Map.of(),
            flinkConfig,
            100,
            new DynamicWriterMetrics(UnregisteredMetricsGroup.createSinkWriterMetricGroup()),
            0,
            0);
    DynamicRecordInternal record1 = getDynamicRecordInternal(table1);

    dynamicWriter.write(record1, null);
    dynamicWriter.prepareCommit();

    assertThat(getNumDataFiles(table1)).isEqualTo(1);

    dynamicWriter.close();
  }

  @Test
  void testDynamicWriterUpsert() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    DynamicWriter dyamicWriter = createDynamicWriter(catalog);
    Table table1 = CATALOG_EXTENSION.catalog().createTable(TABLE1, SimpleDataUtil.SCHEMA);

    DynamicRecordInternal record = getDynamicRecordInternal(table1);
    record.setUpsertMode(true);
    record.setEqualityFieldIds(Sets.newHashSet(1));

    dyamicWriter.write(record, null);
    dyamicWriter.prepareCommit();

    assertThat(
            dyamicWriter
                .getMetrics()
                .writerMetrics(TABLE1.name())
                .getFlushedDeleteFiles()
                .getCount())
        .isEqualTo(1);
    assertThat(
            dyamicWriter.getMetrics().writerMetrics(TABLE1.name()).getFlushedDataFiles().getCount())
        .isEqualTo(1);
  }

  @Test
  void testDynamicWriterUpsertNoEqualityFields() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    DynamicWriter dyamicWriter = createDynamicWriter(catalog);
    Table table1 = CATALOG_EXTENSION.catalog().createTable(TABLE1, SimpleDataUtil.SCHEMA);

    DynamicRecordInternal record = getDynamicRecordInternal(table1);
    record.setUpsertMode(true);

    assertThatThrownBy(() -> dyamicWriter.write(record, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Equality field columns shouldn't be empty when configuring to use UPSERT data.");
  }

  @Test
  void testUniqueFileSuffixOnFactoryRecreation() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table1 = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);

    DynamicWriter dynamicWriter = createDynamicWriter(catalog);
    DynamicRecordInternal record1 = getDynamicRecordInternal(table1);

    dynamicWriter.write(record1, null);
    dynamicWriter.prepareCommit();

    File dataDir1 = new File(URI.create(table1.location()).getPath(), "data");
    File[] files = dataDir1.listFiles((dir, name) -> !name.startsWith("."));
    assertThat(files).isNotNull().hasSize(1);
    File firstFile = files[0];

    // Clear cache which must create new unique files names for the output files
    dynamicWriter.getTaskWriterFactories().clear();

    dynamicWriter.write(record1, null);
    dynamicWriter.prepareCommit();

    files =
        dataDir1.listFiles(
            (dir, name) -> !name.startsWith(".") && !name.equals(firstFile.getName()));
    assertThat(files).isNotNull().hasSize(1);
    File secondFile = files[0];

    // File names must be different
    assertThat(firstFile.getName()).isNotEqualTo(secondFile.getName());
  }

  @Test
  void testUpsertOfSameKeyAcrossSchemaEvolutionWithinCheckpoint() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);
    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    Schema schemaBefore = table.schema();
    dynamicWriter.write(
        upsertRecord(table, schemaBefore, SimpleDataUtil.createRowData(1, "before")), null);

    // A column added mid-checkpoint gives the table a new schema ID, so the next record for the
    // same table opens a second writer. Its re-write of key 1 must retire the first writer's row.
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    Schema schemaAfter = table.schema();
    dynamicWriter.write(upsertRecord(table, schemaAfter, rowData(1, "after", "x")), null);

    Collection<DynamicWriteResult> results = dynamicWriter.prepareCommit();
    assertThat(results).hasSize(2);
    assertThat(dataFiles(results)).hasSize(2);
    assertThat(deleteFiles(results))
        .filteredOn(file -> file.content() == FileContent.POSITION_DELETES)
        .hasSize(1);
    assertThat(deleteFiles(results))
        .as("Only the first writer's insert of a key it had not seen emits an equality delete")
        .filteredOn(file -> file.content() == FileContent.EQUALITY_DELETES)
        .hasSize(1);

    DynamicWriteResult firstWriter = resultWithDelete(results, FileContent.EQUALITY_DELETES);
    DynamicWriteResult secondWriter = resultWithDelete(results, FileContent.POSITION_DELETES);
    assertThat(secondWriter.writeResult().referencedDataFiles())
        .as("Second writer's position delete must reference the first writer's data file")
        .containsExactly(firstWriter.writeResult().dataFiles()[0].location());

    commit(table, results);
    assertTableRows(table, SimpleDataUtil.createRecord(1, "after", "x"));

    dynamicWriter.close();
  }

  @Test
  void testUpsertAlternatingBetweenSchemaVersionsWithinCheckpoint() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);
    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    Schema schemaBefore = table.schema();
    dynamicWriter.write(
        upsertRecord(table, schemaBefore, SimpleDataUtil.createRowData(1, "100")), null);

    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    Schema schemaAfter = table.schema();
    dynamicWriter.write(upsertRecord(table, schemaAfter, rowData(1, "200", "x")), null);

    // An input still shaped like the old schema resolves to the old table schema, so traffic can
    // return to the first writer after the second one exists. The newest write must win.
    dynamicWriter.write(
        upsertRecord(table, schemaBefore, SimpleDataUtil.createRowData(1, "300")), null);

    Collection<DynamicWriteResult> results = dynamicWriter.prepareCommit();
    assertThat(results).hasSize(2);
    assertThat(dataFiles(results)).hasSize(2);
    assertThat(deleteFiles(results))
        .as("Each writer retires the other writer's row with a position delete")
        .filteredOn(file -> file.content() == FileContent.POSITION_DELETES)
        .hasSize(2);

    commit(table, results);
    assertTableRows(table, SimpleDataUtil.createRecord(1, "300", null));

    dynamicWriter.close();
  }

  @Test
  void testUpsertOfDistinctKeysAcrossSchemaEvolutionWithinCheckpoint() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);
    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    Schema schemaBefore = table.schema();
    dynamicWriter.write(
        upsertRecord(table, schemaBefore, SimpleDataUtil.createRowData(1, "one")), null);

    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    Schema schemaAfter = table.schema();
    dynamicWriter.write(upsertRecord(table, schemaAfter, rowData(2, "two", "x")), null);

    Collection<DynamicWriteResult> results = dynamicWriter.prepareCommit();
    assertThat(results).hasSize(2);
    assertThat(dataFiles(results)).hasSize(2);
    assertThat(deleteFiles(results))
        .as("Distinct keys never hit the shared tracker")
        .noneMatch(file -> file.content() == FileContent.POSITION_DELETES);

    commit(table, results);
    assertTableRows(
        table,
        SimpleDataUtil.createRecord(1, "one", null),
        SimpleDataUtil.createRecord(2, "two", "x"));

    dynamicWriter.close();
  }

  @Test
  void testUpsertOfSameKeyAcrossSchemaEvolutionWithinCheckpointPartitioned() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    PartitionSpec spec = PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("id").build();
    Table table = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA, spec);
    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    Schema schemaBefore = table.schema();
    dynamicWriter.write(
        upsertRecord(table, schemaBefore, SimpleDataUtil.createRowData(1, "before")), null);
    dynamicWriter.write(
        upsertRecord(table, schemaBefore, SimpleDataUtil.createRowData(2, "other")), null);

    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    Schema schemaAfter = table.schema();
    dynamicWriter.write(upsertRecord(table, schemaAfter, rowData(1, "after", "x")), null);

    Collection<DynamicWriteResult> results = dynamicWriter.prepareCommit();
    assertThat(results).hasSize(2);
    assertThat(deleteFiles(results))
        .filteredOn(file -> file.content() == FileContent.POSITION_DELETES)
        .hasSize(1);

    commit(table, results);
    assertTableRows(
        table,
        SimpleDataUtil.createRecord(1, "after", "x"),
        SimpleDataUtil.createRecord(2, "other", null));

    dynamicWriter.close();
  }

  @Test
  void testUpsertAcrossEqualityFieldTypePromotionFallsBackToPrivateTracker() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);
    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    Schema schemaBefore = table.schema();
    dynamicWriter.write(
        upsertRecord(table, schemaBefore, SimpleDataUtil.createRowData(1, "before")), null);

    // Promoting the equality field changes the tracker key type, so the writers cannot share a
    // tracker. The second writer must still work, with a private tracker; the re-write is then
    // retired by an equality delete, which cannot apply within this commit (documented limitation).
    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
    Schema schemaAfter = table.schema();
    dynamicWriter.write(
        upsertRecord(table, schemaAfter, GenericRowData.of(1L, StringData.fromString("after"))),
        null);

    Collection<DynamicWriteResult> results = dynamicWriter.prepareCommit();
    assertThat(results).hasSize(2);
    assertThat(dataFiles(results)).hasSize(2);
    assertThat(deleteFiles(results))
        .as("Each writer emits its own insert-time equality delete and no position delete")
        .hasSize(2)
        .allMatch(file -> file.content() == FileContent.EQUALITY_DELETES);

    commit(table, results);
    Record before = GenericRecord.create(table.schema());
    before.setField("id", 1L);
    before.setField("data", "before");
    Record after = GenericRecord.create(table.schema());
    after.setField("id", 1L);
    after.setField("data", "after");
    assertTableRows(table, before, after);

    dynamicWriter.close();
  }

  @Test
  void testUpsertOfSameKeyAcrossColumnDocChangeWithinCheckpoint() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);
    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    Schema schemaBefore = table.schema();
    dynamicWriter.write(
        upsertRecord(table, schemaBefore, SimpleDataUtil.createRowData(1, "before")), null);

    // The sink evolves column docs on its own; a doc change on the key column must not stop the
    // writers from sharing a tracker, since key values compare by type, not by name or doc.
    table.updateSchema().updateColumnDoc("id", "primary key").commit();
    Schema schemaAfter = table.schema();
    assertThat(schemaAfter.schemaId()).isNotEqualTo(schemaBefore.schemaId());
    dynamicWriter.write(
        upsertRecord(table, schemaAfter, SimpleDataUtil.createRowData(1, "after")), null);

    Collection<DynamicWriteResult> results = dynamicWriter.prepareCommit();
    assertThat(results).hasSize(2);
    assertThat(deleteFiles(results))
        .filteredOn(file -> file.content() == FileContent.POSITION_DELETES)
        .hasSize(1);

    commit(table, results);
    assertTableRows(table, SimpleDataUtil.createRecord(1, "after"));

    dynamicWriter.close();
  }

  @Test
  void testDeleteThroughSecondWriterWithinCheckpoint() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);
    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    Schema schemaBefore = table.schema();
    dynamicWriter.write(
        upsertRecord(table, schemaBefore, SimpleDataUtil.createRowData(1, "before")), null);

    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    Schema schemaAfter = table.schema();
    RowData delete =
        GenericRowData.ofKind(
            RowKind.DELETE, 1, StringData.fromString("before"), StringData.fromString("x"));
    dynamicWriter.write(upsertRecord(table, schemaAfter, delete), null);

    Collection<DynamicWriteResult> results = dynamicWriter.prepareCommit();
    assertThat(results).hasSize(2);
    assertThat(dataFiles(results)).as("The delete writer produces no data file").hasSize(1);
    assertThat(deleteFiles(results))
        .filteredOn(file -> file.content() == FileContent.POSITION_DELETES)
        .hasSize(1);

    commit(table, results);
    assertTableRows(table);

    dynamicWriter.close();
  }

  @Test
  void testCdcUpdateAcrossSchemaEvolutionWithinCheckpoint() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);
    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    Schema schemaBefore = table.schema();
    dynamicWriter.write(
        cdcRecord(table, schemaBefore, SimpleDataUtil.createInsert(1, "before")), null);

    // Without upsert mode an update arrives as UPDATE_BEFORE + UPDATE_AFTER. The UPDATE_BEFORE
    // through the second writer must retire the first writer's row with a position delete.
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    Schema schemaAfter = table.schema();
    dynamicWriter.write(
        cdcRecord(
            table,
            schemaAfter,
            GenericRowData.ofKind(RowKind.UPDATE_BEFORE, 1, StringData.fromString("before"), null)),
        null);
    dynamicWriter.write(
        cdcRecord(
            table,
            schemaAfter,
            GenericRowData.ofKind(
                RowKind.UPDATE_AFTER,
                1,
                StringData.fromString("after"),
                StringData.fromString("x"))),
        null);

    Collection<DynamicWriteResult> results = dynamicWriter.prepareCommit();
    assertThat(results).hasSize(2);
    assertThat(dataFiles(results)).hasSize(2);
    assertThat(deleteFiles(results))
        .as("The only delete is the second writer's position delete; inserts emit no delete")
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.POSITION_DELETES);

    DynamicWriteResult secondWriter = resultWithDelete(results, FileContent.POSITION_DELETES);
    DataFile firstWriterDataFile =
        results.stream()
            .filter(result -> result != secondWriter)
            .findFirst()
            .orElseThrow()
            .writeResult()
            .dataFiles()[0];
    assertThat(secondWriter.writeResult().referencedDataFiles())
        .containsExactly(firstWriterDataFile.location());

    commit(table, results);
    assertTableRows(table, SimpleDataUtil.createRecord(1, "after", "x"));

    dynamicWriter.close();
  }

  @Test
  void testInsertedRowTrackersDoNotOutliveCheckpoint() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);
    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    dynamicWriter.write(
        upsertRecord(table, table.schema(), SimpleDataUtil.createRowData(1, "first")), null);
    assertThat(insertedRowTrackers(dynamicWriter)).hasSize(1);

    Collection<DynamicWriteResult> firstCheckpoint = dynamicWriter.prepareCommit();
    assertThat(insertedRowTrackers(dynamicWriter)).isEmpty();
    commit(table, firstCheckpoint);

    // The key's row is now committed at a lower sequence number, so the next checkpoint retires it
    // with an equality delete. A tracker surviving prepareCommit would instead emit a position
    // delete against the previous checkpoint's data file.
    dynamicWriter.write(
        upsertRecord(table, table.schema(), SimpleDataUtil.createRowData(1, "second")), null);
    Collection<DynamicWriteResult> secondCheckpoint = dynamicWriter.prepareCommit();
    assertThat(deleteFiles(secondCheckpoint))
        .hasSize(1)
        .allMatch(file -> file.content() == FileContent.EQUALITY_DELETES);
    assertThat(secondCheckpoint.iterator().next().writeResult().referencedDataFiles()).isEmpty();

    commit(table, secondCheckpoint);
    assertTableRows(table, SimpleDataUtil.createRecord(1, "second"));

    dynamicWriter.write(
        upsertRecord(table, table.schema(), SimpleDataUtil.createRowData(2, "open")), null);
    assertThat(insertedRowTrackers(dynamicWriter)).hasSize(1);
    dynamicWriter.close();
    assertThat(insertedRowTrackers(dynamicWriter)).isEmpty();
  }

  @Test
  void testInsertedRowTrackersAreScopedPerTable() throws Exception {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    Table table1 = catalog.createTable(TABLE1, SimpleDataUtil.SCHEMA);
    Table table2 = catalog.createTable(TABLE2, SimpleDataUtil.SCHEMA);
    DynamicWriter dynamicWriter = createDynamicWriter(catalog);

    dynamicWriter.write(
        upsertRecord(table1, table1.schema(), SimpleDataUtil.createRowData(1, "table1")), null);
    Schema table2Before = table2.schema();
    dynamicWriter.write(
        upsertRecord(table2, table2Before, SimpleDataUtil.createRowData(1, "before")), null);

    // Only the second table evolves; its second writer shares a tracker with its first writer,
    // never with the other table's writer for the same key.
    table2.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    dynamicWriter.write(upsertRecord(table2, table2.schema(), rowData(1, "after", "x")), null);
    assertThat(insertedRowTrackers(dynamicWriter)).hasSize(2);

    Collection<DynamicWriteResult> results = dynamicWriter.prepareCommit();
    Collection<DynamicWriteResult> table1Results = resultsFor(results, TABLE1);
    Collection<DynamicWriteResult> table2Results = resultsFor(results, TABLE2);
    assertThat(table1Results).hasSize(1);
    assertThat(table2Results).hasSize(2);
    assertThat(deleteFiles(table1Results))
        .noneMatch(file -> file.content() == FileContent.POSITION_DELETES);

    DynamicWriteResult table2Second = resultWithDelete(table2Results, FileContent.POSITION_DELETES);
    DynamicWriteResult table2First = resultWithDelete(table2Results, FileContent.EQUALITY_DELETES);
    assertThat(table2Second.writeResult().referencedDataFiles())
        .containsExactly(table2First.writeResult().dataFiles()[0].location());

    commit(table1, table1Results);
    commit(table2, table2Results);
    assertTableRows(table1, SimpleDataUtil.createRecord(1, "table1"));
    assertTableRows(table2, SimpleDataUtil.createRecord(1, "after", "x"));

    dynamicWriter.close();
  }

  private static DynamicRecordInternal upsertRecord(Table table, Schema schema, RowData row) {
    DynamicRecordInternal record = cdcRecord(table, schema, row);
    record.setUpsertMode(true);
    return record;
  }

  private static DynamicRecordInternal cdcRecord(Table table, Schema schema, RowData row) {
    DynamicRecordInternal record = new DynamicRecordInternal();
    record.setTableName(TableIdentifier.parse(table.name()).name());
    record.setSchema(schema);
    record.setSpec(table.spec());
    record.setEqualityFieldIds(Sets.newHashSet(1));
    record.setRowData(row);
    return record;
  }

  private static Map<?, ?> insertedRowTrackers(DynamicWriter dynamicWriter) {
    DynFields.BoundField<Map<?, ?>> trackersField =
        DynFields.builder()
            .hiddenImpl(DynamicWriter.class, "insertedRowTrackers")
            .build(dynamicWriter);
    return trackersField.get();
  }

  private static Collection<DynamicWriteResult> resultsFor(
      Collection<DynamicWriteResult> results, TableIdentifier tableId) {
    return results.stream()
        .filter(result -> result.key().tableName().equals(tableId.name()))
        .collect(Collectors.toList());
  }

  private static RowData rowData(int id, String data, String extra) {
    return GenericRowData.of(id, StringData.fromString(data), StringData.fromString(extra));
  }

  private static List<DataFile> dataFiles(Collection<DynamicWriteResult> results) {
    List<DataFile> files = Lists.newArrayList();
    results.forEach(result -> files.addAll(Arrays.asList(result.writeResult().dataFiles())));
    return files;
  }

  private static List<DeleteFile> deleteFiles(Collection<DynamicWriteResult> results) {
    List<DeleteFile> files = Lists.newArrayList();
    results.forEach(result -> files.addAll(Arrays.asList(result.writeResult().deleteFiles())));
    return files;
  }

  private static DynamicWriteResult resultWithDelete(
      Collection<DynamicWriteResult> results, FileContent content) {
    return results.stream()
        .filter(
            result ->
                Arrays.stream(result.writeResult().deleteFiles())
                    .anyMatch(file -> file.content() == content))
        .findFirst()
        .orElseThrow();
  }

  // Compares by the table's struct type: IcebergGenerics appends the _pos metadata column to rows
  // read from data files that have position deletes, which breaks element-wise Record equality.
  private static void assertTableRows(Table table, Record... expected) throws Exception {
    table.refresh();
    assertThat(SimpleDataUtil.actualRowSet(table, "*"))
        .isEqualTo(SimpleDataUtil.expectedRowSet(table, expected));
  }

  private static void commit(Table table, Collection<DynamicWriteResult> results) {
    RowDelta rowDelta = table.newRowDelta();
    for (DynamicWriteResult result : results) {
      Arrays.stream(result.writeResult().dataFiles()).forEach(rowDelta::addRows);
      Arrays.stream(result.writeResult().deleteFiles()).forEach(rowDelta::addDeletes);
    }

    rowDelta.commit();
  }

  private static @Nonnull DynamicWriter createDynamicWriter(
      Catalog catalog, Map<String, String> properties) {
    DynamicWriter dynamicWriter =
        new DynamicWriter(
            catalog,
            properties,
            new Configuration(),
            100,
            new DynamicWriterMetrics(UnregisteredMetricsGroup.createSinkWriterMetricGroup()),
            0,
            0);
    return dynamicWriter;
  }

  private static @Nonnull DynamicWriter createDynamicWriter(Catalog catalog) {
    return createDynamicWriter(catalog, Map.of());
  }

  private static @Nonnull DynamicRecordInternal getDynamicRecordInternal(Table table1) {
    DynamicRecordInternal record = new DynamicRecordInternal();
    record.setTableName(TableIdentifier.parse(table1.name()).name());
    record.setSchema(table1.schema());
    record.setSpec(table1.spec());
    record.setRowData(SimpleDataUtil.createRowData(1, "test"));
    return record;
  }

  private static int getNumDataFiles(Table table) {
    File dataDir = new File(URI.create(table.location()).getPath(), "data");
    if (dataDir.exists()) {
      return dataDir.listFiles((dir, name) -> !name.startsWith(".")).length;
    }
    return 0;
  }

  private Map<String, String> properties(DynamicWriter dynamicWriter) {
    DynFields.BoundField<Map<WriteTarget, TaskWriter<RowData>>> writerField =
        DynFields.builder().hiddenImpl(dynamicWriter.getClass(), "writers").build(dynamicWriter);

    DynFields.BoundField<FileWriterFactory<?>> writerFactoryField =
        DynFields.builder()
            .hiddenImpl(BaseTaskWriter.class, "writerFactory")
            .build(writerField.get().values().iterator().next());
    DynFields.BoundField<Map<String, String>> propsField =
        DynFields.builder()
            .hiddenImpl(RegistryBasedFileWriterFactory.class, "writerProperties")
            .build(writerFactoryField.get());
    return propsField.get();
  }
}
