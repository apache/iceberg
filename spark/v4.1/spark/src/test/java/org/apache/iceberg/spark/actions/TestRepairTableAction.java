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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.spark.data.TestHelpers.dataFiles;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileGenerationUtil;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RepairTable;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestRepairTableAction extends TestBase {

  private static final int DEFAULT_FORMAT_VERSION = 2;
  private static final FileFormat DEFAULT_FILE_FORMAT = FileFormat.PARQUET;
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  @TempDir private Path temp;
  @TempDir private File tableDir;

  @Test
  void repairEmptyTable() {
    Table table = createTable(PartitionSpec.unpartitioned());

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedManifests()).isEmpty();
    assertThat(result.repairedEntryCount()).isZero();
    assertThat(table.currentSnapshot()).isNull();
  }

  @ParameterizedTest
  @EnumSource(
      value = FileFormat.class,
      names = {"PARQUET", "ORC", "AVRO"})
  void repairTableWithCorrectStats(FileFormat fileFormat) throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned(), DEFAULT_FORMAT_VERSION, fileFormat);
    table.newFastAppend().appendFile(writeDataFile(table)).commit();

    assertNoRepair(table, true);
  }

  @Test
  void noRepairSelectedIsNoOp() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    table.newFastAppend().appendFile(corruptCounts(table, writeDataFile(table))).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    RepairTable.Result result = SparkActions.get().repairTable(table).execute();

    assertThat(result.repairedManifests()).isEmpty();
    assertThat(result.repairedEntryCount()).isZero();
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
  }

  @ParameterizedTest
  @EnumSource(
      value = FileFormat.class,
      names = {"PARQUET", "ORC", "AVRO"})
  void repairIncorrectRecordCountAndFileSize(FileFormat fileFormat) throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned(), DEFAULT_FORMAT_VERSION, fileFormat);
    DataFile original = writeDataFile(table);
    table.newFastAppend().appendFile(corruptCounts(table, original)).commit();

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    DataFile repaired = onlyDataFile(table);
    assertThat(repaired.recordCount()).isEqualTo(original.recordCount());
    assertThat(repaired.fileSizeInBytes()).isEqualTo(original.fileSizeInBytes());
    assertNoRepair(table, false);
  }

  @Test
  void repairOnlyRewritesAffectedManifests() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    table.newFastAppend().appendFile(writeDataFile(table)).commit();
    ManifestFile untouched = table.currentSnapshot().dataManifests(table.io()).get(0);
    table.newFastAppend().appendFile(corruptCounts(table, writeDataFile(table))).commit();
    ManifestFile affected = table.currentSnapshot().dataManifests(table.io()).get(0);

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedManifests()).containsExactly(affected);
    table.refresh();
    assertThat(table.currentSnapshot().dataManifests(table.io()))
        .hasSize(2)
        .contains(untouched)
        .doesNotContain(affected);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void repairPreservesEntryLineage(int formatVersion) throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned(), formatVersion, DEFAULT_FILE_FORMAT);
    table.newFastAppend().appendFile(corruptCounts(table, writeDataFile(table))).commit();
    table.newFastAppend().appendFile(writeDataFile(table)).commit();
    table.rewriteManifests().clusterBy(file -> "").commit();
    List<Row> lineageBefore = entryLineage();
    if (formatVersion >= 3) {
      assertThat(dataFiles(table))
          .extracting(DataFile::firstRowId)
          .doesNotContainNull()
          .doesNotHaveDuplicates();
    }

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    assertThat(entryLineage()).containsExactlyInAnyOrderElementsOf(lineageBefore);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void repairAfterPartitionSpecEvolution(int formatVersion) throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned(), formatVersion, DEFAULT_FILE_FORMAT);
    DataFile original = writeDataFile(table);
    table.newFastAppend().appendFile(corruptCounts(table, original)).commit();
    table.updateSpec().addField("c1").commit();

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    DataFile repaired = onlyDataFile(table);
    assertThat(repaired.specId()).isEqualTo(original.specId());
    assertThat(repaired.partition().size()).isEqualTo(original.partition().size());
    assertThat(table.currentSnapshot().dataManifests(table.io()))
        .extracting(ManifestFile::partitionSpecId)
        .containsExactly(original.specId());
    assertNoRepair(table, false);
  }

  @Test
  void repairAfterDroppingPartitionSourceColumn() throws IOException {
    Table table = createTable(PartitionSpec.builderFor(SCHEMA).bucket("c1", 16).build());
    DataFile original = writeDataFile(table);
    table.newFastAppend().appendFile(corruptCounts(table, original)).commit();
    table.updateSpec().removeField("c1_bucket").addField("c2").commit();
    table.updateSchema().deleteColumn("c1").commit();

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    DataFile repaired = onlyDataFile(table);
    assertThat(repaired.specId()).isEqualTo(original.specId());
    assertThat(repaired.partition().get(0, Integer.class))
        .isEqualTo(original.partition().get(0, Integer.class));
    assertThat(repaired.recordCount()).isEqualTo(original.recordCount());
    assertNoRepair(table, false);
  }

  @Test
  void repairSkipsColumnMetricsByDefault() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    table.newFastAppend().appendFile(corruptColumnMetrics(table, writeDataFile(table))).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isZero();
    assertThat(result.repairedManifests()).isEmpty();
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
  }

  @Test
  void repairColumnMetricsWhenEnabled() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    table
        .updateProperties()
        .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "c2", "none")
        .commit();
    DataFile original = writeDataFile(table);
    table.newFastAppend().appendFile(corruptColumnMetrics(table, original)).commit();

    RepairTable.Result result =
        SparkActions.get()
            .repairTable(table)
            .repairFileMetrics()
            .option(RepairTableSparkAction.REPAIR_COLUMN_METRICS, "true")
            .execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    DataFile repaired = onlyDataFile(table);
    assertThat(repaired.columnSizes()).isEqualTo(original.columnSizes());
    assertThat(repaired.valueCounts()).isEqualTo(original.valueCounts());
    assertThat(repaired.nullValueCounts()).isEqualTo(original.nullValueCounts());
    assertThat(repaired.lowerBounds()).isEqualTo(original.lowerBounds());
    assertThat(repaired.upperBounds()).isEqualTo(original.upperBounds());
    assertNoRepair(table, true);
  }

  @ParameterizedTest
  @EnumSource(
      value = FileFormat.class,
      names = {"PARQUET", "ORC", "AVRO"})
  void repairEqualityDeleteStats(FileFormat fileFormat) throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned(), DEFAULT_FORMAT_VERSION, fileFormat);
    DeleteFile original = writeEqDeletes(table, fileFormat, "c1", 1);
    table.newRowDelta().addDeletes(corruptDeleteCounts(table, original)).commit();

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    DeleteFile repaired = onlyDeleteFile(table);
    assertThat(repaired.recordCount()).isEqualTo(original.recordCount());
    assertThat(repaired.fileSizeInBytes()).isEqualTo(original.fileSizeInBytes());
    assertNoRepair(table, false);
  }

  @ParameterizedTest
  @EnumSource(
      value = FileFormat.class,
      names = {"PARQUET", "ORC", "AVRO"})
  void repairPositionDeleteStats(FileFormat fileFormat) throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned(), DEFAULT_FORMAT_VERSION, fileFormat);
    DeleteFile original = writePosDeletes(table, fileFormat);
    table.newRowDelta().addDeletes(corruptDeleteCounts(table, original)).commit();

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    DeleteFile repaired = onlyDeleteFile(table);
    assertThat(repaired.recordCount()).isEqualTo(original.recordCount());
    assertThat(repaired.fileSizeInBytes()).isEqualTo(original.fileSizeInBytes());
    assertNoRepair(table, false);
  }

  @Test
  void repairDeleteManifestHoldingBothDeleteTypes() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none")
        .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "c1", "full")
        .commit();
    DeleteFile posDelete = writePosDeletes(table, DEFAULT_FILE_FORMAT);
    DeleteFile eqDelete = writeEqDeletes(table, DEFAULT_FILE_FORMAT, "c1", 1);
    table
        .newRowDelta()
        .addDeletes(corruptDeleteCounts(table, posDelete))
        .addDeletes(corruptDeleteCounts(table, eqDelete))
        .commit();
    assertThat(table.currentSnapshot().deleteManifests(table.io())).hasSize(1);

    RepairTable.Result result =
        SparkActions.get()
            .repairTable(table)
            .repairFileMetrics()
            .option(RepairTableSparkAction.REPAIR_COLUMN_METRICS, "true")
            .execute();

    assertThat(result.repairedEntryCount()).isEqualTo(2);
    table.refresh();
    List<DeleteFile> repaired =
        readDeleteFiles(table, table.currentSnapshot().deleteManifests(table.io()).get(0));
    assertThat(repaired)
        .hasSize(2)
        .allSatisfy(
            file -> {
              DeleteFile original =
                  file.content() == FileContent.POSITION_DELETES ? posDelete : eqDelete;
              assertThat(file.columnSizes()).isEqualTo(original.columnSizes());
              assertThat(file.valueCounts()).isEqualTo(original.valueCounts());
              assertThat(file.nullValueCounts()).isEqualTo(original.nullValueCounts());
              assertThat(file.lowerBounds()).isEqualTo(original.lowerBounds());
              assertThat(file.upperBounds()).isEqualTo(original.upperBounds());
            });
    assertNoRepair(table, true);
  }

  @Test
  void repairPreservesDeletionVectorFields() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned(), 3, DEFAULT_FILE_FORMAT);
    DataFile dataFile = writeDataFile(table);
    table.newFastAppend().appendFile(dataFile).commit();
    DeleteFile dv = FileGenerationUtil.generateDV(table, dataFile);
    DeleteFile eqDelete = writeEqDeletes(table, DEFAULT_FILE_FORMAT, "c1", 1);
    table.newRowDelta().addDeletes(dv).addDeletes(corruptDeleteCounts(table, eqDelete)).commit();
    assertThat(table.currentSnapshot().deleteManifests(table.io())).hasSize(1);

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    table.refresh();
    assertThat(readDeleteFiles(table, table.currentSnapshot().deleteManifests(table.io()).get(0)))
        .filteredOn(file -> file.location().equals(dv.location()))
        .singleElement()
        .satisfies(
            repaired -> {
              assertThat(repaired.referencedDataFile()).isEqualTo(dv.referencedDataFile());
              assertThat(repaired.contentOffset()).isEqualTo(dv.contentOffset());
              assertThat(repaired.contentSizeInBytes()).isEqualTo(dv.contentSizeInBytes());
              assertThat(repaired.recordCount()).isEqualTo(dv.recordCount());
            });
    assertNoRepair(table, false);
  }

  @Test
  void repairCleansUpManifestsOnCommitFailure() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    table.newFastAppend().appendFile(corruptCounts(table, writeDataFile(table))).commit();
    Set<String> manifestsBefore = manifestPaths();
    RewriteManifests rewrite = spy(table.rewriteManifests());
    doThrow(new CommitFailedException("Injected commit failure")).when(rewrite).commit();
    Table spyTable = spy(table);
    when(spyTable.rewriteManifests()).thenReturn(rewrite);

    assertThatThrownBy(() -> SparkActions.get().repairTable(spyTable).repairFileMetrics().execute())
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected commit failure");

    assertThat(manifestPaths()).isEqualTo(manifestsBefore);
  }

  @Test
  void repairCleansUpManifestsOnWriteFailure() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    DataFile first = writeDataFile(table);
    DataFile second = writeDataFile(table);
    table.newFastAppend().appendFile(first).appendFile(corruptCounts(table, second)).commit();
    Set<String> manifestsBefore = manifestPaths();
    Table repairTable = withIO(table, new FailingMetricsReadFileIO(second.location()));

    try {
      assertThatThrownBy(
              () -> SparkActions.get().repairTable(repairTable).repairFileMetrics().execute())
          .rootCause()
          .isInstanceOf(MetricsReadFailure.class)
          .hasMessage("Injected metrics read failure reading " + second.location());
      assertThat(FailingMetricsReadFileIO.manifestCreated).isTrue();
    } finally {
      FailingMetricsReadFileIO.manifestCreated = false;
    }

    assertThat(manifestPaths()).isEqualTo(manifestsBefore);
  }

  @Test
  void repairCleansUpManifestsOfEarlierGroupsWhenLaterGroupFails() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    table.newFastAppend().appendFile(corruptCounts(table, writeDataFile(table))).commit();
    table
        .newRowDelta()
        .addDeletes(corruptDeleteCounts(table, writePosDeletes(table, DEFAULT_FILE_FORMAT)))
        .commit();
    Set<String> manifestsBefore = manifestPaths();
    Table repairTable = withIO(table, new FailingSecondManifestWriteFileIO());

    try {
      assertThatThrownBy(
              () -> SparkActions.get().repairTable(repairTable).repairFileMetrics().execute())
          .rootCause()
          .isInstanceOf(ManifestWriteFailure.class)
          .hasMessage("Injected manifest write failure");
      assertThat(FailingSecondManifestWriteFileIO.MANIFESTS.get()).isEqualTo(2);
    } finally {
      FailingSecondManifestWriteFileIO.MANIFESTS.set(0);
    }

    assertThat(manifestPaths()).isEqualTo(manifestsBefore);
  }

  @Test
  void repairKeepsManifestsOnCommitStateUnknown() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    table.newFastAppend().appendFile(corruptCounts(table, writeDataFile(table))).commit();
    Set<String> manifestsBefore = manifestPaths();
    RewriteManifests rewrite = table.rewriteManifests();
    RewriteManifests spyRewrite = spy(rewrite);
    doAnswer(
            invocation -> {
              rewrite.commit();
              throw new CommitStateUnknownException(new RuntimeException("Datacenter on Fire"));
            })
        .when(spyRewrite)
        .commit();
    Table spyTable = spy(table);
    when(spyTable.rewriteManifests()).thenReturn(spyRewrite);

    assertThatThrownBy(() -> SparkActions.get().repairTable(spyTable).repairFileMetrics().execute())
        .isInstanceOf(CommitStateUnknownException.class)
        .cause()
        .hasMessage("Datacenter on Fire");

    assertThat(manifestPaths())
        .containsAll(manifestsBefore)
        .hasSizeGreaterThan(manifestsBefore.size());
    table.refresh();
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      assertThat(table.io().newInputFile(manifest.path()).exists()).isTrue();
    }
  }

  @Test
  void dryRunDoesNotCommit() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    table.newFastAppend().appendFile(corruptCounts(table, writeDataFile(table))).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    RepairTable.Result result =
        SparkActions.get().repairTable(table).repairFileMetrics().dryRun().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    assertThat(result.repairedManifests()).hasSize(1);
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
  }

  @Test
  void dryRunLeavesNoManifestsBehind() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    table.newFastAppend().appendFile(corruptCounts(table, writeDataFile(table))).commit();
    Set<String> manifestsBefore = manifestPaths();

    SparkActions.get().repairTable(table).repairFileMetrics().dryRun().execute();

    assertThat(manifestPaths()).isEqualTo(manifestsBefore);
  }

  private void assertNoRepair(Table table, boolean repairColumnMetrics) {
    long snapshotId = table.currentSnapshot().snapshotId();
    RepairTable.Result result =
        SparkActions.get()
            .repairTable(table)
            .repairFileMetrics()
            .option(
                RepairTableSparkAction.REPAIR_COLUMN_METRICS, Boolean.toString(repairColumnMetrics))
            .execute();

    assertThat(result.repairedEntryCount()).isZero();
    assertThat(result.repairedManifests()).isEmpty();
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
  }

  private Table createTable(PartitionSpec spec) {
    return createTable(spec, DEFAULT_FORMAT_VERSION, DEFAULT_FILE_FORMAT);
  }

  private Table createTable(PartitionSpec spec, int formatVersion, FileFormat fileFormat) {
    return TABLES.create(
        SCHEMA,
        spec,
        Map.of(
            TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
            TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name()),
        tableDir.toURI().toString());
  }

  private DataFile writeDataFile(Table table) throws IOException {
    Record record = GenericRecord.create(table.schema()).copy("c1", 1, "c2", "a", "c3", "b");
    PartitionKey partition = new PartitionKey(table.spec(), table.schema());
    partition.partition(record);
    FileFormat format =
        FileFormat.fromString(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT));
    OutputFile output =
        Files.localOutput(File.createTempFile("data", format.addExtension(""), temp.toFile()));
    return FileHelpers.writeDataFile(table, output, partition, List.of(record));
  }

  private DataFile corruptCounts(Table table, DataFile file) {
    return DataFiles.builder(table.spec())
        .copy(file)
        .withRecordCount(file.recordCount() + 100)
        .withFileSizeInBytes(file.fileSizeInBytes() + 4096)
        .build();
  }

  private DataFile corruptColumnMetrics(Table table, DataFile file) {
    return DataFiles.builder(table.spec())
        .copy(file)
        .withMetrics(new Metrics(file.recordCount()))
        .build();
  }

  // also drops the column metrics, so a test that repairs them has to recompute them
  private DeleteFile corruptDeleteCounts(Table table, DeleteFile file) {
    FileMetadata.Builder builder =
        FileMetadata.deleteFileBuilder(table.spec())
            .copy(file)
            .withMetrics(new Metrics(file.recordCount() + 100))
            .withFileSizeInBytes(file.fileSizeInBytes() + 4096);
    if (file.content() == FileContent.EQUALITY_DELETES) {
      builder.ofEqualityDeletes(
          file.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray());
    }

    return builder.build();
  }

  private Table withIO(Table table, FileIO io) {
    return new BaseTable(((HasTableOperations) table).operations(), table.name()) {
      @Override
      public FileIO io() {
        return io;
      }
    };
  }

  private List<Row> entryLineage() {
    return spark
        .read()
        .format("iceberg")
        .load(tableDir.toURI() + "#entries")
        .filter("status < 2")
        .selectExpr(
            "snapshot_id",
            "sequence_number",
            "file_sequence_number",
            "data_file.file_path",
            "data_file.first_row_id")
        .collectAsList();
  }

  private DataFile onlyDataFile(Table table) {
    table.refresh();
    List<DataFile> files = dataFiles(table);
    assertThat(files).hasSize(1);
    return files.get(0);
  }

  private DeleteFile onlyDeleteFile(Table table) throws IOException {
    table.refresh();
    List<ManifestFile> manifests = table.currentSnapshot().deleteManifests(table.io());
    assertThat(manifests).hasSize(1);
    List<DeleteFile> files = readDeleteFiles(table, manifests.get(0));
    assertThat(files).hasSize(1);
    return files.get(0);
  }

  private List<DeleteFile> readDeleteFiles(Table table, ManifestFile manifest) throws IOException {
    List<DeleteFile> files = Lists.newArrayList();
    try (CloseableIterable<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
      reader.forEach(file -> files.add(file.copy()));
    }

    return files;
  }

  private DeleteFile writeEqDeletes(
      Table table, FileFormat fileFormat, String key, Object... values) throws IOException {
    Schema deleteSchema = table.schema().select(key);
    Record template = GenericRecord.create(deleteSchema);
    List<Record> deletes = Lists.newArrayList();
    for (Object value : values) {
      deletes.add(template.copy(key, value));
    }

    OutputFile output =
        Files.localOutput(
            File.createTempFile("eq-deletes", fileFormat.addExtension(""), temp.toFile()));
    return FileHelpers.writeDeleteFile(table, output, null, deletes, deleteSchema);
  }

  private DeleteFile writePosDeletes(Table table, FileFormat fileFormat) throws IOException {
    OutputFile output =
        Files.localOutput(
            File.createTempFile("pos-deletes", fileFormat.addExtension(""), temp.toFile()));
    return FileHelpers.writeDeleteFile(table, output, List.of(Pair.of("data.parquet", 0L))).first();
  }

  private Set<String> manifestPaths() throws IOException {
    Set<String> paths = Sets.newHashSet();
    File[] files = new File(tableDir, "metadata").listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.getName().endsWith(".avro")) {
          paths.add(file.getCanonicalPath());
        }
      }
    }

    return paths;
  }

  // Static failure state is shared with Spark local tasks, which do not retry.
  private static class FailingMetricsReadFileIO extends HadoopFileIO {
    private static volatile boolean manifestCreated = false;
    private final String failingPath;

    FailingMetricsReadFileIO(String failingPath) {
      this.failingPath = failingPath;
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      if (manifestCreated && path.equals(failingPath)) {
        throw new MetricsReadFailure(path);
      }

      return super.newInputFile(path, length);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      OutputFile output = super.newOutputFile(path);
      if (path.endsWith(".avro")) {
        manifestCreated = true;
      }

      return output;
    }
  }

  private static class MetricsReadFailure extends RuntimeException {
    MetricsReadFailure(String path) {
      super("Injected metrics read failure reading " + path);
    }
  }

  private static class FailingSecondManifestWriteFileIO extends HadoopFileIO {
    private static final AtomicInteger MANIFESTS = new AtomicInteger(0);

    @Override
    public OutputFile newOutputFile(String path) {
      if (path.endsWith(".avro") && MANIFESTS.incrementAndGet() > 1) {
        throw new ManifestWriteFailure();
      }

      return super.newOutputFile(path);
    }
  }

  private static class ManifestWriteFailure extends RuntimeException {
    ManifestWriteFailure() {
      super("Injected manifest write failure");
    }
  }
}
