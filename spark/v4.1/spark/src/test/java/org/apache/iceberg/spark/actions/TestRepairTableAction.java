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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RepairTable;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRepairTableAction extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  @Parameters(name = "formatVersion = {0}, fileFormat = {1}")
  public static Object[] parameters() {
    return new Object[][] {
      {1, FileFormat.PARQUET}, {1, FileFormat.ORC}, {1, FileFormat.AVRO},
      {2, FileFormat.PARQUET}, {2, FileFormat.ORC}, {2, FileFormat.AVRO},
      {3, FileFormat.PARQUET}, {3, FileFormat.ORC}, {3, FileFormat.AVRO}
    };
  }

  @Parameter private int formatVersion;

  @Parameter(index = 1)
  private FileFormat fileFormat;

  private String tableLocation = null;

  @TempDir private Path temp;
  @TempDir private File tableDir;

  @BeforeEach
  public void setupTableLocation() {
    this.tableLocation = tableDir.toURI().toString();
  }

  @TestTemplate
  public void testRepairEmptyTable() {
    Table table = createTable(PartitionSpec.unpartitioned());

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedManifests()).isEmpty();
    assertThat(result.repairedEntryCount()).isEqualTo(0);
  }

  @TestTemplate
  public void testRepairTableWithCorrectStats() {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    assertNoRepair(table, true);
  }

  @TestTemplate
  public void testNoRepairSelectedIsNoOp() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);
    replaceManifestWithCorruptStats(table, original);

    table.refresh();
    Snapshot before = table.currentSnapshot();
    DataFile corrupt = onlyDataFile(table);

    // no repair was selected, so execute() must do nothing even though the stats are incorrect
    RepairTable.Result result = SparkActions.get().repairTable(table).execute();

    assertThat(result.repairedManifests()).isEmpty();
    assertThat(result.repairedEntryCount()).isEqualTo(0);

    table.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .as("a repair with nothing selected must not commit")
        .isEqualTo(before.snapshotId());
    assertThat(onlyDataFile(table).recordCount())
        .as("a repair with nothing selected must leave the incorrect stats in place")
        .isEqualTo(corrupt.recordCount());
  }

  @TestTemplate
  public void testRepairIncorrectRecordCountAndFileSize() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    List<Object[]> expectedRows = currentRows();
    DataFile original = onlyDataFile(table);

    // replace the manifest with one whose entry records a wrong record count and file size
    replaceManifestWithCorruptStats(table, original);

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    assertThat(result.repairedManifests()).hasSize(1);

    table.refresh();
    DataFile repaired = onlyDataFile(table);
    assertThat(repaired.recordCount()).isEqualTo(original.recordCount());
    assertThat(repaired.fileSizeInBytes()).isEqualTo(original.fileSizeInBytes());
    assertThat(repaired.location()).isEqualTo(original.location());
    assertThat(repaired.format()).isEqualTo(fileFormat);

    assertThat(currentRows())
        .as("table contents must be unchanged by the repair")
        .containsExactlyInAnyOrderElementsOf(expectedRows);
    assertNoRepair(table, false);
  }

  @TestTemplate
  public void testRepairPreservesEntryLineage() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));
    appendRecords(table, records(2));
    table.rewriteManifests().clusterBy(file -> "").commit();

    List<DataFile> originals = dataFiles(table);
    assertThat(originals).hasSize(2);
    if (formatVersion >= 3) {
      assertThat(originals).allSatisfy(file -> assertThat(file.firstRowId()).isNotNull());
      assertThat(originals).extracting(DataFile::firstRowId).doesNotHaveDuplicates();
    }

    List<Row> lineageBefore = entryLineage();

    replaceManifestWithCorruptStats(table, originals.get(0));

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    table.refresh();
    assertThat(entryLineage())
        .as("snapshot id, sequence numbers and first row IDs must survive the repair")
        .containsExactlyInAnyOrderElementsOf(lineageBefore);
  }

  @TestTemplate
  public void testDryRunDoesNotCommit() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);
    replaceManifestWithCorruptStats(table, original);

    table.refresh();
    Snapshot before = table.currentSnapshot();
    DataFile corrupt = onlyDataFile(table);

    RepairTable.Result result =
        SparkActions.get().repairTable(table).repairFileMetrics().dryRun().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    assertThat(result.repairedManifests()).hasSize(1);

    table.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .as("dry run must not commit")
        .isEqualTo(before.snapshotId());
    assertThat(onlyDataFile(table).recordCount())
        .as("dry run must leave the incorrect stats in place")
        .isEqualTo(corrupt.recordCount());
  }

  @TestTemplate
  public void testRepairOnlyRewritesAffectedManifests() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(2));
    appendRecords(table, records(2));

    table.refresh();
    assertThat(table.currentSnapshot().dataManifests(table.io())).hasSize(2);

    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
    ManifestFile untouched = manifests.get(1);

    // corrupt the entry of one manifest only
    DataFile fileToCorrupt = readDataFiles(table, manifests.get(0)).get(0);
    corruptStats(table, manifests.get(0), fileToCorrupt.location());

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedManifests()).hasSize(1);
    assertThat(result.repairedEntryCount()).isEqualTo(1);

    table.refresh();
    assertThat(table.currentSnapshot().dataManifests(table.io()))
        .as("the manifest without incorrect entries must be left in place")
        .anyMatch(manifest -> manifest.path().equals(untouched.path()));
  }

  @TestTemplate
  public void testRepairPartitionedTable() throws IOException {
    Table table = createTable(PartitionSpec.builderFor(SCHEMA).identity("c1").build());

    Dataset<Row> df =
        spark
            .createDataFrame(
                Lists.newArrayList(
                    new ThreeColumnRecord(1, "AAAA", "A"), new ThreeColumnRecord(2, "BBBB", "B")),
                ThreeColumnRecord.class)
            .coalesce(1);
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    table.refresh();
    List<Object[]> expectedRows = currentRows();
    ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    List<DataFile> files = readDataFiles(table, manifest);

    corruptStats(table, manifest, files.get(0).location());

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    assertThat(currentRows()).containsExactlyInAnyOrderElementsOf(expectedRows);
  }

  @TestTemplate
  public void testRepairSkipsColumnMetricsByDefault() throws IOException {
    assumeThat(fileFormat).isNotEqualTo(FileFormat.AVRO);
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);

    // only the column level statistics are wrong, the record count and the file size are correct
    ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    corruptStats(table, manifest, original.location(), false);

    RepairTable.Result skipped =
        SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(skipped.repairedEntryCount())
        .as("column metrics must not be compared by default")
        .isEqualTo(0);

    RepairTable.Result repaired =
        SparkActions.get()
            .repairTable(table)
            .repairFileMetrics()
            .option(RepairTableSparkAction.REPAIR_COLUMN_METRICS, "true")
            .execute();

    assertThat(repaired.repairedEntryCount())
        .as("column metrics are compared when enabled")
        .isEqualTo(1);
    assertNoRepair(table, true);
  }

  @TestTemplate
  void repairWithCustomColumnMetrics() throws IOException {
    assumeThat(fileFormat).isNotEqualTo(FileFormat.AVRO);
    Table table = createTable(PartitionSpec.unpartitioned());
    table
        .updateProperties()
        .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "c2", "none")
        .commit();
    appendRecords(table, records(4));

    List<Object[]> expectedRows = currentRows();
    DataFile original = onlyDataFile(table);
    int columnId = table.schema().findField("c2").fieldId();
    assertThat(original.valueCounts()).doesNotContainKey(columnId);
    assertThat(original.lowerBounds()).doesNotContainKey(columnId);
    assertThat(original.upperBounds()).doesNotContainKey(columnId);

    replaceManifestWithCorruptStats(table, original);

    RepairTable.Result result =
        SparkActions.get()
            .repairTable(table)
            .repairFileMetrics()
            .option(RepairTableSparkAction.REPAIR_COLUMN_METRICS, "true")
            .execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    assertThat(result.repairedManifests()).hasSize(1);

    DataFile repaired = onlyDataFile(table);
    assertThat(repaired.recordCount()).isEqualTo(original.recordCount());
    assertThat(repaired.fileSizeInBytes()).isEqualTo(original.fileSizeInBytes());
    assertThat(repaired.columnSizes()).isEqualTo(original.columnSizes());
    assertThat(repaired.valueCounts()).isEqualTo(original.valueCounts());
    assertThat(repaired.nullValueCounts()).isEqualTo(original.nullValueCounts());
    assertThat(repaired.lowerBounds()).isEqualTo(original.lowerBounds());
    assertThat(repaired.upperBounds()).isEqualTo(original.upperBounds());
    assertThat(currentRows()).containsExactlyInAnyOrderElementsOf(expectedRows);
    assertNoRepair(table, true);
  }

  @TestTemplate
  public void testRepairPreservesColumnStatsWhenColumnMetricsDisabled() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);

    // the record count, file size and column stats of the entry are all wrong
    ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    corruptStats(table, manifest, original.location(), true);
    DataFile corrupt = onlyDataFile(table);

    // repair with column metrics disabled: the record count and file size are corrected, but the
    // wrong column stats must be left untouched rather than replaced with the recomputed ones
    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);

    table.refresh();
    DataFile repaired = onlyDataFile(table);
    assertThat(repaired.recordCount())
        .as("the record count must be repaired")
        .isEqualTo(original.recordCount());
    assertThat(repaired.fileSizeInBytes())
        .as("the file size must be repaired")
        .isEqualTo(original.fileSizeInBytes());
    assertThat(repaired.valueCounts())
        .as("value counts must be kept, not replaced with recomputed ones")
        .isEqualTo(corrupt.valueCounts());
    assertThat(repaired.nullValueCounts())
        .as("null value counts must be kept, not replaced with recomputed ones")
        .isEqualTo(corrupt.nullValueCounts());
    assertThat(repaired.columnSizes())
        .as("column sizes must be kept, not replaced with recomputed ones")
        .isEqualTo(corrupt.columnSizes());
    assertThat(repaired.nanValueCounts()).isEqualTo(corrupt.nanValueCounts());
    assertThat(repaired.lowerBounds()).isEqualTo(corrupt.lowerBounds());
    assertThat(repaired.upperBounds()).isEqualTo(corrupt.upperBounds());
    assertThat(repaired.avgValueSizes()).isEqualTo(corrupt.avgValueSizes());
    assertNoRepair(table, false);
  }

  @TestTemplate
  void renamedColumnMetricsAreUnchanged() throws IOException {
    assumeThat(fileFormat).isNotEqualTo(FileFormat.AVRO);
    Table table = createTable(PartitionSpec.unpartitioned());
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none")
        .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "c2", "full")
        .commit();
    appendRecords(table, records(4));
    DataFile original = onlyDataFile(table);
    int columnId = table.schema().findField("c2").fieldId();
    assertThat(original.valueCounts()).containsOnlyKeys(columnId);

    table.updateSchema().renameColumn("c2", "renamed").commit();
    assertThat(table.properties())
        .containsEntry(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "renamed", "full")
        .doesNotContainKey(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "c2");

    assertNoRepair(table, true);
    assertThat(onlyDataFile(table).lowerBounds()).isEqualTo(original.lowerBounds());
  }

  @TestTemplate
  void repairWithCustomInferredMetricsLimit() throws IOException {
    assumeThat(fileFormat).isNotEqualTo(FileFormat.AVRO);
    Table table = createTable(PartitionSpec.unpartitioned());
    table
        .updateProperties()
        .set(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "1")
        .set(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "c3", "full")
        .commit();
    appendRecords(table, records(4));
    DataFile original = onlyDataFile(table);
    int inferredId = table.schema().findField("c1").fieldId();
    int explicitId = table.schema().findField("c3").fieldId();
    assertThat(original.valueCounts()).containsOnlyKeys(inferredId, explicitId);
    assertThat(original.lowerBounds()).containsOnlyKeys(inferredId, explicitId);
    assertNoRepair(table, true);

    replaceManifestWithCorruptStats(table, original);
    RepairTable.Result result =
        SparkActions.get()
            .repairTable(table)
            .repairFileMetrics()
            .option(RepairTableSparkAction.REPAIR_COLUMN_METRICS, "true")
            .execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1L);
    DataFile repaired = onlyDataFile(table);
    assertThat(repaired.valueCounts()).isEqualTo(original.valueCounts());
    assertThat(repaired.nullValueCounts()).isEqualTo(original.nullValueCounts());
    assertThat(repaired.columnSizes()).isEqualTo(original.columnSizes());
    assertThat(repaired.lowerBounds()).isEqualTo(original.lowerBounds());
    assertThat(repaired.upperBounds()).isEqualTo(original.upperBounds());
    assertNoRepair(table, true);
  }

  @TestTemplate
  void nanMetricsArePreserved() throws IOException {
    assumeThat(fileFormat).isNotEqualTo(FileFormat.AVRO);
    Table table = createTable(PartitionSpec.unpartitioned());
    table
        .updateSchema()
        .addColumn("float_col", Types.FloatType.get())
        .addColumn("double_col", Types.DoubleType.get())
        .commit();
    spark
        .sql(
            "SELECT 1 AS c1, 'a' AS c2, 'b' AS c3, "
                + "CAST(value AS FLOAT) AS float_col, CAST(value AS DOUBLE) AS double_col "
                + "FROM VALUES ('NaN'), ('1.0'), ('2.0'), (NULL) AS input(value)")
        .coalesce(1)
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
    DataFile original = onlyDataFile(table);
    assertThat(original.nanValueCounts())
        .containsEntry(table.schema().findField("float_col").fieldId(), 1L)
        .containsEntry(table.schema().findField("double_col").fieldId(), 1L);
    assertNoRepair(table, true);

    // Retain writer-tracked metrics while making the file-level metrics incorrect.
    DataFile corrupt =
        DataFiles.builder(table.spec())
            .copy(original)
            .withRecordCount(original.recordCount() + 100)
            .withFileSizeInBytes(original.fileSizeInBytes() + 4096)
            .build();
    table.newOverwrite().deleteFile(original).addFile(corrupt).commit();
    RepairTable.Result result =
        SparkActions.get()
            .repairTable(table)
            .repairFileMetrics()
            .option(RepairTableSparkAction.REPAIR_COLUMN_METRICS, "true")
            .execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1L);
    DataFile repaired = onlyDataFile(table);
    assertThat(repaired.recordCount()).isEqualTo(original.recordCount());
    assertThat(repaired.fileSizeInBytes()).isEqualTo(original.fileSizeInBytes());
    assertThat(repaired.nanValueCounts()).isEqualTo(original.nanValueCounts());
    assertThat(repaired.lowerBounds()).isEqualTo(original.lowerBounds());
    assertThat(repaired.upperBounds()).isEqualTo(original.upperBounds());
    assertNoRepair(table, true);
  }

  @TestTemplate
  void correctGeometryMetricsAreUnchanged() throws IOException {
    assumeThat(formatVersion).isEqualTo(3);
    assumeThat(fileFormat).isEqualTo(FileFormat.PARQUET);
    Table table = createTable(PartitionSpec.unpartitioned());
    table.updateSchema().addColumn("geom", Types.GeometryType.crs84()).commit();
    Record template = GenericRecord.create(table.schema());
    List<Record> rows =
        Lists.newArrayList(
            template.copy("geom", wkbPoint(30, 10)), template.copy("geom", wkbPoint(-5, 40)));
    DataFile file =
        FileHelpers.writeDataFile(
            table,
            table.io().newOutputFile(temp.resolve(fileFormat.addExtension("data")).toString()),
            rows);
    table.newFastAppend().appendFile(file).commit();

    DataFile original = onlyDataFile(table);
    int geometryId = table.schema().findField("geom").fieldId();
    ByteBuffer lowerBound = original.lowerBounds().get(geometryId);
    ByteBuffer upperBound = original.upperBounds().get(geometryId);
    assertThat(lowerBound).isNotNull();
    assertThat(upperBound).isNotNull();

    assertNoRepair(table, true);

    DataFile unchanged = onlyDataFile(table);
    assertThat(unchanged.lowerBounds().get(geometryId)).isEqualTo(lowerBound);
    assertThat(unchanged.upperBounds().get(geometryId)).isEqualTo(upperBound);
  }

  @TestTemplate
  void repairOverstatedSnapshotTotals() throws IOException {
    repairSnapshotTotals(true);
  }

  @TestTemplate
  void repairUnderstatedSnapshotTotals() throws IOException {
    repairSnapshotTotals(false);
  }

  private void repairSnapshotTotals(boolean overstated) throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    Record template = GenericRecord.create(table.schema());
    List<Record> rows = Lists.newArrayList();
    for (ThreeColumnRecord record : records(4)) {
      rows.add(template.copy("c1", record.getC1(), "c2", record.getC2(), "c3", record.getC3()));
    }

    DataFile original =
        FileHelpers.writeDataFile(
            table,
            table.io().newOutputFile(temp.resolve(fileFormat.addExtension("data")).toString()),
            rows);
    DataFile corrupt =
        DataFiles.builder(table.spec())
            .copy(original)
            .withRecordCount(overstated ? original.recordCount() + 100 : original.recordCount() - 1)
            .withFileSizeInBytes(
                overstated ? original.fileSizeInBytes() + 4096 : original.fileSizeInBytes() - 1)
            .build();
    table.newFastAppend().appendFile(corrupt).commit();
    assertThat(table.currentSnapshot().summary())
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, Long.toString(corrupt.recordCount()))
        .containsEntry(
            SnapshotSummary.TOTAL_FILE_SIZE_PROP, Long.toString(corrupt.fileSizeInBytes()));

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1L);
    table.refresh();
    assertDeleteTotals(table, original, 0L, 0L, 0L, 0L);
    assertNoRepair(table, false);
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

  private void assertDeleteTotals(
      Table table,
      DataFile dataFile,
      long positionDeletes,
      long equalityDeletes,
      long deleteFiles,
      long deleteSize) {
    assertThat(table.currentSnapshot().summary())
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, Long.toString(dataFile.recordCount()))
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DELETE_FILES_PROP, Long.toString(deleteFiles))
        .containsEntry(SnapshotSummary.TOTAL_POS_DELETES_PROP, Long.toString(positionDeletes))
        .containsEntry(SnapshotSummary.TOTAL_EQ_DELETES_PROP, Long.toString(equalityDeletes))
        .containsEntry(
            SnapshotSummary.TOTAL_FILE_SIZE_PROP,
            Long.toString(dataFile.fileSizeInBytes() + deleteSize));
  }

  @TestTemplate
  public void testWithStatsPreservesEqualityFieldIds() {
    // a rebuilt equality delete must keep its equality field ids, otherwise reading the table fails
    // when the delete is applied. FileMetadata.Builder.copy(DeleteFile) does not carry them.
    PartitionSpec spec = PartitionSpec.unpartitioned();
    DeleteFile equalityDelete =
        FileMetadata.deleteFileBuilder(spec)
            .ofEqualityDeletes(2, 3)
            .withPath(tableLocation + "/data/eq-delete.parquet")
            .withFileSizeInBytes(1024)
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(10)
            .build();

    Metrics recomputed = new Metrics(10L, null, null, null, null);
    ContentFile<?> rebuilt = RepairMetrics.withStats(equalityDelete, spec, recomputed, 1024L);

    assertThat(rebuilt.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(((DeleteFile) rebuilt).equalityFieldIds())
        .as("equality field ids must survive a rebuild")
        .containsExactly(2, 3);
  }

  @TestTemplate
  public void testRepairEqualityDeleteStats() throws IOException {
    assumeThat(formatVersion)
        .as("delete files require format version 2 or higher")
        .isGreaterThanOrEqualTo(2);
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    // write a real equality delete file, whose statistics on disk are correct, then commit an entry
    // for it that records the wrong statistics, mimicking a writer that recorded them incorrectly
    DeleteFile delete = writeEqDeletes(table, "c1", 0);
    DeleteFile corruptEntry =
        FileMetadata.deleteFileBuilder(table.spec())
            .copy(delete)
            .ofEqualityDeletes(
                delete.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray())
            .withRecordCount(delete.recordCount() + 100)
            .withFileSizeInBytes(delete.fileSizeInBytes() + 4096)
            .build();
    table.newRowDelta().addDeletes(corruptEntry).commit();
    table.refresh();

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);

    table.refresh();
    DeleteFile repaired = onlyDeleteFile(table);
    assertThat(repaired.recordCount())
        .as("the record count must be repaired")
        .isEqualTo(delete.recordCount());
    assertThat(repaired.fileSizeInBytes())
        .as("the file size must be repaired")
        .isEqualTo(delete.fileSizeInBytes());
    assertThat(repaired.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(repaired.equalityFieldIds())
        .as("equality field ids must survive the repair")
        .isEqualTo(delete.equalityFieldIds());
    assertDeleteTotals(
        table, onlyDataFile(table), 0L, delete.recordCount(), 1L, delete.fileSizeInBytes());
    assertNoRepair(table, false);
  }

  @TestTemplate
  public void testRepairPositionDeleteStats() throws IOException {
    assumeThat(formatVersion)
        .as("position delete files are written in format version 2")
        .isEqualTo(2);
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));
    DataFile dataFile = onlyDataFile(table);

    DeleteFile delete =
        writePosDeletes(table, Lists.newArrayList(Pair.of(dataFile.location(), 0L)));
    DeleteFile corruptEntry =
        FileMetadata.deleteFileBuilder(table.spec())
            .copy(delete)
            .withRecordCount(delete.recordCount() + 100)
            .withFileSizeInBytes(delete.fileSizeInBytes() + 4096)
            .build();
    table.newRowDelta().addDeletes(corruptEntry).commit();
    table.refresh();

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);

    table.refresh();
    DeleteFile repaired = onlyDeleteFile(table);
    assertThat(repaired.recordCount())
        .as("the record count must be repaired")
        .isEqualTo(delete.recordCount());
    assertThat(repaired.fileSizeInBytes())
        .as("the file size must be repaired")
        .isEqualTo(delete.fileSizeInBytes());
    assertThat(repaired.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertDeleteTotals(table, dataFile, delete.recordCount(), 0L, 1L, delete.fileSizeInBytes());
    assertNoRepair(table, false);
  }

  @TestTemplate
  void correctPositionDeleteMetricsAreUnchanged() throws IOException {
    positionDeleteMetricsAreUnchanged(false);
  }

  @TestTemplate
  void correctMultiFilePositionDeleteMetricsAreUnchanged() throws IOException {
    positionDeleteMetricsAreUnchanged(true);
  }

  private void positionDeleteMetricsAreUnchanged(boolean multipleDataFiles) throws IOException {
    assumeThat(formatVersion).isEqualTo(2);
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));
    if (multipleDataFiles) {
      appendRecords(table, records(2));
    }

    List<Pair<CharSequence, Long>> positions = Lists.newArrayList();
    for (DataFile file : dataFiles(table)) {
      positions.add(Pair.of(file.location(), 0L));
    }

    table.newRowDelta().addDeletes(writePosDeletes(table, positions)).commit();
    assertNoRepair(table, true);
  }

  @TestTemplate
  public void testRepairDeleteManifestHoldingBothDeleteTypes() throws IOException {
    assumeThat(formatVersion)
        .as("position delete files are written in format version 2")
        .isEqualTo(2);
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));
    DataFile dataFile = onlyDataFile(table);

    DeleteFile posDelete =
        writePosDeletes(table, Lists.newArrayList(Pair.of(dataFile.location(), 0L)));
    DeleteFile eqDelete = writeEqDeletes(table, "c1", 1);

    // commit both deletes together so they share a single delete manifest, whose entries then have
    // two different content types and therefore two different metrics configs
    DeleteFile posEntry =
        FileMetadata.deleteFileBuilder(table.spec())
            .copy(posDelete)
            .withRecordCount(posDelete.recordCount() + 100)
            .withFileSizeInBytes(posDelete.fileSizeInBytes() + 4096)
            .build();
    DeleteFile eqEntry =
        FileMetadata.deleteFileBuilder(table.spec())
            .copy(eqDelete)
            .ofEqualityDeletes(
                eqDelete.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray())
            .withRecordCount(eqDelete.recordCount() + 100)
            .withFileSizeInBytes(eqDelete.fileSizeInBytes() + 4096)
            .build();
    table.newRowDelta().addDeletes(posEntry).addDeletes(eqEntry).commit();
    table.refresh();
    assertThat(table.currentSnapshot().deleteManifests(table.io()))
        .as("both deletes must land in a single manifest for this to exercise mixed content")
        .hasSize(1);

    // enable column metrics so the metrics config actually matters: the equality delete must be
    // repaired under the table's config, not the position delete's, which is what keying the config
    // by content type ensures
    RepairTable.Result result =
        SparkActions.get()
            .repairTable(table)
            .repairFileMetrics()
            .option(RepairTableSparkAction.REPAIR_COLUMN_METRICS, "true")
            .execute();

    assertThat(result.repairedEntryCount()).isEqualTo(2);
    assertThat(result.repairedManifests()).hasSize(1);

    table.refresh();
    Map<String, DeleteFile> repairedByPath = Maps.newHashMap();
    for (DeleteFile file :
        readDeleteFiles(table, table.currentSnapshot().deleteManifests(table.io()).get(0))) {
      repairedByPath.put(file.location(), file);
    }

    DeleteFile repairedPos = repairedByPath.get(posDelete.location());
    assertThat(repairedPos.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(repairedPos.recordCount()).isEqualTo(posDelete.recordCount());
    assertThat(repairedPos.fileSizeInBytes()).isEqualTo(posDelete.fileSizeInBytes());

    DeleteFile repairedEq = repairedByPath.get(eqDelete.location());
    assertThat(repairedEq.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(repairedEq.recordCount()).isEqualTo(eqDelete.recordCount());
    assertThat(repairedEq.fileSizeInBytes()).isEqualTo(eqDelete.fileSizeInBytes());
    assertThat(repairedEq.equalityFieldIds())
        .as("equality field ids must survive the repair of a mixed manifest")
        .isEqualTo(eqDelete.equalityFieldIds());
    // the equality delete's column stats must be recomputed under the table's config; had the
    // position delete's config been used for it, the value counts would differ from the file
    assertThat(repairedEq.valueCounts())
        .as("equality delete column stats must be recomputed under its own metrics config")
        .isEqualTo(eqDelete.valueCounts());
    assertThat(repairedPos.valueCounts()).isEqualTo(posDelete.valueCounts());
    assertThat(repairedPos.lowerBounds()).isEqualTo(posDelete.lowerBounds());
    assertThat(repairedPos.upperBounds()).isEqualTo(posDelete.upperBounds());
    assertDeleteTotals(
        table,
        dataFile,
        posDelete.recordCount(),
        eqDelete.recordCount(),
        2L,
        posDelete.fileSizeInBytes() + eqDelete.fileSizeInBytes());
    assertNoRepair(table, true);
  }

  @TestTemplate
  public void testRepairPreservesDeletionVectorFields() throws IOException {
    assumeThat(formatVersion)
        .as("deletion vectors are only written in format version 3 or higher")
        .isGreaterThanOrEqualTo(3);
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));
    DataFile dataFile = onlyDataFile(table);

    // a deletion vector (a Puffin blob) shares a delete manifest with a repairable equality
    // delete. repair skips the vector because its metrics cannot be read, but still rewrites the
    // manifest to fix the equality delete, re-serializing the vector through SparkDeleteFile. its
    // v3 fields must survive that round trip.
    DeleteFile dv = writeDV(table, dataFile, 1);

    DeleteFile eqDelete = writeEqDeletes(table, "c1", 1);
    DeleteFile corruptEqEntry =
        FileMetadata.deleteFileBuilder(table.spec())
            .copy(eqDelete)
            .ofEqualityDeletes(
                eqDelete.equalityFieldIds().stream().mapToInt(Integer::intValue).toArray())
            .withRecordCount(eqDelete.recordCount() + 100)
            .withFileSizeInBytes(eqDelete.fileSizeInBytes() + 4096)
            .build();

    // commit both together so the deletion vector and the equality delete share one delete manifest
    table.newRowDelta().addDeletes(dv).addDeletes(corruptEqEntry).commit();
    table.refresh();
    assertThat(table.currentSnapshot().deleteManifests(table.io()))
        .as("the deletion vector and the equality delete must share a single manifest")
        .hasSize(1);

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount())
        .as("only the equality delete is repairable; a deletion vector's metrics cannot be read")
        .isEqualTo(1);

    table.refresh();
    Map<String, DeleteFile> repairedByPath = Maps.newHashMap();
    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      for (DeleteFile file : readDeleteFiles(table, manifest)) {
        repairedByPath.put(file.location(), file);
      }
    }

    DeleteFile repairedDv = repairedByPath.get(dv.location());
    assertThat(repairedDv).as("the deletion vector must survive the repair").isNotNull();
    assertThat(repairedDv.referencedDataFile())
        .as("the referenced data file of the deletion vector must survive the repair")
        .isEqualTo(dv.referencedDataFile());
    assertThat(repairedDv.contentOffset())
        .as("the content offset of the deletion vector must survive the repair")
        .isEqualTo(dv.contentOffset());
    assertThat(repairedDv.contentSizeInBytes())
        .as("the content size of the deletion vector must survive the repair")
        .isEqualTo(dv.contentSizeInBytes());
    assertThat(repairedDv.recordCount()).isEqualTo(dv.recordCount());
    assertDeleteTotals(
        table,
        dataFile,
        dv.recordCount(),
        eqDelete.recordCount(),
        2L,
        dv.contentSizeInBytes() + eqDelete.fileSizeInBytes());
    assertNoRepair(table, false);
  }

  @TestTemplate
  public void testRepairSucceedsWithConcurrentAppend() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);
    replaceManifestWithCorruptStats(table, original);

    // append concurrently, after the repair has determined what to rewrite but before it commits
    RepairTable.Result result =
        repairWithConcurrentChange(table, () -> appendRecords(table, records(2)));

    assertThat(result.repairedEntryCount()).isEqualTo(1);

    table.refresh();
    assertThat(currentRows())
        .as("the concurrently appended records must survive the repair")
        .hasSize(6);
    assertThat(dataFiles(table))
        .as("the stats of the repaired entry must be corrected")
        .anySatisfy(
            file -> {
              assertThat(file.location()).isEqualTo(original.location());
              assertThat(file.recordCount()).isEqualTo(original.recordCount());
              assertThat(file.fileSizeInBytes()).isEqualTo(original.fileSizeInBytes());
            });
    assertThat(table.currentSnapshot().summary())
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "6")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "2")
        .containsEntry(
            SnapshotSummary.TOTAL_FILE_SIZE_PROP,
            Long.toString(dataFiles(table).stream().mapToLong(DataFile::fileSizeInBytes).sum()));
  }

  @TestTemplate
  public void testRepairFailsWhenRepairedManifestIsConcurrentlyReplaced() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);
    replaceManifestWithCorruptRecordCount(table, original);

    table.refresh();
    List<Object[]> rowsBeforeRepair = currentRows();
    DataFile corrupt = onlyDataFile(table);

    // concurrently rewrite the very manifest the repair is about to replace
    assertThatThrownBy(
            () ->
                repairWithConcurrentChange(
                    table,
                    () -> {
                      Table concurrent = TABLES.load(tableLocation);
                      concurrent.rewriteManifests().clusterBy(file -> "").commit();
                    }))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("could not be found in the latest snapshot");

    table.refresh();
    assertThat(currentRows())
        .as("a failed repair must leave the contents of the table unchanged")
        .containsExactlyInAnyOrderElementsOf(rowsBeforeRepair);
    assertThat(onlyDataFile(table).recordCount())
        .as("a failed repair must not correct any stats")
        .isEqualTo(corrupt.recordCount());
  }

  @TestTemplate
  public void testRepairCleansUpManifestsOnCommitFailure() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);
    replaceManifestWithCorruptRecordCount(table, original);
    table.refresh();

    List<Object[]> rowsBeforeRepair = currentRows();
    DataFile corrupt = onlyDataFile(table);

    // fail the commit with a cleanable failure, as a table whose retries are exhausted would
    org.apache.iceberg.RewriteManifests spyRewriteManifests = spy(table.rewriteManifests());
    doThrow(new CommitFailedException("Injected commit failure"))
        .when(spyRewriteManifests)
        .commit();

    Table spyTable = spy(table);
    when(spyTable.rewriteManifests()).thenReturn(spyRewriteManifests);

    assertThatThrownBy(() -> SparkActions.get().repairTable(spyTable).repairFileMetrics().execute())
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected commit failure");

    table.refresh();
    assertThat(currentRows())
        .as("a failed repair must leave the contents of the table unchanged")
        .containsExactlyInAnyOrderElementsOf(rowsBeforeRepair);
    assertThat(onlyDataFile(table).recordCount())
        .as("a failed repair must not correct any stats")
        .isEqualTo(corrupt.recordCount());
    assertThat(manifestPaths())
        .filteredOn(path -> new File(path).getName().startsWith("repaired-m-"))
        .as("the manifests written by a failed repair must be deleted")
        .isEmpty();
  }

  @TestTemplate
  void repairCleansUpManifestsOnWriteFailure() throws IOException {
    assumeThat(fileFormat).isEqualTo(FileFormat.PARQUET);

    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));
    appendRecords(table, records(2));
    table.rewriteManifests().clusterBy(file -> "").commit();

    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
    assertThat(manifests).hasSize(1);
    List<DataFile> files = readDataFiles(table, manifests.get(0));
    assertThat(files).hasSize(2);
    String failingPath = files.get(1).location();
    corruptStats(table, manifests.get(0), failingPath, true, false);

    String metadataBefore =
        ((HasTableOperations) table).operations().current().metadataFileLocation();
    Set<String> manifestsBefore = manifestPaths();
    FileIO failingIO = new FailingMetricsReadFileIO(failingPath);
    Table repairTable =
        new BaseTable(((HasTableOperations) table).operations(), table.name()) {
          @Override
          public FileIO io() {
            return failingIO;
          }
        };

    try {
      assertThatThrownBy(
              () -> SparkActions.get().repairTable(repairTable).repairFileMetrics().execute())
          .hasMessageContaining("Injected metrics read failure")
          .hasRootCauseInstanceOf(MetricsReadFailure.class);
      assertThat(FailingMetricsReadFileIO.manifestCreated).isTrue();
    } finally {
      FailingMetricsReadFileIO.manifestCreated = false;
    }

    table.refresh();
    assertThat(((HasTableOperations) table).operations().current().metadataFileLocation())
        .isEqualTo(metadataBefore);
    assertThat(manifestPaths())
        .as("the partial manifest from the failed task must be deleted")
        .isEqualTo(manifestsBefore);
  }

  @TestTemplate
  void repairCleansUpManifestsOnSnapshotTotalsFailure() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    table.updateProperties().set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "false").commit();
    appendRecords(table, records(4));
    DataFile original = onlyDataFile(table);
    replaceManifestWithCorruptRecordCount(table, original);
    table.refresh();

    String metadataBefore =
        ((HasTableOperations) table).operations().current().metadataFileLocation();
    Set<String> manifestsBefore = manifestPaths();
    FileIO failingIO = new FailingManifestReadFileIO();
    // Format v1 staging must still be able to read replacement manifests on the driver.
    Table repairTable =
        new BaseTable(((HasTableOperations) table).operations(), table.name()) {
          @Override
          public FileIO io() {
            return failingIO;
          }
        };

    assertThatThrownBy(
            () -> SparkActions.get().repairTable(repairTable).repairFileMetrics().execute())
        .isInstanceOf(CleanableFailure.class)
        .hasMessage("Cannot update snapshot totals during repair")
        .hasRootCauseInstanceOf(SnapshotTotalsReadFailure.class)
        .cause()
        .hasMessageContaining("Injected snapshot totals failure");

    table.refresh();
    assertThat(((HasTableOperations) table).operations().current().metadataFileLocation())
        .isEqualTo(metadataBefore);
    assertThat(manifestPaths())
        .as("replacement manifests and staged copies must be deleted after totals fail")
        .isEqualTo(manifestsBefore);
  }

  @TestTemplate
  public void testRepairKeepsManifestsOnCommitStateUnknown() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);
    replaceManifestWithCorruptStats(table, original);
    table.refresh();

    // commit successfully but report the outcome as unknown
    org.apache.iceberg.RewriteManifests rewriteManifests = table.rewriteManifests();
    org.apache.iceberg.RewriteManifests spyRewriteManifests = spy(rewriteManifests);
    doAnswer(
            invocation -> {
              rewriteManifests.commit();
              throw new CommitStateUnknownException(new RuntimeException("Datacenter on Fire"));
            })
        .when(spyRewriteManifests)
        .commit();

    Table spyTable = spy(table);
    when(spyTable.rewriteManifests()).thenReturn(spyRewriteManifests);

    assertThatThrownBy(() -> SparkActions.get().repairTable(spyTable).repairFileMetrics().execute())
        .cause()
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Datacenter on Fire");

    table.refresh();

    // the commit did succeed, so the repaired manifests must not have been deleted
    assertThat(onlyDataFile(table).recordCount())
        .as("the repair committed, so the corrected stats must be readable")
        .isEqualTo(original.recordCount());
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      assertThat(table.io().newInputFile(manifest.path()).exists())
          .as("manifests of a possibly committed repair must not be deleted")
          .isTrue();
    }
  }

  @TestTemplate
  public void testDryRunLeavesNoManifestsBehind() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);
    replaceManifestWithCorruptStats(table, original);
    table.refresh();
    Set<String> manifestsBefore = manifestPaths();

    RepairTable.Result result =
        SparkActions.get().repairTable(table).repairFileMetrics().dryRun().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    assertThat(manifestPaths())
        .as("a dry run must not leave the manifests it wrote behind")
        .isEqualTo(manifestsBefore);
  }

  /**
   * Runs the repair, applying the given change to the table after the manifests to repair have been
   * determined but before the repair commits.
   */
  private RepairTable.Result repairWithConcurrentChange(Table table, Runnable change) {
    Table spyTable = spy(table);
    when(spyTable.rewriteManifests())
        .thenAnswer(
            invocation -> {
              change.run();
              return table.rewriteManifests();
            });

    return SparkActions.get().repairTable(spyTable).repairFileMetrics().execute();
  }

  private Set<String> manifestPaths() throws IOException {
    Set<String> paths = Sets.newHashSet();
    File metadataDir = new File(tableDir, "metadata");
    File[] files = metadataDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.getName().endsWith(".avro")) {
          paths.add(file.getCanonicalPath());
        }
      }
    }

    return paths;
  }

  @TestTemplate
  public void testRepairAfterPartitionSpecEvolution() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);
    assertThat(original.specId()).isEqualTo(0);
    assertThat(original.partition().size()).isEqualTo(0);

    // evolve the table to a partitioned spec; the existing manifest keeps referring to spec 0
    table.updateSpec().addField("c1").commit();
    table.refresh();
    assertThat(table.spec().specId()).isEqualTo(1);

    ManifestFile oldManifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    assertThat(oldManifest.partitionSpecId())
        .as("the manifest written before the evolution must still be tagged with the old spec")
        .isEqualTo(0);

    // corrupt the stats of the entry that still belongs to the original, unpartitioned spec
    corruptStats(table, oldManifest, original.location());

    SparkActions.get().repairTable(table).repairFileMetrics().execute();

    table.refresh();
    DataFile repaired = onlyDataFile(table);
    assertThat(repaired.recordCount())
        .as("the repair must still correct the stats")
        .isEqualTo(original.recordCount());
    assertThat(repaired.specId())
        .as("the repaired entry must keep the spec it was originally written under")
        .isEqualTo(0);
    assertThat(repaired.partition().size())
        .as("an unpartitioned file's partition data must still have zero fields after repair")
        .isEqualTo(0);
  }

  @TestTemplate
  void repairAfterDroppingPartitionSourceColumn() throws IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);

    Table table = createTable(PartitionSpec.builderFor(SCHEMA).bucket("c1", 16).build());
    appendRecords(table, records(8));
    table.rewriteManifests().clusterBy(file -> "").commit();

    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
    assertThat(manifests).hasSize(1);
    ManifestFile manifest = manifests.get(0);
    List<DataFile> files = readDataFiles(table, manifest);
    assertThat(files.size()).isGreaterThan(1);
    Map<String, DataFile> originals = Maps.newHashMap();
    for (DataFile file : files) {
      assertThat(file.partition().get(0, Integer.class)).isNotNull();
      originals.put(file.location(), file);
    }

    List<Row> lineageBefore = entryLineage();
    table.updateSpec().removeField("c1_bucket").addField("c2").commit();
    table.updateSchema().deleteColumn("c1").commit();
    List<Row> rowsBefore = spark.read().format("iceberg").load(tableLocation).collectAsList();
    corruptStats(table, manifest, files.get(0).location());

    String metadataBefore =
        ((HasTableOperations) table).operations().current().metadataFileLocation();
    Set<String> manifestsBefore = manifestPaths();
    RepairTable.Result dryRun =
        SparkActions.get().repairTable(table).repairFileMetrics().dryRun().execute();

    assertThat(dryRun.repairedEntryCount()).isEqualTo(1);
    assertThat(dryRun.repairedManifests()).hasSize(1);
    table.refresh();
    assertThat(((HasTableOperations) table).operations().current().metadataFileLocation())
        .isEqualTo(metadataBefore);
    assertThat(manifestPaths()).isEqualTo(manifestsBefore);

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    assertThat(result.repairedManifests()).hasSize(1);
    table.refresh();
    assertThat(dataFiles(table))
        .hasSize(originals.size())
        .allSatisfy(
            repaired -> {
              DataFile original = originals.get(repaired.location());
              assertThat(original).isNotNull();
              assertThat(repaired.specId()).isEqualTo(original.specId());
              assertThat(repaired.partition().size()).isEqualTo(original.partition().size());
              assertThat(repaired.partition().get(0, Integer.class))
                  .isEqualTo(original.partition().get(0, Integer.class));
              assertThat(repaired.recordCount()).isEqualTo(original.recordCount());
              assertThat(repaired.fileSizeInBytes()).isEqualTo(original.fileSizeInBytes());
            });
    assertThat(entryLineage()).containsExactlyInAnyOrderElementsOf(lineageBefore);
    assertThat(spark.read().format("iceberg").load(tableLocation).collectAsList())
        .containsExactlyInAnyOrderElementsOf(rowsBefore);
  }

  private List<DataFile> dataFiles(Table table) throws IOException {
    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      files.addAll(readDataFiles(table, manifest));
    }

    return files;
  }

  private Table createTable(PartitionSpec spec) {
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name());
    return TABLES.create(SCHEMA, spec, options, tableLocation);
  }

  private List<ThreeColumnRecord> records(int count) {
    List<ThreeColumnRecord> records = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      records.add(new ThreeColumnRecord(i, "AAAA" + i, "A"));
    }

    return records;
  }

  private void appendRecords(Table table, List<ThreeColumnRecord> records) {
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);
    table.refresh();
  }

  private List<Object[]> currentRows() {
    return rowsToJava(
        spark.read().format("iceberg").load(tableLocation).sort("c1", "c2", "c3").collectAsList());
  }

  /** Returns the snapshot id, sequence numbers and first row ID of every live entry. */
  private List<Row> entryLineage() {
    return spark
        .read()
        .format("iceberg")
        .load(tableLocation + "#entries")
        .filter("status < 2")
        .selectExpr(
            "snapshot_id",
            "sequence_number",
            "file_sequence_number",
            "data_file.file_path",
            "data_file.first_row_id")
        .collectAsList();
  }

  private DataFile onlyDataFile(Table table) throws IOException {
    table.refresh();
    ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    List<DataFile> files = readDataFiles(table, manifest);
    assertThat(files).hasSize(1);
    return files.get(0);
  }

  private List<DataFile> readDataFiles(Table table, ManifestFile manifest) throws IOException {
    List<DataFile> files = Lists.newArrayList();
    try (org.apache.iceberg.io.CloseableIterable<DataFile> reader =
        ManifestFiles.read(manifest, table.io(), table.specs())) {
      reader.forEach(file -> files.add(file.copy()));
    }

    return files;
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
    try (org.apache.iceberg.io.CloseableIterable<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
      reader.forEach(file -> files.add(file.copy()));
    }

    return files;
  }

  private DeleteFile writeEqDeletes(Table table, String key, Object... values) throws IOException {
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

  private DeleteFile writePosDeletes(Table table, List<Pair<CharSequence, Long>> deletes)
      throws IOException {
    OutputFile output =
        Files.localOutput(
            File.createTempFile("pos-deletes", fileFormat.addExtension(""), temp.toFile()));
    return FileHelpers.writeDeleteFile(table, output, null, deletes, formatVersion).first();
  }

  private DeleteFile writeDV(Table table, DataFile dataFile, int numPositionsToDelete)
      throws IOException {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);
    try (DVFileWriter closeableWriter = writer) {
      for (int position = 0; position < numPositionsToDelete; position++) {
        closeableWriter.delete(dataFile.location(), position, table.spec(), dataFile.partition());
      }
    }

    List<DeleteFile> deleteFiles = writer.result().deleteFiles();
    assertThat(deleteFiles).hasSize(1);
    return deleteFiles.get(0);
  }

  private void replaceManifestWithCorruptStats(Table table, DataFile file) throws IOException {
    ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    corruptStats(table, manifest, file.location());
  }

  /**
   * Corrupts the record count of the given file's entry while leaving its file size accurate. The
   * table therefore stays readable by a scan even while the corruption is unrepaired, which lets a
   * test that expects the repair to fail still read the table back afterwards.
   */
  private void replaceManifestWithCorruptRecordCount(Table table, DataFile file)
      throws IOException {
    ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    corruptStats(table, manifest, file.location(), true, false);
  }

  /**
   * Rewrites a manifest so that the entry of the given file records an incorrect record count, file
   * size and column statistics, mimicking a writer that recorded them incorrectly.
   *
   * <p>Every other entry of the manifest is carried through unchanged, along with the lineage of
   * all entries, so that the manifest differs from the original only in the statistics of one
   * entry.
   */
  private void corruptStats(Table table, ManifestFile manifest, String location)
      throws IOException {
    corruptStats(table, manifest, location, true);
  }

  /**
   * Rewrites a manifest, corrupting the statistics of the entry of the given file. When {@code
   * corruptCounts} is false, only the column level statistics are dropped, leaving the record count
   * and the file size correct.
   */
  private void corruptStats(
      Table table, ManifestFile manifest, String location, boolean corruptCounts)
      throws IOException {
    corruptStats(table, manifest, location, corruptCounts, corruptCounts);
  }

  /**
   * Rewrites a manifest, corrupting the statistics of the entry of the given file. The record count
   * (along with the column statistics) and the file size are corrupted independently, so that a
   * caller can leave the file size accurate and keep the file readable by a scan while its other
   * statistics are wrong.
   */
  private void corruptStats(
      Table table,
      ManifestFile manifest,
      String location,
      boolean corruptCounts,
      boolean corruptSize)
      throws IOException {
    File manifestFile = File.createTempFile("corrupt-manifest", ".avro", temp.toFile());
    assertThat(manifestFile.delete()).isTrue();
    PartitionSpec spec = table.specs().get(manifest.partitionSpecId());

    // the snapshot id is assigned during commit, so the manifest must be written without one
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(
            formatVersion, spec, table.io().newOutputFile(manifestFile.getCanonicalPath()), null);

    // read the lineage of each entry from the metadata table, it is not exposed by the reader
    Map<String, Row> lineageByPath = Maps.newHashMap();
    for (Row row : entryLineage()) {
      lineageByPath.put(row.getString(3), row);
    }

    try {
      for (DataFile file : readDataFiles(table, manifest)) {
        DataFile toWrite =
            file.location().equals(location)
                ? corrupt(spec, file, corruptCounts, corruptSize)
                : file.copy();
        Row lineage = lineageByPath.get(file.location());
        writer.existing(
            toWrite,
            lineage.getLong(0),
            lineage.getLong(1),
            lineage.isNullAt(2) ? null : lineage.getLong(2));
      }
    } finally {
      writer.close();
    }

    table.rewriteManifests().deleteManifest(manifest).addManifest(writer.toManifestFile()).commit();
    table.refresh();
  }

  private DataFile corrupt(
      PartitionSpec spec, DataFile file, boolean corruptCounts, boolean corruptSize) {
    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .copy(file)
            // drop the column level statistics, keeping the column sizes
            .withMetrics(
                new Metrics(
                    corruptCounts ? file.recordCount() + 100 : file.recordCount(),
                    file.columnSizes(),
                    Maps.newHashMap(),
                    Maps.newHashMap(),
                    Maps.newHashMap()));

    return builder
        .withFileSizeInBytes(corruptSize ? file.fileSizeInBytes() + 4096 : file.fileSizeInBytes())
        .build();
  }

  private static ByteBuffer wkbPoint(double xCoord, double yCoord) {
    return ByteBuffer.allocate(21)
        .order(ByteOrder.LITTLE_ENDIAN)
        .put((byte) 1) // little-endian
        .putInt(1) // WKB geometry type: Point
        .putDouble(xCoord)
        .putDouble(yCoord)
        .flip();
  }

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
      if (path.contains("/repaired-m-")) {
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

  private static class FailingManifestReadFileIO extends HadoopFileIO {
    @Override
    public InputFile newInputFile(ManifestFile manifest) {
      InputFile input = super.newInputFile(manifest);
      if (manifest.path().contains("/repaired-m-") && input.exists()) {
        throw new SnapshotTotalsReadFailure(manifest.path());
      }

      return input;
    }
  }

  private static class SnapshotTotalsReadFailure extends RuntimeException {
    SnapshotTotalsReadFailure(String path) {
      super("Injected snapshot totals failure reading " + path);
    }
  }
}
