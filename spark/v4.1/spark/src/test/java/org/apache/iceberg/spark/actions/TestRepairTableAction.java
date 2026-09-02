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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Files;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RepairTable;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFile;
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

  @Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[][] {new Object[] {1}, new Object[] {2}, new Object[] {3}};
  }

  @Parameter private int formatVersion;

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

    Snapshot before = table.currentSnapshot();

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedManifests()).isEmpty();
    assertThat(result.repairedEntryCount()).isEqualTo(0);

    table.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .as("should not commit a snapshot when nothing is repaired")
        .isEqualTo(before.snapshotId());
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

    assertThat(currentRows())
        .as("table contents must be unchanged by the repair")
        .containsExactlyInAnyOrderElementsOf(expectedRows);
  }

  @TestTemplate
  public void testRepairPreservesEntryLineage() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);
    List<Row> lineageBefore = entryLineage();

    replaceManifestWithCorruptStats(table, original);

    SparkActions.get().repairTable(table).repairFileMetrics().execute();

    table.refresh();
    assertThat(entryLineage())
        .as("snapshot id and sequence numbers must be carried through the repair")
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
  }

  @TestTemplate
  public void testRepairPositionDeleteStats() throws IOException {
    assumeThat(formatVersion)
        .as("position deletes are written as parquet files in format version 2")
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
  }

  @TestTemplate
  public void testRepairDeleteManifestHoldingBothDeleteTypes() throws IOException {
    assumeThat(formatVersion)
        .as("position deletes are written as parquet files in format version 2")
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
  }

  @TestTemplate
  public void testRepairFailsWhenRepairedManifestIsConcurrentlyReplaced() throws IOException {
    Table table = createTable(PartitionSpec.unpartitioned());
    appendRecords(table, records(4));

    DataFile original = onlyDataFile(table);
    replaceManifestWithCorruptStats(table, original);

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
    replaceManifestWithCorruptStats(table, original);
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
    assertThat(repairedManifestPaths())
        .as("the manifests written by a failed repair must be deleted")
        .isEmpty();
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

    RepairTable.Result result =
        SparkActions.get().repairTable(table).repairFileMetrics().dryRun().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    assertThat(repairedManifestPaths())
        .as("a dry run must not leave the manifests it wrote behind")
        .isEmpty();
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

  /**
   * Returns the manifests written by the repair action that are still present in the metadata
   * directory.
   *
   * <p>Only the manifests the action itself wrote are considered. A failed commit can also leave
   * behind a copy of a manifest made by the format version 1 staging path, which is written and
   * owned by the core rewrite manifests operation rather than by this action.
   */
  private Set<String> repairedManifestPaths() throws IOException {
    Set<String> paths = Sets.newHashSet();
    File metadataDir = new File(tableDir, "metadata");
    File[] files = metadataDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.getName().startsWith("repaired-m-")) {
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

  /** Returns the snapshot id and sequence numbers of every live entry. */
  private List<Row> entryLineage() {
    return spark
        .read()
        .format("iceberg")
        .load(tableLocation + "#entries")
        .filter("status < 2")
        .selectExpr("snapshot_id", "sequence_number", "file_sequence_number", "data_file.file_path")
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
        Files.localOutput(File.createTempFile("eq-deletes", ".parquet", temp.toFile()));
    return FileHelpers.writeDeleteFile(table, output, null, deletes, deleteSchema);
  }

  private DeleteFile writePosDeletes(Table table, List<Pair<CharSequence, Long>> deletes)
      throws IOException {
    OutputFile output =
        Files.localOutput(File.createTempFile("pos-deletes", ".parquet", temp.toFile()));
    return FileHelpers.writeDeleteFile(table, output, null, deletes, formatVersion).first();
  }

  private void replaceManifestWithCorruptStats(Table table, DataFile file) throws IOException {
    ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    corruptStats(table, manifest, file.location());
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
            file.location().equals(location) ? corrupt(spec, file, corruptCounts) : file.copy();
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

  private DataFile corrupt(PartitionSpec spec, DataFile file, boolean corruptCounts) {
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
        .withFileSizeInBytes(corruptCounts ? file.fileSizeInBytes() + 4096 : file.fileSizeInBytes())
        .build();
  }
}
