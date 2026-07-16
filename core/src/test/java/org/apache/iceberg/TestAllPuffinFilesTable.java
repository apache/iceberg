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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestAllPuffinFilesTable extends TestBase {

  private static final String SOURCE_STATISTICS = "statistics";
  private static final String SOURCE_DELETION_VECTOR = "deletion_vector";

  @TestTemplate
  public void testSchema() {
    Schema expected =
        new Schema(
            Types.NestedField.required(
                1,
                "reference_snapshot_id",
                Types.LongType.get(),
                "ID of the snapshot that references the Puffin file"),
            Types.NestedField.required(
                2,
                "file_path",
                Types.StringType.get(),
                "Fully qualified location of the Puffin file"),
            Types.NestedField.required(
                3,
                "source",
                Types.StringType.get(),
                "Metadata source that associates the Puffin file with the reference snapshot"),
            Types.NestedField.required(
                4,
                "file_size_in_bytes",
                Types.LongType.get(),
                "Total size of the Puffin file in bytes"),
            Types.NestedField.required(
                5,
                "referenced_blob_count",
                Types.IntegerType.get(),
                "Number of referenced Puffin blobs represented by this row"),
            Types.NestedField.required(
                6,
                "referenced_blob_types",
                Types.ListType.ofRequired(7, Types.StringType.get()),
                "Distinct blob types across the referenced blobs"),
            Types.NestedField.required(
                8,
                "referenced_fields",
                Types.ListType.ofRequired(
                    9,
                    Types.StructType.of(
                        Types.NestedField.required(
                            10,
                            "field_id",
                            Types.IntegerType.get(),
                            "Field ID referenced by at least one blob"),
                        Types.NestedField.optional(
                            11,
                            "current_field_name",
                            Types.StringType.get(),
                            "Name currently assigned to the field ID; null if no longer present"))),
                "Distinct fields referenced across the blobs"));

    assertThat(metadataTable().schema().sameSchema(expected)).isTrue();
  }

  @TestTemplate
  public void testEmptyTable() throws IOException {
    assertThat(rawTaskRows(metadataTable().newScan())).isEmpty();
  }

  @TestTemplate
  public void testStatisticsPuffinFile() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snapshot = table.currentSnapshot();
    int idFieldId = fieldId("id");
    int dataFieldId = fieldId("data");

    StatisticsFile statisticsFile =
        statisticsFile(
            snapshot,
            "/path/to/statistics.puffin",
            100L,
            blob(snapshot, "test-blob-v1", ImmutableList.of(idFieldId, dataFieldId)));

    table.updateStatistics().setStatistics(statisticsFile).commit();

    assertThat(rawTaskRows(metadataTable().newScan()))
        .containsExactly(
            new PuffinRow(
                snapshot.snapshotId(),
                statisticsFile.path(),
                SOURCE_STATISTICS,
                statisticsFile.fileSizeInBytes(),
                1,
                ImmutableList.of("test-blob-v1"),
                ImmutableList.of(
                    new ReferencedField(idFieldId, "id"),
                    new ReferencedField(dataFieldId, "data"))));
  }

  @TestTemplate
  public void testStatisticsBlobAggregation() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snapshot = table.currentSnapshot();
    int idFieldId = fieldId("id");
    int dataFieldId = fieldId("data");

    StatisticsFile statisticsFile =
        statisticsFile(
            snapshot,
            "/path/to/aggregated-statistics.puffin",
            200L,
            blob(snapshot, "type-a", ImmutableList.of(idFieldId, dataFieldId)),
            blob(snapshot, "type-a", ImmutableList.of(dataFieldId)),
            blob(snapshot, "type-b", ImmutableList.of(idFieldId)));

    table.updateStatistics().setStatistics(statisticsFile).commit();

    PuffinRow row = onlyRow(metadataTable().newScan());
    assertThat(row.referencedBlobCount()).isEqualTo(3);
    assertThat(row.referencedBlobTypes()).containsExactly("type-a", "type-b");
    assertThat(row.referencedFields())
        .containsExactly(
            new ReferencedField(idFieldId, "id"), new ReferencedField(dataFieldId, "data"));
  }

  @TestTemplate
  public void testStatisticsReferenceSnapshotComesFromStatisticsFile() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot blobSnapshot = table.currentSnapshot();

    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot referenceSnapshot = table.currentSnapshot();

    StatisticsFile statisticsFile =
        statisticsFile(
            referenceSnapshot,
            "/path/to/cross-snapshot-statistics.puffin",
            100L,
            blob(blobSnapshot, "test-type", ImmutableList.of(fieldId("id"))));

    table.updateStatistics().setStatistics(statisticsFile).commit();

    PuffinRow row = onlyRow(metadataTable().newScan());
    assertThat(row.referenceSnapshotId()).isEqualTo(referenceSnapshot.snapshotId());
  }

  @TestTemplate
  public void testStatisticsFileReplacementAndRemoval() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snapshot = table.currentSnapshot();
    int idFieldId = fieldId("id");
    int dataFieldId = fieldId("data");

    StatisticsFile oldStatistics =
        statisticsFile(
            snapshot,
            "/path/to/old-statistics.puffin",
            100L,
            blob(snapshot, "old-type", ImmutableList.of(idFieldId)));

    StatisticsFile newStatistics =
        statisticsFile(
            snapshot,
            "/path/to/new-statistics.puffin",
            200L,
            blob(snapshot, "new-type", ImmutableList.of(dataFieldId)));

    table.updateStatistics().setStatistics(oldStatistics).commit();
    table.updateStatistics().setStatistics(newStatistics).commit();

    assertThat(rawTaskRows(metadataTable().newScan()))
        .containsExactly(
            new PuffinRow(
                snapshot.snapshotId(),
                newStatistics.path(),
                SOURCE_STATISTICS,
                newStatistics.fileSizeInBytes(),
                1,
                ImmutableList.of("new-type"),
                ImmutableList.of(new ReferencedField(dataFieldId, "data"))));

    table.updateStatistics().removeStatistics(snapshot.snapshotId()).commit();

    assertThat(rawTaskRows(metadataTable().newScan())).isEmpty();
  }

  @TestTemplate
  public void testCurrentFieldNameAfterRename() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snapshot = table.currentSnapshot();
    int dataFieldId = fieldId("data");

    table
        .updateStatistics()
        .setStatistics(
            statisticsFile(
                snapshot,
                "/path/to/rename-statistics.puffin",
                100L,
                blob(snapshot, "test-type", ImmutableList.of(dataFieldId))))
        .commit();

    table.updateSchema().renameColumn("data", "renamed_data").commit();

    assertThat(table.schema().findColumnName(dataFieldId)).isEqualTo("renamed_data");

    PuffinRow row = onlyRow(metadataTable().newScan());
    assertThat(row.referencedFields())
        .containsExactly(new ReferencedField(dataFieldId, "renamed_data"));
  }

  @TestTemplate
  public void testCurrentFieldNameAfterDelete() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snapshot = table.currentSnapshot();
    int idFieldId = fieldId("id");

    table
        .updateStatistics()
        .setStatistics(
            statisticsFile(
                snapshot,
                "/path/to/deleted-field-statistics.puffin",
                100L,
                blob(snapshot, "test-type", ImmutableList.of(idFieldId))))
        .commit();

    table.updateSchema().deleteColumn("id").commit();

    assertThat(table.schema().findColumnName(idFieldId)).isNull();

    PuffinRow row = onlyRow(metadataTable().newScan());
    assertThat(row.referencedFields()).containsExactly(new ReferencedField(idFieldId, null));
  }

  @TestTemplate
  public void testDeletionVectorPuffinFile() throws IOException {
    assumeDeletionVectorsSupported();

    table.newFastAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(FILE_A_DV).commit();
    Snapshot deletionVectorSnapshot = table.currentSnapshot();

    assertThat(rawTaskRows(metadataTable().newScan()))
        .containsExactly(
            new PuffinRow(
                deletionVectorSnapshot.snapshotId(),
                FILE_A_DV.location(),
                SOURCE_DELETION_VECTOR,
                FILE_A_DV.fileSizeInBytes(),
                1,
                ImmutableList.of(StandardBlobTypes.DV_V1),
                ImmutableList.of(
                    new ReferencedField(
                        MetadataColumns.ROW_POSITION.fieldId(),
                        MetadataColumns.ROW_POSITION.name()))));
  }

  @TestTemplate
  public void testMultipleDeletionVectorsInSamePuffinFile() throws IOException {
    assumeDeletionVectorsSupported();

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    String sharedPuffinPath = "/path/to/shared-deletion-vectors.puffin";
    DeleteFile dvA = deletionVector(FILE_A, sharedPuffinPath, 100L, 4L, 10L);
    DeleteFile dvB = deletionVector(FILE_B, sharedPuffinPath, 100L, 20L, 12L);

    table.newRowDelta().addDeletes(dvA).addDeletes(dvB).commit();
    Snapshot deletionVectorSnapshot = table.currentSnapshot();

    assertThat(rawTaskRows(metadataTable().newScan()))
        .containsExactly(
            new PuffinRow(
                deletionVectorSnapshot.snapshotId(),
                sharedPuffinPath,
                SOURCE_DELETION_VECTOR,
                100L,
                2,
                ImmutableList.of(StandardBlobTypes.DV_V1),
                ImmutableList.of(
                    new ReferencedField(
                        MetadataColumns.ROW_POSITION.fieldId(),
                        MetadataColumns.ROW_POSITION.name()))));
  }

  @TestTemplate
  public void testDuplicateDeletionVectorBlobIsCountedOnce() throws IOException {
    assumeDeletionVectorsSupported();

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    String sharedPuffinPath = "/path/to/duplicate-deletion-vector.puffin";
    DeleteFile dvA = deletionVector(FILE_A, sharedPuffinPath, 100L, 4L, 10L);
    DeleteFile dvB = deletionVector(FILE_B, sharedPuffinPath, 100L, 4L, 10L);

    table.newRowDelta().addDeletes(dvA).addDeletes(dvB).commit();

    PuffinRow row = onlyRow(metadataTable().newScan());
    assertThat(row.referencedBlobCount()).isEqualTo(1);
  }

  @TestTemplate
  public void testDeletionVectorsInSamePuffinFileMustHaveConsistentFileSize() {
    assumeDeletionVectorsSupported();

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    String sharedPuffinPath = "/path/to/inconsistent-deletion-vectors.puffin";
    DeleteFile dvA = deletionVector(FILE_A, sharedPuffinPath, 100L, 4L, 10L);
    DeleteFile dvB = deletionVector(FILE_B, sharedPuffinPath, 200L, 20L, 12L);

    table.newRowDelta().addDeletes(dvA).addDeletes(dvB).commit();

    assertThatThrownBy(
            () ->
                rawTaskRows(
                    metadataTable()
                        .newScan()
                        .filter(Expressions.equal("source", SOURCE_DELETION_VECTOR))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("different file sizes")
        .hasMessageContaining(sharedPuffinPath);
  }

  @TestTemplate
  public void testEqualityDeletesAreNotPuffinFiles() throws IOException {
    assumeDeletionVectorsSupported();

    table.newFastAppend().appendFile(FILE_A).commit();

    DeleteFile equalityDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(fieldId("id"))
            .withPath("/path/to/equality-deletes.parquet")
            .withFileSizeInBytes(10L)
            .withPartition(FILE_A.partition())
            .withRecordCount(1L)
            .build();

    table.newRowDelta().addDeletes(equalityDelete).commit();

    assertThat(
            rawTaskRows(
                metadataTable()
                    .newScan()
                    .filter(Expressions.equal("source", SOURCE_DELETION_VECTOR))))
        .isEmpty();
  }

  @TestTemplate
  public void testDeletionVectorRepeatedAcrossSnapshots() throws IOException {
    assumeDeletionVectorsSupported();

    table.newFastAppend().appendFile(FILE_A).commit();

    table.newRowDelta().addDeletes(FILE_A_DV).commit();
    long deletionVectorSnapshotId = table.currentSnapshot().snapshotId();

    table.newFastAppend().appendFile(FILE_B).commit();
    long laterSnapshotId = table.currentSnapshot().snapshotId();

    List<PuffinRow> deletionVectorRows = deletionVectorRows();

    assertThat(deletionVectorRows).hasSize(2);
    assertThat(deletionVectorRows)
        .extracting(PuffinRow::referenceSnapshotId)
        .containsExactlyInAnyOrder(deletionVectorSnapshotId, laterSnapshotId);
    assertThat(deletionVectorRows)
        .extracting(PuffinRow::filePath)
        .containsOnly(FILE_A_DV.location());
  }

  @TestTemplate
  public void testDeletionVectorRemovedFromLaterSnapshot() throws IOException {
    assumeDeletionVectorsSupported();

    table.newFastAppend().appendFile(FILE_A).commit();

    table.newRowDelta().addDeletes(FILE_A_DV).commit();
    long addedSnapshotId = table.currentSnapshot().snapshotId();

    table.newFastAppend().appendFile(FILE_B).commit();
    long carriedSnapshotId = table.currentSnapshot().snapshotId();

    table.newRowDelta().removeDeletes(FILE_A_DV).commit();
    long removedSnapshotId = table.currentSnapshot().snapshotId();

    assertThat(deletionVectorRows())
        .extracting(PuffinRow::referenceSnapshotId)
        .containsExactlyInAnyOrder(addedSnapshotId, carriedSnapshotId)
        .doesNotContain(removedSnapshotId);
  }

  @TestTemplate
  public void testExpiredSnapshotIsNotReported() throws IOException {
    assumeDeletionVectorsSupported();

    table.newFastAppend().appendFile(FILE_A).commit();

    table.newRowDelta().addDeletes(FILE_A_DV).commit();
    long expiredSnapshotId = table.currentSnapshot().snapshotId();

    table.newFastAppend().appendFile(FILE_B).commit();
    long currentSnapshotId = table.currentSnapshot().snapshotId();

    table
        .expireSnapshots()
        .expireSnapshotId(expiredSnapshotId)
        .cleanupLevel(ExpireSnapshots.CleanupLevel.NONE)
        .commit();

    assertThat(deletionVectorRows())
        .extracting(PuffinRow::referenceSnapshotId)
        .containsExactly(currentSnapshotId);
  }

  @TestTemplate
  public void testSourcePredicatePruning() throws IOException {
    assumeDeletionVectorsSupported();
    PreparedTable prepared = prepareStatisticsAndDeletionVector();

    assertPlannedTaskFormats(Expressions.equal("source", SOURCE_STATISTICS), FileFormat.METADATA);
    assertPlannedTaskFormats(Expressions.notEqual("source", SOURCE_STATISTICS), FileFormat.AVRO);
    assertPlannedTaskFormats(Expressions.in("source", SOURCE_STATISTICS), FileFormat.METADATA);
    assertPlannedTaskFormats(Expressions.notIn("source", SOURCE_STATISTICS), FileFormat.AVRO);
    assertPlannedTaskFormats(Expressions.startsWith("source", "stat"), FileFormat.METADATA);
    assertPlannedTaskFormats(Expressions.notStartsWith("source", "stat"), FileFormat.AVRO);
    assertPlannedTaskFormats(Expressions.isNull("source"));
    assertPlannedTaskFormats(Expressions.notNull("source"), FileFormat.METADATA, FileFormat.AVRO);

    FileScanTask deletionVectorTask =
        onlyTaskWithFormat(
            plannedTasks(
                metadataTable()
                    .newScan()
                    .filter(Expressions.equal("source", SOURCE_DELETION_VECTOR))),
            FileFormat.AVRO);
    assertThat(deletionVectorTask.file().location())
        .isEqualTo(prepared.deletionVectorManifestList());
  }

  @TestTemplate
  public void testSourcePruningUsesCaseInsensitiveBinding() throws IOException {
    assumeDeletionVectorsSupported();
    prepareStatisticsAndDeletionVector();

    List<FileScanTask> tasks =
        plannedTasks(
            metadataTable()
                .newScan()
                .caseSensitive(false)
                .filter(Expressions.equal("SOURCE", SOURCE_STATISTICS)));

    assertThat(tasks).extracting(task -> task.file().format()).containsExactly(FileFormat.METADATA);
  }

  @TestTemplate
  public void testReferenceSnapshotIdPredicatePruning() throws IOException {
    assumeDeletionVectorsSupported();
    PreparedTable prepared = prepareStatisticsAndDeletionVector();

    assertPlannedTaskFormats(
        Expressions.equal("reference_snapshot_id", prepared.statisticsSnapshotId()),
        FileFormat.METADATA);
    assertPlannedTaskFormats(
        Expressions.notEqual("reference_snapshot_id", prepared.statisticsSnapshotId()),
        FileFormat.AVRO);
    assertPlannedTaskFormats(
        Expressions.in("reference_snapshot_id", prepared.statisticsSnapshotId()),
        FileFormat.METADATA);
    assertPlannedTaskFormats(
        Expressions.notIn("reference_snapshot_id", prepared.statisticsSnapshotId()),
        FileFormat.AVRO);
    assertPlannedTaskFormats(Expressions.isNull("reference_snapshot_id"));
    assertPlannedTaskFormats(
        Expressions.notNull("reference_snapshot_id"), FileFormat.METADATA, FileFormat.AVRO);
  }

  @TestTemplate
  public void testReferenceSnapshotIdComparisonPruning() throws IOException {
    assumeDeletionVectorsSupported();
    PreparedTable prepared = prepareStatisticsAndDeletionVector();

    long lowerSnapshotId =
        Math.min(prepared.statisticsSnapshotId(), prepared.deletionVectorSnapshotId());
    long higherSnapshotId =
        Math.max(prepared.statisticsSnapshotId(), prepared.deletionVectorSnapshotId());

    assertPlannedTaskFormats(
        Expressions.lessThan("reference_snapshot_id", higherSnapshotId),
        formatForSnapshot(prepared, lowerSnapshotId));
    assertPlannedTaskFormats(
        Expressions.greaterThanOrEqual("reference_snapshot_id", higherSnapshotId),
        formatForSnapshot(prepared, higherSnapshotId));
    assertPlannedTaskFormats(
        Expressions.lessThanOrEqual("reference_snapshot_id", lowerSnapshotId),
        formatForSnapshot(prepared, lowerSnapshotId));
    assertPlannedTaskFormats(
        Expressions.greaterThan("reference_snapshot_id", lowerSnapshotId),
        formatForSnapshot(prepared, higherSnapshotId));
  }

  @TestTemplate
  public void testBooleanPredicatePruning() throws IOException {
    assumeDeletionVectorsSupported();
    PreparedTable prepared = prepareStatisticsAndDeletionVector();

    Expression unknown = Expressions.equal("file_path", "/unknown/path.puffin");

    assertPlannedTaskFormats(
        Expressions.and(unknown, Expressions.equal("source", SOURCE_STATISTICS)),
        FileFormat.METADATA);
    assertPlannedTaskFormats(Expressions.and(unknown, Expressions.equal("source", "unknown")));
    assertPlannedTaskFormats(
        Expressions.or(unknown, Expressions.equal("source", SOURCE_STATISTICS)),
        FileFormat.METADATA,
        FileFormat.AVRO);
    assertPlannedTaskFormats(Expressions.not(unknown), FileFormat.METADATA, FileFormat.AVRO);
    assertPlannedTaskFormats(
        Expressions.not(Expressions.equal("source", SOURCE_STATISTICS)), FileFormat.AVRO);
    assertPlannedTaskFormats(
        Expressions.and(
            Expressions.equal("source", SOURCE_STATISTICS),
            Expressions.equal("reference_snapshot_id", prepared.statisticsSnapshotId())),
        FileFormat.METADATA);
    assertPlannedTaskFormats(
        Expressions.or(
            Expressions.equal("source", SOURCE_STATISTICS),
            Expressions.equal("reference_snapshot_id", prepared.deletionVectorSnapshotId())),
        FileFormat.METADATA,
        FileFormat.AVRO);
  }

  @TestTemplate
  public void testDeletionVectorTaskPreservesUnknownPredicateAsResidual() throws IOException {
    assumeDeletionVectorsSupported();
    prepareStatisticsAndDeletionVector();

    Expression filter = Expressions.greaterThan("referenced_blob_count", 100);
    List<FileScanTask> tasks = plannedTasks(metadataTable().newScan().filter(filter));
    FileScanTask deletionVectorTask = onlyTaskWithFormat(tasks, FileFormat.AVRO);

    assertThat(
            ExpressionUtil.equivalent(
                filter, deletionVectorTask.residual(), metadataTable().schema().asStruct(), true))
        .isTrue();
  }

  @TestTemplate
  public void testProjection() throws IOException {
    assumeDeletionVectorsSupported();
    PreparedTable prepared = prepareStatisticsAndDeletionVector();

    Table metadataTable = metadataTable();
    TableScan scan = metadataTable.newScan().select("reference_snapshot_id", "source");
    Schema expectedSchema = metadataTable.schema().select("reference_snapshot_id", "source");

    assertThat(scan.schema().sameSchema(expectedSchema))
        .as("Projected scan schema should match the selected metadata-table schema")
        .isTrue();

    List<SnapshotSourceRow> projectedRows = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        try (CloseableIterable<StructLike> taskRows = task.asDataTask().rows()) {
          for (StructLike row : taskRows) {
            assertThat(row.size()).isEqualTo(2);
            projectedRows.add(
                new SnapshotSourceRow(
                    row.get(0, Long.class), row.get(1, CharSequence.class).toString()));
          }
        }
      }
    }

    assertThat(projectedRows)
        .containsExactlyInAnyOrder(
            new SnapshotSourceRow(prepared.statisticsSnapshotId(), SOURCE_STATISTICS),
            new SnapshotSourceRow(prepared.deletionVectorSnapshotId(), SOURCE_DELETION_VECTOR));
  }

  @TestTemplate
  public void testReferencedFieldsProjection() throws IOException {
    assumeDeletionVectorsSupported();
    prepareStatisticsAndDeletionVector();

    Table metadataTable = metadataTable();
    TableScan scan = metadataTable.newScan().select("referenced_fields");
    Schema expectedSchema = metadataTable.schema().select("referenced_fields");

    assertThat(scan.schema().sameSchema(expectedSchema)).isTrue();

    Set<ReferencedField> referencedFields = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        try (CloseableIterable<StructLike> taskRows = task.asDataTask().rows()) {
          for (StructLike row : taskRows) {
            assertThat(row.size()).isEqualTo(1);

            List<?> fields = row.get(0, List.class);
            for (Object fieldObject : fields) {
              StructLike field = (StructLike) fieldObject;
              assertThat(field.size()).isEqualTo(2);

              CharSequence currentFieldName = field.get(1, CharSequence.class);
              referencedFields.add(
                  new ReferencedField(
                      field.get(0, Integer.class),
                      currentFieldName != null ? currentFieldName.toString() : null));
            }
          }
        }
      }
    }

    assertThat(referencedFields)
        .containsExactlyInAnyOrder(
            new ReferencedField(fieldId("id"), "id"),
            new ReferencedField(
                MetadataColumns.ROW_POSITION.fieldId(), MetadataColumns.ROW_POSITION.name()));
  }

  @TestTemplate
  public void testDeletionVectorTaskHonorsIgnoreResiduals() throws IOException {
    assumeDeletionVectorsSupported();

    table.newFastAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(FILE_A_DV).commit();

    Expression filter = Expressions.greaterThan("referenced_blob_count", 0);

    FileScanTask task = onlyTask(plannedTasks(metadataTable().newScan().filter(filter)));
    assertThat(
            ExpressionUtil.equivalent(
                filter, task.residual(), metadataTable().schema().asStruct(), true))
        .isTrue();

    FileScanTask taskWithoutResidual =
        onlyTask(plannedTasks(metadataTable().newScan().filter(filter).ignoreResiduals()));

    assertThat(taskWithoutResidual.residual()).isEqualTo(Expressions.alwaysTrue());
  }

  private FileScanTask onlyTask(List<FileScanTask> tasks) {
    assertThat(tasks).hasSize(1);
    return tasks.get(0);
  }

  private Table metadataTable() {
    return new AllPuffinFilesTable(table);
  }

  private int fieldId(String name) {
    Types.NestedField field = table.schema().findField(name);
    assertThat(field).as("Field %s should exist in the current table schema", name).isNotNull();
    return field.fieldId();
  }

  private void assumeDeletionVectorsSupported() {
    assumeThat(formatVersion)
        .as("Deletion vectors require format version 3 or later")
        .isGreaterThanOrEqualTo(3);
  }

  private PreparedTable prepareStatisticsAndDeletionVector() {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot statisticsSnapshot = table.currentSnapshot();

    table
        .updateStatistics()
        .setStatistics(
            statisticsFile(
                statisticsSnapshot,
                "/path/to/source-pruning-statistics.puffin",
                100L,
                blob(statisticsSnapshot, "test-type", ImmutableList.of(fieldId("id")))))
        .commit();

    table.newRowDelta().addDeletes(FILE_A_DV).commit();
    Snapshot deletionVectorSnapshot = table.currentSnapshot();

    return new PreparedTable(
        statisticsSnapshot.snapshotId(),
        deletionVectorSnapshot.snapshotId(),
        deletionVectorSnapshot.manifestListLocation());
  }

  private List<PuffinRow> deletionVectorRows() throws IOException {
    return rawTaskRows(
        metadataTable().newScan().filter(Expressions.equal("source", SOURCE_DELETION_VECTOR)));
  }

  private void assertPlannedTaskFormats(Expression filter, FileFormat... expectedFormats)
      throws IOException {
    assertThat(plannedTasks(metadataTable().newScan().filter(filter)))
        .extracting(task -> task.file().format())
        .containsExactlyInAnyOrder(expectedFormats);
  }

  private FileFormat formatForSnapshot(PreparedTable prepared, long snapshotId) {
    if (snapshotId == prepared.statisticsSnapshotId()) {
      return FileFormat.METADATA;
    }

    assertThat(snapshotId).isEqualTo(prepared.deletionVectorSnapshotId());
    return FileFormat.AVRO;
  }

  private FileScanTask onlyTaskWithFormat(List<FileScanTask> tasks, FileFormat format) {
    List<FileScanTask> matchingTasks =
        tasks.stream()
            .filter(task -> task.file().format() == format)
            .collect(ImmutableList.toImmutableList());

    assertThat(matchingTasks).hasSize(1);
    return matchingTasks.get(0);
  }

  private static StatisticsFile statisticsFile(
      Snapshot snapshot, String path, long fileSizeInBytes, BlobMetadata... blobs) {
    return new GenericStatisticsFile(
        snapshot.snapshotId(), path, fileSizeInBytes, 10L, ImmutableList.copyOf(blobs));
  }

  private static BlobMetadata blob(Snapshot snapshot, String type, List<Integer> fields) {
    return new GenericBlobMetadata(
        type, snapshot.snapshotId(), snapshot.sequenceNumber(), fields, ImmutableMap.of());
  }

  private static DeleteFile deletionVector(
      DataFile referencedDataFile,
      String puffinPath,
      long fileSizeInBytes,
      long contentOffset,
      long contentSizeInBytes) {
    return FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath(puffinPath)
        .withFileSizeInBytes(fileSizeInBytes)
        .withPartition(referencedDataFile.partition())
        .withRecordCount(1L)
        .withReferencedDataFile(referencedDataFile.location())
        .withContentOffset(contentOffset)
        .withContentSizeInBytes(contentSizeInBytes)
        .build();
  }

  private PuffinRow onlyRow(TableScan scan) throws IOException {
    List<PuffinRow> rows = rawTaskRows(scan);
    assertThat(rows).hasSize(1);
    return rows.get(0);
  }

  private List<FileScanTask> plannedTasks(TableScan scan) throws IOException {
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      return Lists.newArrayList(tasks);
    }
  }

  /** Returns raw data-task rows without evaluating task residuals. */
  private List<PuffinRow> rawTaskRows(TableScan scan) throws IOException {
    List<PuffinRow> results = Lists.newArrayList();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        try (CloseableIterable<StructLike> taskRows = task.asDataTask().rows()) {
          for (StructLike row : taskRows) {
            results.add(copyRow(row));
          }
        }
      }
    }

    return results;
  }

  private static PuffinRow copyRow(StructLike row) {
    CharSequence filePath = row.get(1, CharSequence.class);
    CharSequence source = row.get(2, CharSequence.class);

    List<?> rawBlobTypes = row.get(5, List.class);
    List<String> blobTypes =
        rawBlobTypes.stream().map(Object::toString).collect(ImmutableList.toImmutableList());

    List<?> rawReferencedFields = row.get(6, List.class);
    List<ReferencedField> referencedFields =
        rawReferencedFields.stream()
            .map(StructLike.class::cast)
            .map(
                field -> {
                  CharSequence currentName = field.get(1, CharSequence.class);
                  return new ReferencedField(
                      field.get(0, Integer.class),
                      currentName != null ? currentName.toString() : null);
                })
            .collect(ImmutableList.toImmutableList());

    return new PuffinRow(
        row.get(0, Long.class),
        filePath.toString(),
        source.toString(),
        row.get(3, Long.class),
        row.get(4, Integer.class),
        blobTypes,
        referencedFields);
  }

  private record PreparedTable(
      long statisticsSnapshotId,
      long deletionVectorSnapshotId,
      String deletionVectorManifestList) {}

  private record SnapshotSourceRow(long referenceSnapshotId, String source) {}

  private record PuffinRow(
      long referenceSnapshotId,
      String filePath,
      String source,
      long fileSizeInBytes,
      int referencedBlobCount,
      List<String> referencedBlobTypes,
      List<ReferencedField> referencedFields) {}

  private record ReferencedField(int fieldId, String currentFieldName) {}
}
