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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.UnaryOperator;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

class TestScanTaskPlanner {
  private static final long SNAPSHOT_ID = 42L;
  private static final int WRITER_FORMAT_VERSION = 4;
  private static final long RECORD_COUNT = 100L;
  private static final long FILE_SIZE_IN_BYTES = 1024L;
  private static final String TABLE_LOCATION = "s3://bucket/db/table";
  private static final String DV_LOCATION = "s3://bucket/db/table/dv.puffin";
  private static final long DV_OFFSET = 100L;
  private static final long DV_SIZE_IN_BYTES = 50L;
  private static final long DV_CARDINALITY = 5L;

  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA).identity("id").build();
  private static final Types.StructType PARTITION_TYPE = SPEC.partitionType();
  private static final Types.StructType EMPTY_PARTITION = Types.StructType.of();
  private static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_PARTITION);
  private static final Map<Integer, PartitionSpec> PARTITIONED_SPECS =
      ImmutableMap.of(SPEC.specId(), SPEC);
  private static final Map<Integer, PartitionSpec> UNPARTITIONED_SPECS =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());

  private static final List<FileFormat> MANIFEST_FORMATS =
      ImmutableList.of(FileFormat.AVRO, FileFormat.PARQUET);

  private final InMemoryFileIO fileIO = new InMemoryFileIO();

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void rootWithDirectDataEntries(FileFormat format) throws IOException {
    InputFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFile("a.parquet", EMPTY_PARTITION_DATA),
                dataFile("b.parquet", EMPTY_PARTITION_DATA)));

    List<FileScanTask> tasks = plan(root, UNPARTITIONED_SPECS);

    assertThat(tasks)
        .hasSize(2)
        .extracting(task -> task.file().location())
        .containsExactlyInAnyOrder(resolved("a.parquet"), resolved("b.parquet"));
    assertThat(tasks).allSatisfy(task -> assertThat(task.deletes()).isEmpty());
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void rootWithDataManifestExpandsLeaf(FileFormat format) throws IOException {
    InputFile leaf =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFile("leaf-a.parquet", EMPTY_PARTITION_DATA),
                dataFile("leaf-b.parquet", EMPTY_PARTITION_DATA)));
    InputFile root =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(dataManifest(leaf.location())));

    List<FileScanTask> tasks = plan(root, UNPARTITIONED_SPECS);

    assertThat(tasks)
        .hasSize(2)
        .extracting(task -> task.file().location())
        .containsExactlyInAnyOrder(resolved("leaf-a.parquet"), resolved("leaf-b.parquet"));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void mixedRootDataAndLeafManifest(FileFormat format) throws IOException {
    InputFile leaf =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataFile("leaf-data-file.parquet", EMPTY_PARTITION_DATA)));
    InputFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFile("root-data-file.parquet", EMPTY_PARTITION_DATA),
                dataManifest(leaf.location())));

    List<FileScanTask> tasks = plan(root, UNPARTITIONED_SPECS);

    assertThat(tasks)
        .hasSize(2)
        .extracting(task -> task.file().location())
        .containsExactlyInAnyOrder(
            resolved("root-data-file.parquet"), resolved("leaf-data-file.parquet"));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void deletionVectorAttachedToTask(FileFormat format) throws IOException {
    TrackedFile fileWithDv =
        dataFile(
            "with-dv.parquet",
            EMPTY_PARTITION_DATA,
            deletionVector(DV_LOCATION, DV_OFFSET, DV_SIZE_IN_BYTES, DV_CARDINALITY));
    InputFile root = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(fileWithDv));

    List<FileScanTask> tasks = plan(root, UNPARTITIONED_SPECS);

    assertThat(tasks).hasSize(1);
    assertThat(tasks.get(0).deletes())
        .hasSize(1)
        .allSatisfy(
            delete -> {
              assertThat(delete.content()).isEqualTo(FileContent.POSITION_DELETES);
              assertThat(delete.referencedDataFile()).isEqualTo(resolved("with-dv.parquet"));
              assertThat(delete.recordCount()).isEqualTo(DV_CARDINALITY);
              assertThat(delete.contentOffset()).isEqualTo(DV_OFFSET);
              assertThat(delete.contentSizeInBytes()).isEqualTo(DV_SIZE_IN_BYTES);
            });
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void onlyLiveEntriesArePlanned(FileFormat format) throws IOException {
    // EXISTING/ADDED/MODIFIED are live; DELETED/REPLACED are not
    InputFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFileWithStatus(EntryStatus.ADDED, "added.parquet"),
                dataFileWithStatus(EntryStatus.EXISTING, "existing.parquet"),
                dataFileWithStatus(EntryStatus.MODIFIED, "modified.parquet"),
                dataFileWithStatus(EntryStatus.DELETED, "deleted.parquet"),
                dataFileWithStatus(EntryStatus.REPLACED, "replaced.parquet")));

    List<FileScanTask> tasks = plan(root, UNPARTITIONED_SPECS);

    assertThat(tasks)
        .extracting(task -> task.file().location())
        .containsExactlyInAnyOrder(
            resolved("added.parquet"), resolved("existing.parquet"), resolved("modified.parquet"));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void partitionFilterPrunesFiles(FileFormat format) throws IOException {
    InputFile root =
        writeManifest(
            format,
            PARTITION_TYPE,
            ImmutableList.of(
                dataFile("keep.parquet", partition(1)), dataFile("prune.parquet", partition(2))));

    List<FileScanTask> tasks =
        plan(root, PARTITIONED_SPECS, expander -> expander.filterData(Expressions.equal("id", 1)));

    assertThat(tasks)
        .hasSize(1)
        .extracting(task -> task.file().location())
        .containsExactly(resolved("keep.parquet"));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void filterDataAccumulatesWithAnd(FileFormat format) throws IOException {
    InputFile root =
        writeManifest(
            format,
            PARTITION_TYPE,
            ImmutableList.of(
                dataFile("id1.parquet", partition(1)),
                dataFile("id2.parquet", partition(2)),
                dataFile("id3.parquet", partition(3))));

    // two filters AND together: id >= 2 AND id <= 2 keeps only id2. A last-filter-wins bug would
    // apply just id <= 2 and also keep id1; dropping the second filter would also keep id3.
    List<FileScanTask> tasks =
        plan(
            root,
            PARTITIONED_SPECS,
            expander ->
                expander
                    .filterData(Expressions.greaterThanOrEqual("id", 2))
                    .filterData(Expressions.lessThanOrEqual("id", 2)));

    assertThat(tasks)
        .extracting(task -> task.file().location())
        .containsExactly(resolved("id2.parquet"));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void residualAttachedFromFilter(FileFormat format) throws IOException {
    InputFile root =
        writeManifest(
            format, PARTITION_TYPE, ImmutableList.of(dataFile("keep.parquet", partition(1))));

    List<FileScanTask> withResidual =
        plan(
            root,
            PARTITIONED_SPECS,
            expander -> expander.filterData(Expressions.equal("data", "x")));
    // the identity partition on id leaves the data predicate as a residual
    assertThat(withResidual.get(0).residual())
        .hasToString(Expressions.equal("data", "x").toString());

    List<FileScanTask> ignored =
        plan(
            root,
            PARTITIONED_SPECS,
            expander -> expander.filterData(Expressions.equal("data", "x")).ignoreResiduals());
    assertThat(ignored.get(0).residual()).isEqualTo(Expressions.alwaysTrue());
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void sameSpecFilesShareContext(FileFormat format) throws IOException {
    InputFile root =
        writeManifest(
            format,
            PARTITION_TYPE,
            ImmutableList.of(
                dataFile("a.parquet", partition(1)), dataFile("b.parquet", partition(1))));

    List<FileScanTask> tasks =
        plan(
            root,
            PARTITIONED_SPECS,
            expander -> expander.filterData(Expressions.equal("data", "x")));

    // both files share spec 0, so both tasks must carry the same schema, spec, and residual
    assertThat(tasks)
        .hasSize(2)
        .allSatisfy(
            task -> {
              assertThat(task.schema().asStruct()).isEqualTo(TABLE_SCHEMA.asStruct());
              assertThat(task.spec()).isEqualTo(SPEC);
              assertThat(task.residual()).hasToString(Expressions.equal("data", "x").toString());
            });
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void buildingAndClosingWithoutIteratingDoesNotScanLeaves(FileFormat format) throws IOException {
    InputFile leaf =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataFile("leaf.parquet", EMPTY_PARTITION_DATA)));
    InputFile root =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(dataManifest(leaf.location())));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    ScanTaskPlanner planner =
        ScanTaskPlanner.builder(fileIO, root, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .scanMetrics(metrics)
            .build();
    // the root is read eagerly to route its entries; leaf readers open lazily, so closing the plan
    // without iterating scans only the root, not the leaf
    planner.planFiles().close();

    assertThat(metrics.scannedDataManifests().value()).isEqualTo(1L);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void caseInsensitiveFilterPrunesFiles(FileFormat format) throws IOException {
    InputFile root =
        writeManifest(
            format,
            PARTITION_TYPE,
            ImmutableList.of(
                dataFile("keep.parquet", partition(1)), dataFile("prune.parquet", partition(2))));

    List<FileScanTask> tasks =
        plan(
            root,
            PARTITIONED_SPECS,
            expander -> expander.caseSensitive(false).filterData(Expressions.equal("ID", 1)));

    assertThat(tasks)
        .as("a case-insensitive filter resolves the upper-case column and prunes")
        .hasSize(1)
        .extracting(task -> task.file().location())
        .containsExactly(resolved("keep.parquet"));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void parallelPlanningMatchesSequential(FileFormat format) throws IOException {
    // a DV and a residual-bearing filter in the tree so parity actually exercises createTask's DV
    // branch and per-spec residual keying on worker threads, not just locations
    TrackedFile withDv =
        dataFile(
            "leaf1.parquet",
            partition(1),
            deletionVector(DV_LOCATION, DV_OFFSET, DV_SIZE_IN_BYTES, DV_CARDINALITY));
    InputFile leaf1 = writeManifest(format, PARTITION_TYPE, ImmutableList.of(withDv));
    InputFile leaf2 =
        writeManifest(
            format, PARTITION_TYPE, ImmutableList.of(dataFile("leaf2.parquet", partition(1))));
    InputFile root =
        writeManifest(
            format,
            PARTITION_TYPE,
            ImmutableList.of(dataManifest(leaf1.location()), dataManifest(leaf2.location())));

    List<FileScanTask> sequential =
        plan(
            root,
            PARTITIONED_SPECS,
            expander -> expander.filterData(Expressions.equal("data", "x")));

    ExecutorService pool = ThreadPools.newFixedThreadPool("test-scan-task-planner", 2);
    try {
      List<FileScanTask> parallel =
          plan(
              root,
              PARTITIONED_SPECS,
              expander -> expander.filterData(Expressions.equal("data", "x")).planWith(pool));
      assertThat(parallel)
          .extracting(
              task -> task.file().location(),
              task -> task.residual().toString(),
              task -> task.deletes().size())
          .containsExactlyInAnyOrderElementsOf(
              Lists.transform(
                  sequential,
                  task ->
                      tuple(
                          task.file().location(),
                          task.residual().toString(),
                          task.deletes().size())));
    } finally {
      pool.shutdownNow();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void deleteContentInRootIsUnsupported(FileFormat format) throws IOException {
    InputFile root =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(deleteManifest("deletes.avro")));

    // delete content is only produced by upgraded trees; that path is not yet implemented
    ScanTaskPlanner expander =
        ScanTaskPlanner.builder(fileIO, root, UNPARTITIONED_SPECS, TABLE_LOCATION).build();
    assertThatThrownBy(() -> Lists.newArrayList(expander.planFiles()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot plan content type in root manifest: DELETE_MANIFEST");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void deleteContentInLeafIsUnsupported(FileFormat format) throws IOException {
    InputFile leaf =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFile("leaf-data.parquet", EMPTY_PARTITION_DATA),
                deleteManifest("leaf-deletes.avro")));
    InputFile root =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(dataManifest(leaf.location())));

    ScanTaskPlanner expander =
        ScanTaskPlanner.builder(fileIO, root, UNPARTITIONED_SPECS, TABLE_LOCATION).build();
    assertThatThrownBy(() -> Lists.newArrayList(expander.planFiles()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot plan content type in leaf manifest: DELETE_MANIFEST");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void nestedDataManifestInLeafIsRejected(FileFormat format) throws IOException {
    InputFile leaf =
        writeManifest(
            format, EMPTY_PARTITION, ImmutableList.of(dataManifest("nested-leaf.parquet")));
    InputFile root =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(dataManifest(leaf.location())));

    // a nested manifest is structurally impossible in a two-level tree, not merely unsupported
    ScanTaskPlanner expander =
        ScanTaskPlanner.builder(fileIO, root, UNPARTITIONED_SPECS, TABLE_LOCATION).build();
    assertThatThrownBy(() -> Lists.newArrayList(expander.planFiles()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot expand a nested manifest in a leaf manifest: "
                + resolved("nested-leaf.parquet"));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void leafWithManifestDeletionVectorIsUnsupported(FileFormat format) throws IOException {
    InputFile leaf =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataFile("leaf.parquet", EMPTY_PARTITION_DATA)));
    InputFile root =
        writeManifest(
            format, EMPTY_PARTITION, ImmutableList.of(dataManifestWithDv(leaf.location())));

    // applying the manifest-level DV is not built yet; expanding the leaf would resurface deleted
    // entries, so the planner rejects rather than silently corrupting
    ScanTaskPlanner expander =
        ScanTaskPlanner.builder(fileIO, root, UNPARTITIONED_SPECS, TABLE_LOCATION).build();
    assertThatThrownBy(() -> Lists.newArrayList(expander.planFiles()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot apply manifest deletion vector for leaf manifest: " + leaf.location());
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void nonV4LeafManifestIsUnsupported(FileFormat format) throws IOException {
    InputFile leaf =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataFile("leaf.parquet", EMPTY_PARTITION_DATA)));
    // an upgraded tree can reference a v3-format leaf; per-leaf reader dispatch is not built yet
    InputFile root =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(dataManifest(leaf.location(), 3)));

    ScanTaskPlanner expander =
        ScanTaskPlanner.builder(fileIO, root, UNPARTITIONED_SPECS, TABLE_LOCATION).build();
    assertThatThrownBy(() -> Lists.newArrayList(expander.planFiles()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot expand leaf manifest with format version 3: " + leaf.location());
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void encryptedLeafManifestIsUnsupported(FileFormat format) throws IOException {
    InputFile leaf =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataFile("leaf.parquet", EMPTY_PARTITION_DATA)));
    TrackedFile encryptedLeaf =
        encryptedDataManifest(leaf.location(), ByteBuffer.wrap(new byte[] {1, 2, 3}));
    InputFile root = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(encryptedLeaf));

    // reading the leaf as a plain file would hand ciphertext to the reader; the decrypting path
    // is not built yet, so the planner rejects rather than silently corrupting
    ScanTaskPlanner expander =
        ScanTaskPlanner.builder(fileIO, root, UNPARTITIONED_SPECS, TABLE_LOCATION).build();
    assertThatThrownBy(() -> Lists.newArrayList(expander.planFiles()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot read encrypted leaf manifest: " + leaf.location());
  }

  @Test
  void emptyRootYieldsNoTasks() throws IOException {
    // an empty root has no plannable entries regardless of manifest format; AVRO is used because
    // the Parquet writer does not materialize a file when no records are appended
    InputFile root = writeManifest(FileFormat.AVRO, EMPTY_PARTITION, ImmutableList.of());

    assertThat(plan(root, UNPARTITIONED_SPECS)).isEmpty();
  }

  @Test
  void planFilesAcrossMixedManifestFormats() throws IOException {
    // each manifest's format is derived from its own location, so a tree can mix formats; a
    // Parquet root can point at both an Avro and a Parquet leaf
    InputFile avroLeaf =
        writeManifest(
            FileFormat.AVRO,
            EMPTY_PARTITION,
            ImmutableList.of(dataFile("data-from-avro-leaf.parquet", EMPTY_PARTITION_DATA)));
    InputFile parquetLeaf =
        writeManifest(
            FileFormat.PARQUET,
            EMPTY_PARTITION,
            ImmutableList.of(dataFile("data-from-parquet-leaf.parquet", EMPTY_PARTITION_DATA)));
    InputFile root =
        writeManifest(
            FileFormat.PARQUET,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataManifest(avroLeaf.location()), dataManifest(parquetLeaf.location())));

    assertThat(plan(root, UNPARTITIONED_SPECS))
        .extracting(task -> task.file().location())
        .containsExactlyInAnyOrder(
            resolved("data-from-avro-leaf.parquet"), resolved("data-from-parquet-leaf.parquet"));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void planFilesAcrossMultipleSpecs(FileFormat format) throws IOException {
    PartitionSpec spec0 =
        PartitionSpec.builderFor(TABLE_SCHEMA)
            .withSpecId(0)
            .add(1, 1000, "id", Transforms.identity())
            .build();
    PartitionSpec spec1 =
        PartitionSpec.builderFor(TABLE_SCHEMA)
            .withSpecId(1)
            .add(2, 1001, "data", Transforms.identity())
            .build();
    Map<Integer, PartitionSpec> specsById =
        ImmutableMap.of(spec0.specId(), spec0, spec1.specId(), spec1);
    Types.StructType unionType = Partitioning.unionPartitionTypes(specsById.values());

    TrackedFile spec0Keep =
        dataFile("spec0-keep.parquet", spec0.specId(), unionPartition(unionType, 1, null), null);
    TrackedFile spec0Prune =
        dataFile("spec0-prune.parquet", spec0.specId(), unionPartition(unionType, 2, null), null);
    TrackedFile spec1File =
        dataFile("spec1.parquet", spec1.specId(), unionPartition(unionType, null, "x"), null);
    InputFile root =
        writeManifest(format, unionType, ImmutableList.of(spec0Keep, spec0Prune, spec1File));

    // id = 1 prunes the spec0 file partitioned on id=2; spec1 is not partitioned by id, so its
    // residual keeps the predicate. A per-spec residual mis-keying would surface here.
    List<FileScanTask> tasks =
        plan(root, specsById, expander -> expander.filterData(Expressions.equal("id", 1)));

    assertThat(tasks)
        .extracting(
            task -> task.file().location(),
            task -> task.spec().specId(),
            task -> task.residual().toString())
        .containsExactlyInAnyOrder(
            tuple(resolved("spec0-keep.parquet"), 0, Expressions.alwaysTrue().toString()),
            tuple(resolved("spec1.parquet"), 1, Expressions.equal("id", 1).toString()));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void planFilesReportsScanMetrics(FileFormat format) throws IOException {
    TrackedFile fileWithDv =
        dataFile(
            "with-dv.parquet",
            EMPTY_PARTITION_DATA,
            deletionVector(DV_LOCATION, DV_OFFSET, DV_SIZE_IN_BYTES, DV_CARDINALITY));
    InputFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataFile("plain.parquet", EMPTY_PARTITION_DATA), fileWithDv));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    List<FileScanTask> tasks =
        plan(root, UNPARTITIONED_SPECS, expander -> expander.scanMetrics(metrics));

    assertThat(tasks).hasSize(2);
    assertThat(metrics.scannedDataManifests().value())
        .as("the root is scanned; there are no leaves")
        .isEqualTo(1L);
    assertThat(metrics.resultDataFiles().value()).isEqualTo(2L);
    assertThat(metrics.totalFileSizeInBytes().value()).isEqualTo(2 * FILE_SIZE_IN_BYTES);
    assertThat(metrics.resultDeleteFiles().value())
        .as("only the file with a colocated DV contributes a delete file")
        .isEqualTo(1L);
    assertThat(metrics.totalDeleteFileSizeInBytes().value())
        .as("the DV delete contributes its size")
        .isEqualTo(DV_SIZE_IN_BYTES);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void scannedManifestsCountsRootAndLeaves(FileFormat format) throws IOException {
    InputFile leaf1 =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataFile("leaf1.parquet", EMPTY_PARTITION_DATA)));
    InputFile leaf2 =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataFile("leaf2.parquet", EMPTY_PARTITION_DATA)));
    InputFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataManifest(leaf1.location()), dataManifest(leaf2.location())));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    plan(root, UNPARTITIONED_SPECS, expander -> expander.scanMetrics(metrics));

    assertThat(metrics.scannedDataManifests().value())
        .as("the root and both leaves are each counted as a scanned manifest")
        .isEqualTo(3L);
  }

  private List<FileScanTask> plan(InputFile root, Map<Integer, PartitionSpec> specsById)
      throws IOException {
    return plan(root, specsById, UnaryOperator.identity());
  }

  private List<FileScanTask> plan(
      InputFile root,
      Map<Integer, PartitionSpec> specsById,
      UnaryOperator<ScanTaskPlanner.Builder> configure)
      throws IOException {
    ScanTaskPlanner expander =
        configure.apply(ScanTaskPlanner.builder(fileIO, root, specsById, TABLE_LOCATION)).build();
    try (CloseableIterable<FileScanTask> tasks = expander.planFiles()) {
      return Lists.newArrayList(tasks);
    }
  }

  private static TrackedFile dataFile(String location, PartitionData partition) {
    return dataFile(location, specId(partition), partition, null);
  }

  private static TrackedFile dataFile(String location, PartitionData partition, DeletionVector dv) {
    return dataFile(location, specId(partition), partition, dv);
  }

  private static TrackedFile dataFile(
      String location, Integer specId, PartitionData partition, DeletionVector dv) {
    return trackedFile(addedTracking(), FileContent.DATA, location, specId, partition, dv, null);
  }

  private static TrackedFile dataFileWithStatus(EntryStatus status, String location) {
    return trackedFile(
        tracking(status),
        FileContent.DATA,
        location,
        specId(EMPTY_PARTITION_DATA),
        EMPTY_PARTITION_DATA,
        null,
        null);
  }

  private static TrackedFile dataManifest(String location) {
    return trackedFile(
        addedTracking(), FileContent.DATA_MANIFEST, location, null, null, null, null);
  }

  private static TrackedFile dataManifest(String location, int formatVersion) {
    return new TrackedFileStruct(
        /* tracking= */ addedTracking(),
        /* contentType= */ FileContent.DATA_MANIFEST,
        /* formatVersion= */ formatVersion,
        /* location= */ location,
        /* fileFormat= */ FileFormat.PARQUET,
        /* recordCount= */ RECORD_COUNT,
        /* fileSizeInBytes= */ FILE_SIZE_IN_BYTES,
        /* specId= */ null,
        /* partition= */ null,
        /* contentStats= */ null,
        /* sortOrderId= */ null,
        /* deletionVector= */ null,
        /* manifestInfo= */ null,
        /* keyMetadata= */ null,
        /* splitOffsets= */ null,
        /* equalityIds= */ null);
  }

  private static TrackedFile dataManifestWithDv(String location) {
    ManifestInfo manifestInfo =
        ManifestInfoStruct.builder()
            .addedFilesCount(1)
            .existingFilesCount(0)
            .deletedFilesCount(0)
            .replacedFilesCount(0)
            .addedRowsCount(RECORD_COUNT)
            .existingRowsCount(0)
            .deletedRowsCount(0)
            .replacedRowsCount(0)
            .minSequenceNumber(0L)
            .dv(ByteBuffer.wrap(new byte[] {1, 2, 3}))
            .dvCardinality(1L)
            .build();
    return trackedFile(
        addedTracking(), FileContent.DATA_MANIFEST, location, null, null, null, manifestInfo);
  }

  private static TrackedFile encryptedDataManifest(String location, ByteBuffer keyMetadata) {
    return new TrackedFileStruct(
        /* tracking= */ addedTracking(),
        /* contentType= */ FileContent.DATA_MANIFEST,
        /* formatVersion= */ WRITER_FORMAT_VERSION,
        /* location= */ location,
        /* fileFormat= */ FileFormat.PARQUET,
        /* recordCount= */ RECORD_COUNT,
        /* fileSizeInBytes= */ FILE_SIZE_IN_BYTES,
        /* specId= */ null,
        /* partition= */ null,
        /* contentStats= */ null,
        /* sortOrderId= */ null,
        /* deletionVector= */ null,
        /* manifestInfo= */ null,
        /* keyMetadata= */ keyMetadata,
        /* splitOffsets= */ null,
        /* equalityIds= */ null);
  }

  private static TrackedFile deleteManifest(String location) {
    return trackedFile(
        addedTracking(), FileContent.DELETE_MANIFEST, location, null, null, null, null);
  }

  private static TrackedFile trackedFile(
      TrackingStruct tracking,
      FileContent contentType,
      String location,
      Integer specId,
      PartitionData partition,
      DeletionVector dv,
      ManifestInfo manifestInfo) {
    return new TrackedFileStruct(
        /* tracking= */ tracking,
        /* contentType= */ contentType,
        /* formatVersion= */ WRITER_FORMAT_VERSION,
        /* location= */ location,
        /* fileFormat= */ FileFormat.PARQUET,
        /* recordCount= */ RECORD_COUNT,
        /* fileSizeInBytes= */ FILE_SIZE_IN_BYTES,
        /* specId= */ specId,
        /* partition= */ partition,
        /* contentStats= */ null,
        /* sortOrderId= */ null,
        /* deletionVector= */ dv,
        /* manifestInfo= */ manifestInfo,
        /* keyMetadata= */ null,
        /* splitOffsets= */ null,
        /* equalityIds= */ null);
  }

  private static TrackingStruct addedTracking() {
    return tracking(EntryStatus.ADDED);
  }

  private static TrackingStruct tracking(EntryStatus status) {
    return new TrackingStruct(
        /* status= */ status,
        /* snapshotId= */ SNAPSHOT_ID,
        /* dataSequenceNumber= */ null,
        /* fileSequenceNumber= */ null,
        /* dvSnapshotId= */ null,
        /* firstRowId= */ null,
        /* deletedPositions= */ null,
        /* replacedPositions= */ null);
  }

  private static Integer specId(PartitionData partition) {
    boolean unpartitioned = partition.size() == 0;
    return unpartitioned ? PartitionSpec.unpartitioned().specId() : SPEC.specId();
  }

  private static PartitionData partition(int id) {
    PartitionData partition = new PartitionData(PARTITION_TYPE);
    partition.set(0, id);
    return partition;
  }

  // the location a relative fixture resolves to once read against TABLE_LOCATION
  private static String resolved(String location) {
    return LocationUtil.resolveLocation(TABLE_LOCATION, location);
  }

  private static PartitionData unionPartition(Types.StructType unionType, Integer id, String data) {
    PartitionData partition = new PartitionData(unionType);
    partition.set(0, id);
    partition.set(1, data);
    return partition;
  }

  private static DeletionVector deletionVector(
      String location, long offset, long sizeInBytes, long cardinality) {
    return DeletionVectorStruct.builder()
        .location(location)
        .offset(offset)
        .sizeInBytes(sizeInBytes)
        .cardinality(cardinality)
        .build();
  }

  private InputFile writeManifest(
      FileFormat format, Types.StructType partitionType, Iterable<TrackedFile> files)
      throws IOException {
    Schema writeSchema = TrackedFile.schema(partitionType, Types.StructType.of());
    // write under the table location so a leaf's resolved reference round-trips to the file on disk
    OutputFile out =
        fileIO.newOutputFile(
            TABLE_LOCATION
                + "/metadata/manifest-"
                + System.nanoTime()
                + "."
                + format.name().toLowerCase(Locale.ROOT));
    try (FileAppender<StructLike> appender =
        InternalData.write(format, out).schema(writeSchema).named("tracked_file").build()) {
      for (TrackedFile file : files) {
        appender.add((StructLike) file);
      }
    }

    return out.toInputFile();
  }
}
