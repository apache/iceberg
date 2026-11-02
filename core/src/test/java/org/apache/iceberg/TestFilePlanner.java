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

import static org.apache.iceberg.V4TestHelpers.FILE_SIZE_IN_BYTES;
import static org.apache.iceberg.V4TestHelpers.FORMAT_VERSION_V4;
import static org.apache.iceberg.V4TestHelpers.RECORD_COUNT;
import static org.apache.iceberg.V4TestHelpers.SNAPSHOT_ID;
import static org.apache.iceberg.V4TestHelpers.dataFile;
import static org.apache.iceberg.V4TestHelpers.deletionVector;
import static org.apache.iceberg.V4TestHelpers.manifestRef;
import static org.apache.iceberg.V4TestHelpers.manifestRefWithStats;
import static org.apache.iceberg.V4TestHelpers.writeTrackedFiles;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.mockito.Mockito;

class TestFilePlanner {
  private static final long SEQUENCE_NUMBER = 7L;
  private static final long FIRST_ROW_ID = 1000L;
  private static final String TABLE_LOCATION = "s3://bucket/db/table";
  private static final DeletionVector DV =
      deletionVector("s3://bucket/db/table/dv.puffin", 100L, 50L, 5L);

  private static final String FILE_A = "s3://bucket/db/table/file-a.parquet";
  private static final String FILE_B = "s3://bucket/db/table/file-b.parquet";
  private static final String FILE_C = "s3://bucket/db/table/file-c.parquet";
  private static final String FILE_D = "s3://bucket/db/table/file-d.parquet";
  private static final String PARTITIONED_FILE = "s3://bucket/db/table/partitioned.parquet";
  private static final String UNPARTITIONED_FILE = "s3://bucket/db/table/unpartitioned.parquet";

  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final int UNPARTITIONED_SPEC_ID = PartitionSpec.unpartitioned().specId();
  private static final Types.StructType EMPTY_PARTITION = Types.StructType.of();
  private static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_PARTITION);
  private static final Map<Integer, PartitionSpec> UNPARTITIONED_SPECS =
      ImmutableMap.of(UNPARTITIONED_SPEC_ID, PartitionSpec.unpartitioned());

  private static final PartitionSpec UNPARTITIONED_WITH_SCHEMA =
      PartitionSpec.builderFor(TABLE_SCHEMA).withSpecId(0).build();
  private static final PartitionSpec BY_ID_SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA).withSpecId(1).identity("id").build();
  private static final Map<Integer, PartitionSpec> MIXED_SPECS =
      ImmutableMap.of(
          UNPARTITIONED_WITH_SCHEMA.specId(), UNPARTITIONED_WITH_SCHEMA,
          BY_ID_SPEC.specId(), BY_ID_SPEC);

  private static final List<FileFormat> MANIFEST_FORMATS =
      ImmutableList.of(FileFormat.AVRO, FileFormat.PARQUET);

  private static final MetricsConfig METRICS_CONFIG =
      MetricsConfig.from(ImmutableMap.of(), TABLE_SCHEMA, null);
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsWriteSchema(TABLE_SCHEMA, METRICS_CONFIG);

  private final InMemoryFileIO fileIO = new InMemoryFileIO();

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void rootWithOnlyDataFiles(FileFormat format) throws IOException {
    ManifestFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(unpartitionedDataFile(FILE_A), unpartitionedDataFile(FILE_B)));

    List<FileScanTask> tasks = plan(root, UNPARTITIONED_SPECS);

    assertThat(tasks)
        .hasSize(2)
        .extracting(task -> task.file().location())
        .containsExactlyInAnyOrder(FILE_A, FILE_B);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void rootWithOnlyLeafManifests(FileFormat format) throws IOException {
    ManifestFile leafA =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(unpartitionedDataFile(FILE_A)));
    ManifestFile leafB =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(unpartitionedDataFile(FILE_B)));
    ManifestFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataManifest(leafA.path()), dataManifest(leafB.path())));

    List<FileScanTask> tasks = plan(root, UNPARTITIONED_SPECS);

    assertThat(tasks)
        .hasSize(2)
        .extracting(task -> task.file().location())
        .containsExactlyInAnyOrder(FILE_A, FILE_B);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void rootWithMixedDataAndLeafManifests(FileFormat format) throws IOException {
    ManifestFile leaf1 =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(unpartitionedDataFile(FILE_C)));
    ManifestFile leaf2 =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(unpartitionedDataFile(FILE_D)));
    ManifestFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                unpartitionedDataFile(FILE_A),
                dataManifest(leaf1.path()),
                dataManifest(leaf2.path())));

    List<FileScanTask> tasks = plan(root, UNPARTITIONED_SPECS);

    assertThat(tasks)
        .hasSize(3)
        .extracting(task -> task.file().location())
        .containsExactlyInAnyOrder(FILE_A, FILE_C, FILE_D);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void filterSkipsAndMatchesRootDataFiles(FileFormat format) throws IOException {
    ManifestFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFileWithStats(FILE_A, idStats(0, 99)),
                dataFileWithStats(FILE_B, idStats(100, 199))));

    List<FileScanTask> tasks =
        plan(root, UNPARTITIONED_SPECS, planner -> planner.filterData(Expressions.equal("id", 50)));

    assertThat(tasks)
        .extracting(task -> task.file().location())
        .as("only the root data file whose stats match the filter survives")
        .containsExactlyInAnyOrder(FILE_A);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void filterSkipsAndMatchesRootDataFilesAndManifests(FileFormat format) throws IOException {
    ManifestFile matchedLeaf =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFileWithStats(FILE_C, idStats(0, 99)),
                dataFileWithStats(FILE_D, idStats(100, 199))));
    ManifestFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFileWithStats(FILE_A, idStats(0, 99)),
                dataFileWithStats(FILE_B, idStats(100, 199)),
                dataManifestWithStats(matchedLeaf.path(), idStats(0, 199)),
                dataManifestWithStats(
                    "s3://bucket/db/table/pruned-leaf.parquet", idStats(100, 199))));

    List<FileScanTask> tasks =
        plan(root, UNPARTITIONED_SPECS, planner -> planner.filterData(Expressions.equal("id", 50)));

    assertThat(tasks)
        .extracting(task -> task.file().location())
        .as(
            "the matched root data file and the file in the matched leaf survive; the pruned data file and leaf do not")
        .containsExactlyInAnyOrder(FILE_A, FILE_C);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void filterSkipsAndMatchesLeafDataFiles(FileFormat format) throws IOException {
    ManifestFile leaf =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFileWithStats(FILE_C, idStats(0, 99)),
                dataFileWithStats(FILE_D, idStats(100, 199))));
    ManifestFile root =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(dataManifest(leaf.path(), 2)));

    List<FileScanTask> tasks =
        plan(root, UNPARTITIONED_SPECS, planner -> planner.filterData(Expressions.equal("id", 50)));

    assertThat(Iterables.getOnlyElement(tasks).file().location())
        .as("only the leaf file whose stats match the filter survives")
        .isEqualTo(FILE_C);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void filterHonorsCaseSensitivity(FileFormat format) throws IOException {
    ManifestFile leaf =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFileWithStats(FILE_C, idStats(0, 99)),
                dataFileWithStats(FILE_D, idStats(100, 199))));
    ManifestFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFileWithStats(FILE_A, idStats(0, 99)),
                dataFileWithStats(FILE_B, idStats(100, 199)),
                dataManifestWithStats(leaf.path(), idStats(0, 199))));

    List<FileScanTask> tasks =
        plan(
            root,
            UNPARTITIONED_SPECS,
            planner -> planner.filterData(Expressions.equal("ID", 50)).caseSensitive(false));
    assertThat(tasks)
        .extracting(task -> task.file().location())
        .as("a case-insensitive filter resolves \"ID\" to \"id\" in both the root and leaf readers")
        .containsExactlyInAnyOrder(FILE_A, FILE_C);

    assertThatThrownBy(
            () ->
                plan(
                    root,
                    UNPARTITIONED_SPECS,
                    planner -> planner.filterData(Expressions.equal("ID", 50))))
        .as("the default case-sensitive filter cannot resolve \"ID\"")
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'ID'");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void taskAttachesColocatedDV(FileFormat format) throws IOException {
    TrackedFile fileWithDV =
        unpartitionedDataFileWithDV("s3://bucket/db/table/with-dv.parquet", DV);
    TrackedFile fileWithoutDV = unpartitionedDataFile(FILE_A);
    ManifestFile root =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(fileWithDV, fileWithoutDV));

    List<FileScanTask> tasks = plan(root, UNPARTITIONED_SPECS);

    assertThat(tasks)
        .hasSize(2)
        .satisfiesExactlyInAnyOrder(
            task -> {
              assertThat(task.file().location()).isEqualTo("s3://bucket/db/table/with-dv.parquet");
              assertThat(task.deletes())
                  .as("the colocated DV is attached as the file's only delete")
                  .hasSize(1)
                  .allSatisfy(delete -> assertThat(delete.location()).isEqualTo(DV.location()));
            },
            task -> {
              assertThat(task.file().location()).isEqualTo(FILE_A);
              assertThat(task.deletes())
                  .as("a file without a colocated DV has no deletes")
                  .isEmpty();
            });
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void taskExposesFileSchemaAndSpec(FileFormat format) throws IOException {
    Types.StructType unionType = Partitioning.unionPartitionTypes(MIXED_SPECS.values());
    ManifestFile root =
        writeManifest(
            format,
            unionType,
            ImmutableList.of(
                dataFile(PARTITIONED_FILE, BY_ID_SPEC.specId(), idPartition(unionType, 1)),
                dataFile(
                    UNPARTITIONED_FILE,
                    UNPARTITIONED_WITH_SCHEMA.specId(),
                    idPartition(unionType, null))));

    List<FileScanTask> tasks = plan(root, MIXED_SPECS);

    assertThat(tasks)
        .hasSize(2)
        .satisfiesExactlyInAnyOrder(
            task -> {
              assertThat(task.file().location()).isEqualTo(PARTITIONED_FILE);
              assertThat(task.spec())
                  .as("the task carries the full spec its file was written with")
                  .isEqualTo(BY_ID_SPEC);
              assertThat(task.schema().asStruct()).isEqualTo(TABLE_SCHEMA.asStruct());
            },
            task -> {
              assertThat(task.file().location()).isEqualTo(UNPARTITIONED_FILE);
              assertThat(task.spec())
                  .as("the task carries the full spec its file was written with")
                  .isEqualTo(UNPARTITIONED_WITH_SCHEMA);
              assertThat(task.schema().asStruct()).isEqualTo(TABLE_SCHEMA.asStruct());
            });
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void residualForMixedPartitionedAndUnpartitioned(FileFormat format) throws IOException {
    Types.StructType unionType = Partitioning.unionPartitionTypes(MIXED_SPECS.values());
    ManifestFile root =
        writeManifest(
            format,
            unionType,
            ImmutableList.of(
                dataFile(PARTITIONED_FILE, BY_ID_SPEC.specId(), idPartition(unionType, 1)),
                dataFile(
                    UNPARTITIONED_FILE,
                    UNPARTITIONED_WITH_SCHEMA.specId(),
                    idPartition(unionType, null))));

    Expression filter = Expressions.and(Expressions.equal("id", 1), Expressions.equal("data", "x"));
    List<FileScanTask> tasks = plan(root, MIXED_SPECS, planner -> planner.filterData(filter));

    assertThat(tasks)
        .extracting(task -> task.file().location(), task -> task.residual().toString())
        .as(
            "the id predicate drops out of the partitioned file's residual but not the unpartitioned")
        .containsExactlyInAnyOrder(
            tuple(PARTITIONED_FILE, Expressions.equal("data", "x").toString()),
            tuple(UNPARTITIONED_FILE, filter.toString()));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void ignoreResidualsProducesAlwaysTrueResidual(FileFormat format) throws IOException {
    Types.StructType unionType = Partitioning.unionPartitionTypes(MIXED_SPECS.values());
    ManifestFile root =
        writeManifest(
            format,
            unionType,
            ImmutableList.of(
                dataFile(PARTITIONED_FILE, BY_ID_SPEC.specId(), idPartition(unionType, 1)),
                dataFile(
                    UNPARTITIONED_FILE,
                    UNPARTITIONED_WITH_SCHEMA.specId(),
                    idPartition(unionType, null))));

    Expression filter = Expressions.and(Expressions.equal("id", 1), Expressions.equal("data", "x"));
    List<FileScanTask> tasks =
        plan(root, MIXED_SPECS, planner -> planner.filterData(filter).ignoreResiduals());

    assertThat(tasks)
        .as("ignoreResiduals forces an alwaysTrue residual for every file regardless of spec")
        .hasSize(2)
        .allSatisfy(task -> assertThat(task.residual()).isEqualTo(Expressions.alwaysTrue()));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void tableLocationForwardedToRootAndLeafReaders(FileFormat format) throws IOException {
    ManifestFile leaf =
        writeManifest(
            format, EMPTY_PARTITION, ImmutableList.of(unpartitionedDataFile("leaf-data.parquet")));
    ManifestFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                unpartitionedDataFile("root-data.parquet"), dataManifest(leaf.path())));

    List<FileScanTask> tasks =
        plan(root, UNPARTITIONED_SPECS, planner -> planner.tableLocation(TABLE_LOCATION));

    assertThat(tasks)
        .extracting(task -> task.file().location())
        .as("relative paths resolve against the table location in both the root and leaf readers")
        .containsExactlyInAnyOrder(
            TABLE_LOCATION + "/root-data.parquet", TABLE_LOCATION + "/leaf-data.parquet");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void closingWithoutIteratingOpensOnlyRoot(FileFormat format) throws IOException {
    ManifestFile leaf =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(unpartitionedDataFile(FILE_A)));
    ManifestFile root =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(dataManifest(leaf.path())));

    FileIO io = Mockito.spy(fileIO);
    new FilePlanner(io, root, TABLE_SCHEMA, UNPARTITIONED_SPECS).planFiles().close();

    Mockito.verify(io).newInputFile(root.path());
    Mockito.verify(io, Mockito.never()).newInputFile(leaf.path());
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void leafFilesOpenedLazily(FileFormat format) throws IOException {
    ManifestFile leaf1 =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(unpartitionedDataFile(FILE_A), unpartitionedDataFile(FILE_B)));
    ManifestFile leaf2 =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(unpartitionedDataFile(FILE_C)));
    ManifestFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataManifest(leaf1.path()), dataManifest(leaf2.path())));

    FileIO io = Mockito.spy(fileIO);
    FilePlanner planner =
        new FilePlanner(io, root, TABLE_SCHEMA, UNPARTITIONED_SPECS).planWith(null);

    try (CloseableIterable<FileScanTask> plan = planner.planFiles()) {
      CloseableIterator<FileScanTask> tasks = plan.iterator();

      assertThat(tasks.next().file().location()).isEqualTo(FILE_A);
      Mockito.verify(io).newInputFile(leaf1.path());
      Mockito.verify(io, Mockito.never()).newInputFile(leaf2.path());

      assertThat(tasks.next().file().location()).isEqualTo(FILE_B);
      Mockito.verify(io, Mockito.never()).newInputFile(leaf2.path());

      assertThat(tasks.next().file().location()).isEqualTo(FILE_C);
      Mockito.verify(io).newInputFile(leaf2.path());
      assertThat(tasks).isExhausted();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void deleteManifestInRootUnsupported(FileFormat format) throws IOException {
    ManifestFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(deleteManifest("s3://bucket/db/table/deletes.avro")));

    FilePlanner planner = new FilePlanner(fileIO, root, TABLE_SCHEMA, UNPARTITIONED_SPECS);
    assertThatThrownBy(() -> Lists.newArrayList(planner.planFiles()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("v3 and earlier deletes are not yet supported");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void v3ManifestLeafUnsupported(FileFormat format) throws IOException {
    String leafLocation = "s3://bucket/db/table/v3-leaf." + format.name().toLowerCase(Locale.ROOT);
    TrackedFile v3Leaf =
        manifestRef(FileContent.DATA_MANIFEST, leafLocation, dataManifestInfo(1, 0));
    ManifestFile root = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(v3Leaf));

    FilePlanner planner = new FilePlanner(fileIO, root, TABLE_SCHEMA, UNPARTITIONED_SPECS);
    assertThatThrownBy(() -> Lists.newArrayList(planner.planFiles()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot read manifest with format version 0: only 4 is supported");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  void scanMetricsForComplexFilteredPlan(FileFormat format) throws IOException {
    ManifestFile matchedLeaf =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFileWithStats(FILE_C, idStats(0, 99)),
                dataFileWithStats(FILE_D, idStats(100, 199))));
    ManifestFile root =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(
                dataFileWithStats(FILE_A, idStats(0, 99), DV),
                dataFileWithStats(FILE_B, idStats(100, 199)),
                dataManifestWithStats(matchedLeaf.path(), idStats(0, 199)),
                dataManifestWithStats(
                    "s3://bucket/db/table/pruned-leaf.parquet", idStats(100, 199))));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    List<FileScanTask> tasks =
        plan(
            root,
            UNPARTITIONED_SPECS,
            planner -> planner.filterData(Expressions.equal("id", 50)).scanMetrics(metrics));

    assertThat(tasks)
        .extracting(task -> task.file().location())
        .containsExactlyInAnyOrder(FILE_A, FILE_C);
    assertThat(metrics.resultDataFiles().value()).isEqualTo(2L);
    assertThat(metrics.totalFileSizeInBytes().value()).isEqualTo(2 * FILE_SIZE_IN_BYTES);
    assertThat(metrics.scannedDataManifests().value())
        .as("the root and the matched leaf are scanned")
        .isEqualTo(2L);
    assertThat(metrics.skippedDataManifests().value())
        .as("the pruned leaf ref is skipped")
        .isEqualTo(1L);
    assertThat(metrics.skippedDataFiles().value())
        .as("the pruned root and leaf data files are skipped")
        .isEqualTo(2L);
    assertThat(metrics.resultDeleteFiles().value())
        .as("only the colocated DV contributes a delete file")
        .isEqualTo(1L);
    assertThat(metrics.totalDeleteFileSizeInBytes().value()).isEqualTo(DV.sizeInBytes());
    assertThat(metrics.dvs().value()).isEqualTo(1L);
    assertThat(metrics.indexedDeleteFiles().value())
        .as("colocated DVs are not indexed delete files")
        .isEqualTo(0L);
  }

  private List<FileScanTask> plan(ManifestFile root, Map<Integer, PartitionSpec> specsById)
      throws IOException {
    return plan(root, specsById, UnaryOperator.identity());
  }

  private List<FileScanTask> plan(
      ManifestFile root,
      Map<Integer, PartitionSpec> specsById,
      UnaryOperator<FilePlanner> configure)
      throws IOException {
    FilePlanner planner = configure.apply(new FilePlanner(fileIO, root, TABLE_SCHEMA, specsById));
    try (CloseableIterable<FileScanTask> tasks = planner.planFiles()) {
      return Lists.newArrayList(tasks);
    }
  }

  private static TrackedFile unpartitionedDataFile(String location) {
    return dataFile(location, UNPARTITIONED_SPEC_ID, EMPTY_PARTITION_DATA);
  }

  private static TrackedFile unpartitionedDataFileWithDV(String location, DeletionVector dv) {
    return dataFile(location, UNPARTITIONED_SPEC_ID, EMPTY_PARTITION_DATA, null, dv);
  }

  private static TrackedFile dataFileWithStats(String location, ContentStats stats) {
    return dataFileWithStats(location, stats, null);
  }

  private static TrackedFile dataFileWithStats(
      String location, ContentStats stats, DeletionVector dv) {
    return dataFile(location, UNPARTITIONED_SPEC_ID, EMPTY_PARTITION_DATA, stats, dv);
  }

  private static TrackedFile dataManifest(String location) {
    return dataManifest(location, 1);
  }

  private static TrackedFile dataManifest(String location, int addedFiles) {
    return manifestRef(FileContent.DATA_MANIFEST, location, dataManifestInfo(addedFiles));
  }

  private static TrackedFile dataManifestWithStats(String location, ContentStats stats) {
    return manifestRefWithStats(FileContent.DATA_MANIFEST, location, stats, dataManifestInfo(1));
  }

  private static ContentStats idStats(int lower, int upper) {
    ContentStatsStruct stats = new ContentStatsStruct(STATS_TYPE);
    stats.setStats(
        TABLE_SCHEMA.findField("id").fieldId(),
        new FieldStatsStruct<>(
            STATS_TYPE.fieldType("id").asStructType(),
            lower,
            upper,
            true, // tightBounds
            RECORD_COUNT,
            0, // nullValueCount
            0, // nanValueCount
            0L)); // totalBytes
    return stats;
  }

  private static ManifestInfo dataManifestInfo(int addedFiles) {
    return dataManifestInfo(addedFiles, FORMAT_VERSION_V4);
  }

  private static ManifestInfo dataManifestInfo(int addedFiles, int formatVersion) {
    return ManifestInfoStruct.builder()
        .addedFilesCount(addedFiles)
        .existingFilesCount(0)
        .deletedFilesCount(0)
        .replacedFilesCount(0)
        .modifiedFilesCount(0)
        .addedRowsCount(addedFiles * RECORD_COUNT)
        .existingRowsCount(0)
        .deletedRowsCount(0)
        .replacedRowsCount(0)
        .modifiedRowsCount(0)
        .minSequenceNumber(0L)
        .formatVersion(formatVersion)
        .build();
  }

  private static TrackedFile deleteManifest(String location) {
    return manifestRef(FileContent.DELETE_MANIFEST, location, null);
  }

  private static PartitionData idPartition(Types.StructType unionType, Integer id) {
    PartitionData partition = new PartitionData(unionType);
    partition.set(0, id);
    return partition;
  }

  private ManifestFile writeManifest(
      FileFormat format, Types.StructType partitionType, Iterable<TrackedFile> files)
      throws IOException {
    OutputFile out =
        fileIO.newOutputFile(
            TABLE_LOCATION
                + "/metadata/manifest-"
                + System.nanoTime()
                + "."
                + format.name().toLowerCase(Locale.ROOT));
    InputFile written =
        writeTrackedFiles(out, format, partitionType, STATS_TYPE, files).toInputFile();
    return new RootManifestFile(
        written.location(),
        written.getLength(),
        SNAPSHOT_ID,
        SEQUENCE_NUMBER,
        FIRST_ROW_ID,
        /* keyMetadata= */ null);
  }
}
