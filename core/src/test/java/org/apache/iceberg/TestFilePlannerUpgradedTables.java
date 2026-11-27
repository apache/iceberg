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

import static org.apache.iceberg.V4TestHelpers.FORMAT_VERSION_V4;
import static org.apache.iceberg.V4TestHelpers.dataFile;
import static org.apache.iceberg.V4TestHelpers.manifestInfo;
import static org.apache.iceberg.V4TestHelpers.manifestRef;
import static org.apache.iceberg.V4TestHelpers.writeTrackedFiles;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.TrackedFileAdapters.ManifestTrackedFile;
import org.apache.iceberg.V4Metadata.RootTrackedFileWrapper;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.mockito.Mockito;

/** Tests planning tables that were upgraded to v4, whose roots carry v1-v3 manifests forward. */
class TestFilePlannerUpgradedTables {
  private static final Namespace NAMESPACE = Namespace.of("db");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "table");
  private static final String TABLE_LOCATION = "s3://bucket/db/table";
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();
  private static final PartitionSpec BY_ID =
      PartitionSpec.builderFor(SCHEMA).identity("id").build();

  private static final List<Integer> PRE_V4_VERSIONS = ImmutableList.of(1, 2, 3);

  private static final DataFile FILE_A = newDataFile(BY_ID, "s3://bucket/data/a.parquet", "id=1");
  private static final DataFile FILE_B = newDataFile(BY_ID, "s3://bucket/data/b.parquet", "id=1");
  private static final DataFile FILE_C = newDataFile(BY_ID, "s3://bucket/data/c.parquet", "id=2");
  private static final DataFile FILE_D = newDataFile(BY_ID, "s3://bucket/data/d.parquet", "id=3");
  private static final DataFile UNPARTITIONED_A =
      newDataFile(UNPARTITIONED, "s3://bucket/data/unpartitioned-a.parquet");
  private static final DataFile FILE_WITH_STATS =
      DataFiles.builder(UNPARTITIONED)
          .withPath("s3://bucket/data/stats.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(2)
          .withMetrics(
              new Metrics(
                  2L,
                  null, // columnSizes
                  null, // valueCounts
                  null, // nullValueCounts
                  null, // nanValueCounts
                  ImmutableMap.of(
                      1,
                      Conversions.toByteBuffer(Types.IntegerType.get(), 1),
                      2,
                      Conversions.toByteBuffer(Types.StringType.get(), "a")),
                  null)) // upperBounds
          .build();

  private final InMemoryCatalog catalog = new InMemoryCatalog();

  @BeforeEach
  void createNamespace() {
    catalog.initialize("catalog", ImmutableMap.of());
    catalog.createNamespace(NAMESPACE);
  }

  @ParameterizedTest
  @FieldSource("PRE_V4_VERSIONS")
  void readsDataFilesFromPreV4Manifests(int formatVersion) throws IOException {
    Table table = createTable(BY_ID, formatVersion);
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newFastAppend().appendFile(FILE_C).commit();
    table.newDelete().deleteFile(FILE_B).commit();
    // a v3 commit assigns first row IDs to the data manifests, which a v4 root requires. The root
    // writer can't assign them to carried-forward manifests, so a v1 or v2 table can't be upgraded
    // directly to v4 yet. The v1 and v2 manifest files are unchanged by the v3 commit.
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
    table.newFastAppend().appendFile(FILE_D).commit();
    List<FileScanTask> beforeUpgrade = planFiles(table);
    upgradeToV4(table);

    List<FileScanTask> tasks = plan(planner(table, carryForward(table)));

    Function<FileScanTask, Tuple> fileMetadata =
        task ->
            tuple(
                task.file().location(),
                task.file().specId(),
                task.file().partition(),
                task.file().dataSequenceNumber(),
                task.file().fileSequenceNumber(),
                task.file().firstRowId());
    assertThat(tasks)
        .extracting(fileMetadata)
        .as("files read through the root should keep the metadata assigned before the upgrade")
        .containsExactlyInAnyOrderElementsOf(
            beforeUpgrade.stream().map(fileMetadata).collect(Collectors.toList()));
  }

  @Test
  void readsPreV4ManifestsWrittenWithDifferentSpecs() throws IOException {
    Table table = createTable(BY_ID, 3);
    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateSpec().removeField("id").addField("data").commit();
    DataFile byData = newDataFile(table.spec(), "s3://bucket/data/by-data.parquet", "data=a");
    table.newFastAppend().appendFile(byData).commit();
    upgradeToV4(table);

    List<FileScanTask> tasks =
        plan(planner(table, carryForward(table)).filterData(Expressions.equal("id", 1)));

    assertThat(tasks)
        .extracting(
            task -> task.file().location(),
            task -> task.spec().specId(),
            task -> task.residual().toString())
        .as("each file should use its manifest's spec, which determines its residual")
        .containsExactlyInAnyOrder(
            tuple(FILE_A.location(), BY_ID.specId(), Expressions.alwaysTrue().toString()),
            tuple(byData.location(), table.spec().specId(), Expressions.equal("id", 1).toString()));
  }

  @Test
  void readsManifestsFromMultipleUnpartitionedSpecs() throws IOException {
    Table table = createTable(UNPARTITIONED, 1);
    table.newFastAppend().appendFile(UNPARTITIONED_A).commit();
    // v1 replaces a removed partition field with a void transform, a second unpartitioned spec
    table.updateSpec().addField("data").commit();
    table.updateSpec().removeField("data").commit();
    PartitionSpec voidSpec = table.spec();
    DataFile voidFile = newDataFile(voidSpec, "s3://bucket/data/void.parquet");
    table.newFastAppend().appendFile(voidFile).commit();
    // a v3 commit assigns first row IDs to the data manifests, which a v4 root requires
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
    DataFile v3File = newDataFile(voidSpec, "s3://bucket/data/v3.parquet");
    table.newFastAppend().appendFile(v3File).commit();
    upgradeToV4(table);

    List<FileScanTask> tasks = plan(planner(table, carryForward(table)));

    assertThat(tasks)
        .extracting(task -> task.file().location(), task -> task.file().specId())
        .containsExactlyInAnyOrder(
            tuple(UNPARTITIONED_A.location(), UNPARTITIONED.specId()),
            tuple(voidFile.location(), voidSpec.specId()),
            tuple(v3File.location(), voidSpec.specId()));
  }

  @Test
  void rootMixesPreV4ManifestsAndV4Files() throws IOException {
    Table table = createTable(UNPARTITIONED, 3);
    table.newFastAppend().appendFile(UNPARTITIONED_A).commit();
    upgradeToV4(table);

    String rootFile = "s3://bucket/data/v4-root-file.parquet";
    String leafFile = "s3://bucket/data/v4-leaf-file.parquet";
    OutputFile leaf = table.io().newOutputFile(TABLE_LOCATION + "/metadata/leaf.parquet");
    writeTrackedFiles(
        leaf,
        FileFormat.PARQUET,
        Types.StructType.of(),
        statsType(table),
        ImmutableList.of(dataFile(leafFile, null /* specId */, null /* partition */)));

    ManifestFile root =
        writeRoot(
            table,
            table.currentSnapshot().allManifests(table.io()),
            ImmutableList.of(
                dataFile(rootFile, null /* specId */, null /* partition */),
                manifestRef(
                    FileContent.DATA_MANIFEST,
                    leaf.location(),
                    manifestInfo(1, 0, 0, 0, FORMAT_VERSION_V4))));

    assertThat(plan(planner(table, root)))
        .extracting(task -> task.file().location())
        .containsExactlyInAnyOrder(UNPARTITIONED_A.location(), rootFile, leafFile);
  }

  @Test
  void filterHonorsCaseSensitivityForPreV4Manifests() throws IOException {
    Table table = createTable(BY_ID, 3);
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_C).commit();
    upgradeToV4(table);

    List<FileScanTask> tasks =
        plan(
            planner(table, carryForward(table))
                .filterData(Expressions.equal("ID", 1))
                .caseSensitive(false));

    assertThat(tasks).extracting(task -> task.file().location()).containsExactly(FILE_A.location());
  }

  @Test
  void preV4FilesOmitStatsUnlessProjected() throws IOException {
    Table table = createTable(UNPARTITIONED, 3);
    table.newFastAppend().appendFile(FILE_WITH_STATS).commit();
    upgradeToV4(table);

    DataFile file = Iterables.getOnlyElement(plan(planner(table, carryForward(table)))).file();

    assertThat(file.lowerBounds()).as("no stats are returned unless projected").isNull();
  }

  @Test
  void preV4FilesReturnProjectedStats() throws IOException {
    Table table = createTable(UNPARTITIONED, 3);
    table.newFastAppend().appendFile(FILE_WITH_STATS).commit();
    upgradeToV4(table);

    DataFile file =
        Iterables.getOnlyElement(plan(planner(table, carryForward(table)).projectStats(1))).file();

    assertThat(file.lowerBounds())
        .as("only stats for projected columns are returned")
        .containsOnlyKeys(1);
  }

  @Test
  void closingWithoutIteratingSkipsPreV4Manifests() throws IOException {
    Table table = createTable(UNPARTITIONED, 3);
    table.newFastAppend().appendFile(UNPARTITIONED_A).commit();
    upgradeToV4(table);
    ManifestFile manifest =
        Iterables.getOnlyElement(table.currentSnapshot().allManifests(table.io()));
    ManifestFile root = carryForward(table);

    FileIO io = Mockito.spy(table.io());
    new FilePlanner(io, root, table.schema(), table.specs()).planFiles().close();

    Mockito.verify(io, Mockito.never()).newInputFile(manifest.path());
  }

  @Test
  void scanMetricsForPreV4Manifests() throws IOException {
    Table table = createTable(BY_ID, 3);
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_C).appendFile(FILE_D).commit();
    upgradeToV4(table);

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    plan(
        planner(table, carryForward(table))
            .filterData(Expressions.equal("id", 1))
            .scanMetrics(metrics));

    assertThat(metrics.scannedDataManifests().value())
        .as("the root and the pre-v4 manifest are scanned")
        .isEqualTo(2L);
    assertThat(metrics.totalDataManifests().value()).isEqualTo(2L);
    assertThat(metrics.skippedDataFiles().value())
        .as("files pruned inside the pre-v4 manifest are skipped")
        .isEqualTo(2L);
  }

  private Table createTable(PartitionSpec spec, int formatVersion) {
    return catalog
        .buildTable(TABLE, SCHEMA)
        .withPartitionSpec(spec)
        .withLocation(TABLE_LOCATION)
        .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .create();
  }

  private static void upgradeToV4(Table table) {
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "4").commit();
  }

  // writes the root of the first v4 commit, which carries the current snapshot's manifests forward
  private static RootManifestFile carryForward(Table table) throws IOException {
    return writeRoot(table, table.currentSnapshot().allManifests(table.io()), ImmutableList.of());
  }

  private static RootManifestFile writeRoot(
      Table table, Iterable<ManifestFile> preV4Manifests, Iterable<TrackedFile> v4Files)
      throws IOException {
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata base = ops.current();
    long snapshotId = base.currentSnapshot().snapshotId() + 1;
    long sequenceNumber = base.lastSequenceNumber() + 1;
    Types.StructType partitionType = Partitioning.unionPartitionTypes(table.specs().values());
    Types.StructType statsType = statsType(table);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            snapshotId, sequenceNumber, table.specs(), partitionType, statsType);
    ManifestTrackedFile adapter = TrackedFileAdapters.forManifestFile();
    OutputFile out =
        table
            .io()
            .newOutputFile(ops.metadataFileLocation("root-" + System.nanoTime() + ".parquet"));
    try (FileAppender<StructLike> appender =
        InternalData.write(FileFormat.PARQUET, out)
            .schema(TrackedFile.schema(partitionType, statsType))
            .named("tracked_file")
            .build()) {
      for (ManifestFile manifest : preV4Manifests) {
        TrackedFile file = adapter.wrap(manifest);
        appender.add(wrapper.wrap(file, file.tracking(), null /* assignedFirstRowId */));
      }

      for (TrackedFile file : v4Files) {
        appender.add((StructLike) file);
      }
    }

    return new RootManifestFile(
        out.location(),
        out.toInputFile().getLength(),
        snapshotId,
        sequenceNumber,
        base.nextRowId(),
        null /* keyMetadata */);
  }

  private static FilePlanner planner(Table table, ManifestFile root) {
    return new FilePlanner(table.io(), root, table.schema(), table.specs())
        .tableLocation(table.location());
  }

  private static List<FileScanTask> plan(FilePlanner planner) throws IOException {
    try (CloseableIterable<FileScanTask> tasks = planner.planFiles()) {
      return Lists.newArrayList(tasks);
    }
  }

  private static List<FileScanTask> planFiles(Table table) throws IOException {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      return Lists.newArrayList(tasks);
    }
  }

  private static Types.StructType statsType(Table table) {
    return StatsUtil.statsWriteSchema(table.schema(), MetricsConfig.forTable(table));
  }

  private static DataFile newDataFile(PartitionSpec spec, String location) {
    return DataFiles.builder(spec)
        .withPath(location)
        .withFileSizeInBytes(10)
        .withRecordCount(2)
        .build();
  }

  private static DataFile newDataFile(PartitionSpec spec, String location, String partitionPath) {
    return DataFiles.builder(spec)
        .withPath(location)
        .withFileSizeInBytes(10)
        .withRecordCount(2)
        .withPartitionPath(partitionPath)
        .build();
  }
}
