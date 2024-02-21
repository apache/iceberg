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

import static org.apache.iceberg.ValidationHelpers.dataSeqs;
import static org.apache.iceberg.ValidationHelpers.fileSeqs;
import static org.apache.iceberg.ValidationHelpers.files;
import static org.apache.iceberg.ValidationHelpers.snapshotIds;
import static org.apache.iceberg.ValidationHelpers.validateDataManifest;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteManifestsAction extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  @Parameters(
      name =
          "snapshotIdInheritanceEnabled = {0}, useCaching = {1}, shouldStageManifests = {2}, formatVersion = {3}")
  public static Object[] parameters() {
    return new Object[][] {
      new Object[] {"true", "true", false, 1},
      new Object[] {"false", "true", true, 1},
      new Object[] {"true", "false", false, 2},
      new Object[] {"false", "false", false, 2}
    };
  }

  @Parameter private String snapshotIdInheritanceEnabled;

  @Parameter(index = 1)
  private String useCaching;

  @Parameter(index = 2)
  private boolean shouldStageManifests;

  @Parameter(index = 3)
  private int formatVersion;

  private String tableLocation = null;

  @TempDir private Path temp;

  @BeforeEach
  public void setupTableLocation() throws Exception {
    File tableDir = temp.resolve("junit").toFile();
    this.tableLocation = tableDir.toURI().toString();
  }

  @TestTemplate
  public void testRewriteManifestsEmptyTable() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();

    SparkActions actions = SparkActions.get();

    actions
        .rewriteManifests(table)
        .rewriteIf(manifest -> true)
        .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
        .stagingLocation(java.nio.file.Files.createTempDirectory(temp, "junit").toString())
        .execute();

    assertThat(table.currentSnapshot()).as("Table must stay empty").isNull();
  }

  @TestTemplate
  public void testRewriteSmallManifestsNonPartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    writeRecords(records1);

    List<ThreeColumnRecord> records2 =
        Lists.newArrayList(
            new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD"));
    writeRecords(records2);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).as("Should have 2 manifests before rewrite").hasSize(2);

    SparkActions actions = SparkActions.get();

    RewriteManifests.Result result =
        actions
            .rewriteManifests(table)
            .rewriteIf(manifest -> true)
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 2 manifests").hasSize(2);
    assertThat(result.addedManifests()).as("Action should add 1 manifests").hasSize(1);
    assertManifestsLocation(result.addedManifests());

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(newManifests).as("Should have 1 manifests after rewrite").hasSize(1);

    assertThat(newManifests.get(0).existingFilesCount()).isEqualTo(4);
    assertThat(newManifests.get(0).hasAddedFiles()).isFalse();
    assertThat(newManifests.get(0).hasDeletedFiles()).isFalse();

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.sort("c1", "c2").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    assertThat(actualRecords).as("Rows must match").isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testRewriteManifestsWithCommitStateUnknownException() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    writeRecords(records1);

    List<ThreeColumnRecord> records2 =
        Lists.newArrayList(
            new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD"));
    writeRecords(records2);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).as("Should have 2 manifests before rewrite").hasSize(2);

    SparkActions actions = SparkActions.get();

    // create a spy which would throw a CommitStateUnknownException after successful commit.
    org.apache.iceberg.RewriteManifests newRewriteManifests = table.rewriteManifests();
    org.apache.iceberg.RewriteManifests spyNewRewriteManifests = spy(newRewriteManifests);
    doAnswer(
            invocation -> {
              newRewriteManifests.commit();
              throw new CommitStateUnknownException(new RuntimeException("Datacenter on Fire"));
            })
        .when(spyNewRewriteManifests)
        .commit();

    Table spyTable = spy(table);
    when(spyTable.rewriteManifests()).thenReturn(spyNewRewriteManifests);

    assertThatThrownBy(
            () -> actions.rewriteManifests(spyTable).rewriteIf(manifest -> true).execute())
        .cause()
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Datacenter on Fire");

    table.refresh();

    // table should reflect the changes, since the commit was successful
    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(newManifests).as("Should have 1 manifests after rewrite").hasSize(1);

    assertThat(newManifests.get(0).existingFilesCount()).isEqualTo(4);
    assertThat(newManifests.get(0).hasAddedFiles()).isFalse();
    assertThat(newManifests.get(0).hasDeletedFiles()).isFalse();

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.sort("c1", "c2").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    assertThat(actualRecords).as("Rows must match").isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testRewriteSmallManifestsPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").truncate("c2", 2).build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    writeRecords(records1);

    List<ThreeColumnRecord> records2 =
        Lists.newArrayList(
            new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD"));
    writeRecords(records2);

    List<ThreeColumnRecord> records3 =
        Lists.newArrayList(
            new ThreeColumnRecord(3, "EEEEEEEEEE", "EEEE"),
            new ThreeColumnRecord(3, "FFFFFFFFFF", "FFFF"));
    writeRecords(records3);

    List<ThreeColumnRecord> records4 =
        Lists.newArrayList(
            new ThreeColumnRecord(4, "GGGGGGGGGG", "GGGG"),
            new ThreeColumnRecord(4, "HHHHHHHHHG", "HHHH"));
    writeRecords(records4);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).as("Should have 4 manifests before rewrite").hasSize(4);

    SparkActions actions = SparkActions.get();

    // we will expect to have 2 manifests with 4 entries in each after rewrite
    long manifestEntrySizeBytes = computeManifestEntrySizeBytes(manifests);
    long targetManifestSizeBytes = (long) (1.05 * 4 * manifestEntrySizeBytes);

    table
        .updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, String.valueOf(targetManifestSizeBytes))
        .commit();

    RewriteManifests.Result result =
        actions
            .rewriteManifests(table)
            .rewriteIf(manifest -> true)
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 4 manifests").hasSize(4);
    assertThat(result.addedManifests()).as("Action should add 2 manifests").hasSize(2);
    assertManifestsLocation(result.addedManifests());

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());

    assertThat(newManifests).as("Should have 2 manifests after rewrite").hasSize(2);

    assertThat(newManifests.get(0).existingFilesCount()).isEqualTo(4);
    assertThat(newManifests.get(0).hasAddedFiles()).isFalse();
    assertThat(newManifests.get(0).hasDeletedFiles()).isFalse();

    assertThat(newManifests.get(1).existingFilesCount()).isEqualTo(4);
    assertThat(newManifests.get(1).hasAddedFiles()).isFalse();
    assertThat(newManifests.get(1).hasDeletedFiles()).isFalse();

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.sort("c1", "c2").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    assertThat(actualRecords).as("Rows must match").isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testRewriteImportedManifests() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c3").build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    File parquetTableDir = temp.resolve("parquet_table").toFile();
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> inputDF = spark.createDataFrame(records, ThreeColumnRecord.class);
      inputDF
          .select("c1", "c2", "c3")
          .write()
          .format("parquet")
          .mode("overwrite")
          .option("path", parquetTableLocation)
          .partitionBy("c3")
          .saveAsTable("parquet_table");

      File stagingDir = temp.resolve("staging-dir").toFile();
      SparkTableUtil.importSparkTable(
          spark, new TableIdentifier("parquet_table"), table, stagingDir.toString());

      // add some more data to create more than one manifest for the rewrite
      inputDF.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);
      table.refresh();

      Snapshot snapshot = table.currentSnapshot();

      SparkActions actions = SparkActions.get();

      String rewriteStagingLocation =
          java.nio.file.Files.createTempDirectory(temp, "junit").toString();

      RewriteManifests.Result result =
          actions
              .rewriteManifests(table)
              .rewriteIf(manifest -> true)
              .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
              .stagingLocation(rewriteStagingLocation)
              .execute();

      assertThat(result.rewrittenManifests())
          .as("Action should rewrite all manifests")
          .isEqualTo(snapshot.allManifests(table.io()));
      assertThat(result.addedManifests()).as("Action should add 1 manifest").hasSize(1);
      assertManifestsLocation(result.addedManifests(), rewriteStagingLocation);

    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @TestTemplate
  public void testRewriteLargeManifestsPartitionedTable() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c3").build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<DataFile> dataFiles = Lists.newArrayList();
    for (int fileOrdinal = 0; fileOrdinal < 1000; fileOrdinal++) {
      dataFiles.add(newDataFile(table, "c3=" + fileOrdinal));
    }
    ManifestFile appendManifest = writeManifest(table, dataFiles);
    table.newFastAppend().appendManifest(appendManifest).commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).as("Should have 1 manifests before rewrite").hasSize(1);

    // set the target manifest size to a small value to force splitting records into multiple files
    table
        .updateProperties()
        .set(
            TableProperties.MANIFEST_TARGET_SIZE_BYTES,
            String.valueOf(manifests.get(0).length() / 2))
        .commit();

    SparkActions actions = SparkActions.get();

    String stagingLocation = java.nio.file.Files.createTempDirectory(temp, "junit").toString();

    RewriteManifests.Result result =
        actions
            .rewriteManifests(table)
            .rewriteIf(manifest -> true)
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .stagingLocation(stagingLocation)
            .execute();

    assertThat(result.rewrittenManifests()).hasSize(1);
    assertThat(result.addedManifests()).hasSizeGreaterThanOrEqualTo(2);
    assertManifestsLocation(result.addedManifests(), stagingLocation);

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(newManifests).hasSizeGreaterThanOrEqualTo(2);
  }

  @TestTemplate
  public void testRewriteManifestsPartitionedTableWithInvalidSortColumnsThowsException()
      throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").bucket("c3", 10).build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    SparkActions actions = SparkActions.get();

    //  c2 is not a partition column, cannot use for sorting
    List<String> badSortKeys1 = ImmutableList.of("c1", "c2");
    assertThatThrownBy(
            () ->
                actions
                    .rewriteManifests(table)
                    .rewriteIf(manifest -> true)
                    .sort(badSortKeys1)
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .message()
        .contains("Cannot use custom sort order");

    // c3_bucket is the correct internal partition name to use, c3 is the untransformed colum name,
    // sort() expects
    // the hidden partition column names
    List<String> badSortKeys2 = ImmutableList.of("c1", "c3");
    assertThatThrownBy(
            () ->
                actions
                    .rewriteManifests(table)
                    .rewriteIf(manifest -> true)
                    .sort(badSortKeys2)
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .message()
        .contains("Cannot use custom sort order");
  }

  @TestTemplate
  public void testRewriteManifestsPartitionedTableWithCustomSorting() throws IOException {
    Random random = new Random();

    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA).identity("c1").truncate("c2", 3).bucket("c3", 10).build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<DataFile> dataFiles = Lists.newArrayList();
    for (int fileOrdinal = 0; fileOrdinal < 1000; fileOrdinal++) {
      dataFiles.add(
          newDataFile(
              table,
              TestHelpers.Row.of(
                  new Object[] {
                    fileOrdinal, String.valueOf(random.nextInt() * 100), random.nextInt(10)
                  })));
    }
    ManifestFile appendManifest = writeManifest(table, dataFiles);
    table.newFastAppend().appendManifest(appendManifest).commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).as("Should have 1 manifests before rewrite").hasSize(1);

    // Capture the c3 partition's lower and upper bounds - used for later test assertions
    Integer c3PartitionMin =
        Conversions.fromByteBuffer(
            Types.IntegerType.get(), manifests.get(0).partitions().get(2).lowerBound());
    Integer c3PartitionMax =
        Conversions.fromByteBuffer(
            Types.IntegerType.get(), manifests.get(0).partitions().get(2).upperBound());

    // Set the target manifest size to a small value to force splitting records into multiple files
    table
        .updateProperties()
        .set(
            TableProperties.MANIFEST_TARGET_SIZE_BYTES,
            String.valueOf(manifests.get(0).length() / 2))
        .commit();

    SparkActions actions = SparkActions.get();

    List<String> manifestSortKeys = ImmutableList.of("c3_bucket", "c2_trunc", "c1");
    RewriteManifests.Result result =
        actions
            .rewriteManifests(table)
            .rewriteIf(manifest -> true)
            .sort(manifestSortKeys)
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    table.refresh();
    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());

    assertThat(result.rewrittenManifests()).hasSize(1);
    assertThat(result.addedManifests()).hasSizeGreaterThanOrEqualTo(2);

    assertThat(newManifests).hasSizeGreaterThanOrEqualTo(2);

    // Rewritten manifests are clustered by c3_bucket - each should contain only a subset of the
    // lower and upper bounds
    // of the partition 'c3'.
    List<Tuple2<Integer, Integer>> c3Boundaries =
        newManifests.stream()
            .map(manifest -> manifest.partitions().get(2))
            .sorted(
                Comparator.comparing(
                    ptn -> Conversions.fromByteBuffer(Types.IntegerType.get(), ptn.lowerBound())))
            .map(
                p ->
                    new Tuple2<Integer, Integer>(
                        Conversions.fromByteBuffer(Types.IntegerType.get(), p.lowerBound()),
                        Conversions.fromByteBuffer(Types.IntegerType.get(), p.upperBound())))
            .collect(Collectors.toList());

    List<Integer> lowers = c3Boundaries.stream().map(t -> t._1).collect(Collectors.toList());
    List<Integer> uppers = c3Boundaries.stream().map(t -> t._2).collect(Collectors.toList());

    // With custom sorting, this looks like
    // - manifest 1 -> [lower bound = 0, upper bound = 4]
    // - manifest 2 -> [lower bound = 4, upper bound = 9]
    // Without the custom sorting, each manifest tracks the full range of c3 upper/lower bounds.
    // AKA they look like
    // - manifest 1 -> [lower bound = 0, upper bound = 9]
    // - manifest 2 -> [lower bound = 0, upper bound = 9]
    // So the upper bound of the partitions tracked by the first file should be LEQ the lower bounds
    // of the second. Etc
    assertThat(uppers.get(0))
        .withFailMessage(
            "Upper bound of first manifest partition should be LEQ lower bound of second")
        .isLessThanOrEqualTo(lowers.get(1));

    // Each file should contain less than the full c3 partition span
    c3Boundaries.forEach(
        boundary -> {
          assertThat(boundary._2 - boundary._1)
              .withFailMessage(
                  "Manifest should contain less than the full range of c3 bucket partitions")
              .isLessThanOrEqualTo(c3PartitionMax - c3PartitionMin);
        });

    // c3's Bucket(10) partition means our true lower bound = 0 and true upper bound is 9. The first
    // manifest should
    // include the lower bound of 0, and the last should have the upper bound of 9
    assertThat(lowers.get(0))
        .withFailMessage("Lower bound of first manifest partition should be 0")
        .isEqualTo(c3PartitionMin);
    assertThat(uppers.get(uppers.size() - 1))
        .withFailMessage("Lower bound of first manifest partition should be 0")
        .isEqualTo(c3PartitionMax);
  }

  @TestTemplate
  public void testRewriteManifestsWithPredicate() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").truncate("c2", 2).build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    writeRecords(records1);

    writeRecords(records1);

    List<ThreeColumnRecord> records2 =
        Lists.newArrayList(
            new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD"));
    writeRecords(records2);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).as("Should have 3 manifests before rewrite").hasSize(3);

    SparkActions actions = SparkActions.get();

    String stagingLocation = java.nio.file.Files.createTempDirectory(temp, "junit").toString();

    // rewrite only the first manifest
    RewriteManifests.Result result =
        actions
            .rewriteManifests(table)
            .rewriteIf(
                manifest ->
                    (manifest.path().equals(manifests.get(0).path())
                        || (manifest.path().equals(manifests.get(1).path()))))
            .stagingLocation(stagingLocation)
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 2 manifest").hasSize(2);
    assertThat(result.addedManifests()).as("Action should add 1 manifests").hasSize(1);
    assertManifestsLocation(result.addedManifests(), stagingLocation);

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(newManifests)
        .as("Should have 2 manifests after rewrite")
        .hasSize(2)
        .as("First manifest must be rewritten")
        .doesNotContain(manifests.get(0))
        .as("Second manifest must be rewritten")
        .doesNotContain(manifests.get(1))
        .as("Third manifest must not be rewritten")
        .contains(manifests.get(2));

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.add(records1.get(0));
    expectedRecords.add(records1.get(0));
    expectedRecords.add(records1.get(1));
    expectedRecords.add(records1.get(1));
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.sort("c1", "c2").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    assertThat(actualRecords).as("Rows must match").isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testRewriteSmallManifestsNonPartitionedV2Table() {
    assumeThat(formatVersion).isGreaterThan(1);

    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, properties, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(new ThreeColumnRecord(1, null, "AAAA"));
    writeRecords(records1);

    table.refresh();

    Snapshot snapshot1 = table.currentSnapshot();
    DataFile file1 = Iterables.getOnlyElement(snapshot1.addedDataFiles(table.io()));

    List<ThreeColumnRecord> records2 = Lists.newArrayList(new ThreeColumnRecord(2, "CCCC", "CCCC"));
    writeRecords(records2);

    table.refresh();

    Snapshot snapshot2 = table.currentSnapshot();
    DataFile file2 = Iterables.getOnlyElement(snapshot2.addedDataFiles(table.io()));

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).as("Should have 2 manifests before rewrite").hasSize(2);

    SparkActions actions = SparkActions.get();
    RewriteManifests.Result result =
        actions
            .rewriteManifests(table)
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();
    assertThat(result.rewrittenManifests()).as("Action should rewrite 2 manifests").hasSize(2);
    assertThat(result.addedManifests()).as("Action should add 1 manifests").hasSize(1);
    assertManifestsLocation(result.addedManifests());

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(newManifests).as("Should have 1 manifests after rewrite").hasSize(1);

    ManifestFile newManifest = Iterables.getOnlyElement(newManifests);
    assertThat(newManifest.existingFilesCount()).isEqualTo(2);
    assertThat(newManifest.hasAddedFiles()).isFalse();
    assertThat(newManifest.hasDeletedFiles()).isFalse();

    validateDataManifest(
        table,
        newManifest,
        dataSeqs(1L, 2L),
        fileSeqs(1L, 2L),
        snapshotIds(snapshot1.snapshotId(), snapshot2.snapshotId()),
        files(file1, file2));

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.sort("c1", "c2").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    assertThat(actualRecords).as("Rows must match").isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testRewriteLargeManifestsEvolvedUnpartitionedV1Table() throws IOException {
    assumeThat(formatVersion).isEqualTo(1);

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c3").build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    table.updateSpec().removeField("c3").commit();

    assertThat(table.spec().fields()).hasSize(1).allMatch(field -> field.transform().isVoid());

    List<DataFile> dataFiles = Lists.newArrayList();
    for (int fileOrdinal = 0; fileOrdinal < 1000; fileOrdinal++) {
      dataFiles.add(newDataFile(table, TestHelpers.Row.of(new Object[] {null})));
    }
    ManifestFile appendManifest = writeManifest(table, dataFiles);
    table.newFastAppend().appendManifest(appendManifest).commit();

    List<ManifestFile> originalManifests = table.currentSnapshot().allManifests(table.io());
    ManifestFile originalManifest = Iterables.getOnlyElement(originalManifests);

    // set the target manifest size to a small value to force splitting records into multiple files
    table
        .updateProperties()
        .set(
            TableProperties.MANIFEST_TARGET_SIZE_BYTES,
            String.valueOf(originalManifest.length() / 2))
        .commit();

    SparkActions actions = SparkActions.get();

    RewriteManifests.Result result =
        actions
            .rewriteManifests(table)
            .rewriteIf(manifest -> true)
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).hasSize(1);
    assertThat(result.addedManifests()).hasSizeGreaterThanOrEqualTo(2);
    assertManifestsLocation(result.addedManifests());

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSizeGreaterThanOrEqualTo(2);
  }

  @TestTemplate
  public void testRewriteSmallDeleteManifestsNonPartitionedTable() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);

    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // commit data records
    List<ThreeColumnRecord> records =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "BBBB"),
            new ThreeColumnRecord(3, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(4, "DDDDDDDDDD", "DDDD"));
    writeRecords(records);

    // commit a position delete file to remove records where c1 = 1 OR c1 = 2
    List<Pair<CharSequence, Long>> posDeletes = generatePosDeletes("c1 = 1 OR c1 = 2");
    Pair<DeleteFile, CharSequenceSet> posDeleteWriteResult = writePosDeletes(table, posDeletes);
    table
        .newRowDelta()
        .addDeletes(posDeleteWriteResult.first())
        .validateDataFilesExist(posDeleteWriteResult.second())
        .commit();

    // commit an equality delete file to remove all records where c1 = 3
    DeleteFile eqDeleteFile = writeEqDeletes(table, "c1", 3);
    table.newRowDelta().addDeletes(eqDeleteFile).commit();

    // the current snapshot should contain 1 data manifest and 2 delete manifests
    List<ManifestFile> originalManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(originalManifests).hasSize(3);

    SparkActions actions = SparkActions.get();

    RewriteManifests.Result result =
        actions
            .rewriteManifests(table)
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    // the original delete manifests must be combined
    assertThat(result.rewrittenManifests())
        .hasSize(2)
        .allMatch(m -> m.content() == ManifestContent.DELETES);
    assertThat(result.addedManifests())
        .hasSize(1)
        .allMatch(m -> m.content() == ManifestContent.DELETES);
    assertManifestsLocation(result.addedManifests());

    // the new delete manifest must only contain files with status EXISTING
    ManifestFile deleteManifest =
        Iterables.getOnlyElement(table.currentSnapshot().deleteManifests(table.io()));
    assertThat(deleteManifest.existingFilesCount()).isEqualTo(2);
    assertThat(deleteManifest.hasAddedFiles()).isFalse();
    assertThat(deleteManifest.hasDeletedFiles()).isFalse();

    // the preserved data manifest must only contain files with status ADDED
    ManifestFile dataManifest =
        Iterables.getOnlyElement(table.currentSnapshot().dataManifests(table.io()));
    assertThat(dataManifest.hasExistingFiles()).isFalse();
    assertThat(dataManifest.hasAddedFiles()).isTrue();
    assertThat(dataManifest.hasDeletedFiles()).isFalse();

    // the table must produce expected records after the rewrite
    List<ThreeColumnRecord> expectedRecords =
        Lists.newArrayList(new ThreeColumnRecord(4, "DDDDDDDDDD", "DDDD"));
    assertThat(actualRecords()).isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testRewriteSmallDeleteManifestsPartitionedTable() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c3").build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    options.put(TableProperties.MANIFEST_MERGE_ENABLED, "false");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // commit data records
    List<ThreeColumnRecord> records =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "BBBB"),
            new ThreeColumnRecord(3, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(4, "DDDDDDDDDD", "DDDD"),
            new ThreeColumnRecord(5, "EEEEEEEEEE", "EEEE"));
    writeRecords(records);

    // commit the first position delete file to remove records where c1 = 1
    List<Pair<CharSequence, Long>> posDeletes1 = generatePosDeletes("c1 = 1");
    Pair<DeleteFile, CharSequenceSet> posDeleteWriteResult1 =
        writePosDeletes(table, TestHelpers.Row.of("AAAA"), posDeletes1);
    table
        .newRowDelta()
        .addDeletes(posDeleteWriteResult1.first())
        .validateDataFilesExist(posDeleteWriteResult1.second())
        .commit();

    // commit the second position delete file to remove records where c1 = 2
    List<Pair<CharSequence, Long>> posDeletes2 = generatePosDeletes("c1 = 2");
    Pair<DeleteFile, CharSequenceSet> positionDeleteWriteResult2 =
        writePosDeletes(table, TestHelpers.Row.of("BBBB"), posDeletes2);
    table
        .newRowDelta()
        .addDeletes(positionDeleteWriteResult2.first())
        .validateDataFilesExist(positionDeleteWriteResult2.second())
        .commit();

    // commit the first equality delete file to remove records where c1 = 3
    DeleteFile eqDeleteFile1 = writeEqDeletes(table, TestHelpers.Row.of("CCCC"), "c1", 3);
    table.newRowDelta().addDeletes(eqDeleteFile1).commit();

    // commit the second equality delete file to remove records where c1 = 4
    DeleteFile eqDeleteFile2 = writeEqDeletes(table, TestHelpers.Row.of("DDDD"), "c1", 4);
    table.newRowDelta().addDeletes(eqDeleteFile2).commit();

    // the table must have 1 data manifest and 4 delete manifests
    List<ManifestFile> originalManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(originalManifests).hasSize(5);

    // set the target manifest size to have 2 manifests with 2 entries in each after the rewrite
    List<ManifestFile> originalDeleteManifests =
        table.currentSnapshot().deleteManifests(table.io());
    long manifestEntrySizeBytes = computeManifestEntrySizeBytes(originalDeleteManifests);
    long targetManifestSizeBytes = (long) (1.05 * 2 * manifestEntrySizeBytes);

    table
        .updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, String.valueOf(targetManifestSizeBytes))
        .commit();

    SparkActions actions = SparkActions.get();

    RewriteManifests.Result result =
        actions
            .rewriteManifests(table)
            .rewriteIf(manifest -> manifest.content() == ManifestContent.DELETES)
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    // the original 4 delete manifests must be replaced with 2 new delete manifests
    assertThat(result.rewrittenManifests())
        .hasSize(4)
        .allMatch(m -> m.content() == ManifestContent.DELETES);
    assertThat(result.addedManifests())
        .hasSize(2)
        .allMatch(m -> m.content() == ManifestContent.DELETES);
    assertManifestsLocation(result.addedManifests());

    List<ManifestFile> deleteManifests = table.currentSnapshot().deleteManifests(table.io());
    assertThat(deleteManifests).hasSize(2);

    // the first new delete manifest must only contain files with status EXISTING
    ManifestFile deleteManifest1 = deleteManifests.get(0);
    assertThat(deleteManifest1.existingFilesCount()).isEqualTo(2);
    assertThat(deleteManifest1.hasAddedFiles()).isFalse();
    assertThat(deleteManifest1.hasDeletedFiles()).isFalse();

    // the second new delete manifest must only contain files with status EXISTING
    ManifestFile deleteManifest2 = deleteManifests.get(1);
    assertThat(deleteManifest2.existingFilesCount()).isEqualTo(2);
    assertThat(deleteManifest2.hasAddedFiles()).isFalse();
    assertThat(deleteManifest2.hasDeletedFiles()).isFalse();

    // the table must produce expected records after the rewrite
    List<ThreeColumnRecord> expectedRecords =
        Lists.newArrayList(new ThreeColumnRecord(5, "EEEEEEEEEE", "EEEE"));
    assertThat(actualRecords()).isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testRewriteLargeDeleteManifestsPartitionedTable() throws IOException {
    assumeThat(formatVersion).isGreaterThan(1);

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c3").build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // generate enough delete files to have a reasonably sized manifest
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    for (int fileOrdinal = 0; fileOrdinal < 1000; fileOrdinal++) {
      DeleteFile deleteFile = newDeleteFile(table, "c3=" + fileOrdinal);
      deleteFiles.add(deleteFile);
    }

    // commit delete files
    RowDelta rowDelta = table.newRowDelta();
    for (DeleteFile deleteFile : deleteFiles) {
      rowDelta.addDeletes(deleteFile);
    }
    rowDelta.commit();

    // the current snapshot should contain only 1 delete manifest
    List<ManifestFile> originalDeleteManifests =
        table.currentSnapshot().deleteManifests(table.io());
    ManifestFile originalDeleteManifest = Iterables.getOnlyElement(originalDeleteManifests);

    // set the target manifest size to a small value to force splitting records into multiple files
    table
        .updateProperties()
        .set(
            TableProperties.MANIFEST_TARGET_SIZE_BYTES,
            String.valueOf(originalDeleteManifest.length() / 2))
        .commit();

    SparkActions actions = SparkActions.get();

    String stagingLocation = java.nio.file.Files.createTempDirectory(temp, "junit").toString();

    RewriteManifests.Result result =
        actions
            .rewriteManifests(table)
            .rewriteIf(manifest -> true)
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .stagingLocation(stagingLocation)
            .execute();

    // the action must rewrite the original delete manifest and add at least 2 new ones
    assertThat(result.rewrittenManifests())
        .hasSize(1)
        .allMatch(m -> m.content() == ManifestContent.DELETES);
    assertThat(result.addedManifests())
        .hasSizeGreaterThanOrEqualTo(2)
        .allMatch(m -> m.content() == ManifestContent.DELETES);
    assertManifestsLocation(result.addedManifests(), stagingLocation);

    // the current snapshot must return the correct number of delete manifests
    List<ManifestFile> deleteManifests = table.currentSnapshot().deleteManifests(table.io());
    assertThat(deleteManifests).hasSizeGreaterThanOrEqualTo(2);
  }

  private List<ThreeColumnRecord> actualRecords() {
    return spark
        .read()
        .format("iceberg")
        .load(tableLocation)
        .as(Encoders.bean(ThreeColumnRecord.class))
        .sort("c1", "c2", "c3")
        .collectAsList();
  }

  private void writeRecords(List<ThreeColumnRecord> records) {
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    writeDF(df);
  }

  private void writeDF(Dataset<Row> df) {
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.DISTRIBUTION_MODE, TableProperties.WRITE_DISTRIBUTION_MODE_NONE)
        .mode("append")
        .save(tableLocation);
  }

  private long computeManifestEntrySizeBytes(List<ManifestFile> manifests) {
    long totalSize = 0L;
    int numEntries = 0;

    for (ManifestFile manifest : manifests) {
      totalSize += manifest.length();
      numEntries +=
          manifest.addedFilesCount() + manifest.existingFilesCount() + manifest.deletedFilesCount();
    }

    return totalSize / numEntries;
  }

  private void assertManifestsLocation(Iterable<ManifestFile> manifests) {
    assertManifestsLocation(manifests, null);
  }

  private void assertManifestsLocation(Iterable<ManifestFile> manifests, String stagingLocation) {
    if (shouldStageManifests && stagingLocation != null) {
      assertThat(manifests).allMatch(manifest -> manifest.path().startsWith(stagingLocation));
    } else {
      assertThat(manifests).allMatch(manifest -> manifest.path().startsWith(tableLocation));
    }
  }

  private ManifestFile writeManifest(Table table, List<DataFile> files) throws IOException {
    File manifestFile = File.createTempFile("generated-manifest", ".avro", temp.toFile());
    assertThat(manifestFile.delete()).isTrue();
    OutputFile outputFile = table.io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter<DataFile> writer =
        ManifestFiles.write(formatVersion, table.spec(), outputFile, null);

    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  private DataFile newDataFile(Table table, String partitionPath) {
    return newDataFileBuilder(table).withPartitionPath(partitionPath).build();
  }

  private DataFile newDataFile(Table table, StructLike partition) {
    return newDataFileBuilder(table).withPartition(partition).build();
  }

  private DataFiles.Builder newDataFileBuilder(Table table) {
    return DataFiles.builder(table.spec())
        .withPath("/path/to/data-" + UUID.randomUUID() + ".parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1);
  }

  private DeleteFile newDeleteFile(Table table, String partitionPath) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withPath("/path/to/pos-deletes-" + UUID.randomUUID() + ".parquet")
        .withFileSizeInBytes(5)
        .withPartitionPath(partitionPath)
        .withRecordCount(1)
        .build();
  }

  private List<Pair<CharSequence, Long>> generatePosDeletes(String predicate) {
    List<Row> rows =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation)
            .selectExpr("_file", "_pos")
            .where(predicate)
            .collectAsList();

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();

    for (Row row : rows) {
      deletes.add(Pair.of(row.getString(0), row.getLong(1)));
    }

    return deletes;
  }

  private Pair<DeleteFile, CharSequenceSet> writePosDeletes(
      Table table, List<Pair<CharSequence, Long>> deletes) throws IOException {
    return writePosDeletes(table, null, deletes);
  }

  private Pair<DeleteFile, CharSequenceSet> writePosDeletes(
      Table table, StructLike partition, List<Pair<CharSequence, Long>> deletes)
      throws IOException {
    OutputFile outputFile = Files.localOutput(File.createTempFile("junit", null, temp.toFile()));
    return FileHelpers.writeDeleteFile(table, outputFile, partition, deletes);
  }

  private DeleteFile writeEqDeletes(Table table, String key, Object... values) throws IOException {
    return writeEqDeletes(table, null, key, values);
  }

  private DeleteFile writeEqDeletes(Table table, StructLike partition, String key, Object... values)
      throws IOException {
    List<Record> deletes = Lists.newArrayList();
    Schema deleteSchema = table.schema().select(key);
    Record delete = GenericRecord.create(deleteSchema);

    for (Object value : values) {
      deletes.add(delete.copy(key, value));
    }

    OutputFile outputFile = Files.localOutput(File.createTempFile("junit", null, temp.toFile()));
    return FileHelpers.writeDeleteFile(table, outputFile, partition, deletes, deleteSchema);
  }
}
