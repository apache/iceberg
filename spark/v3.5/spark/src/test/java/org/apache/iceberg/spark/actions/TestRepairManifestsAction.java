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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.RepairManifests;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRepairManifestsAction extends TestBase {

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
  public void setupTableLocation() {
    File tableDir = temp.resolve("junit").toFile();
    this.tableLocation = tableDir.toURI().toString();
  }

  @TestTemplate
  public void testRemoveDuplicatesManifestsEmptyTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();

    SparkActions actions = SparkActions.get();

    actions
        .repairManifests(table)
        .removeDuplicateCommittedFiles()
        .option(RepairManifestsSparkAction.USE_CACHING, useCaching)
        .execute();

    assertThat(table.currentSnapshot()).as("Table must stay empty").isNull();
  }

  @TestTemplate
  public void testRemoveMissingFilesManifestsEmptyTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();

    SparkActions actions = SparkActions.get();

    actions
        .repairManifests(table)
        .removeMissingFiles()
        .option(RepairManifestsSparkAction.USE_CACHING, useCaching)
        .execute();

    assertThat(table.currentSnapshot()).as("Table must stay empty").isNull();
  }

  @TestTemplate
  public void testRemoveDuplicatesNoDuplicates() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    writeRecords(records1);
    table.refresh();

    List<ManifestFile> manifestsBeforeAction = table.currentSnapshot().allManifests(table.io());

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.addedManifests().iterator().hasNext()).isFalse();
    assertThat(result.rewrittenManifests().iterator().hasNext()).isFalse();

    table.refresh();
    List<ManifestFile> manifestsAfterAction = table.currentSnapshot().allManifests(table.io());

    assertThat(manifestsBeforeAction).isEqualTo(manifestsAfterAction);
  }

  @TestTemplate
  public void testRepairManifestsEmptyTableRemoveMissingRemoveDupes() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();

    SparkActions actions = SparkActions.get();

    actions
        .repairManifests(table)
        .removeMissingFiles()
        .removeDuplicateCommittedFiles()
        .option(RepairManifestsSparkAction.USE_CACHING, useCaching)
        .execute();

    assertThat(table.currentSnapshot()).as("Table must stay empty").isNull();
  }

  @TestTemplate
  public void testRepairNoDuplicatesNoMissingFiles() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    writeRecords(records1);
    table.refresh();

    List<ManifestFile> manifestsBeforeAction = table.currentSnapshot().allManifests(table.io());

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .removeMissingFiles()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.addedManifests().iterator().hasNext()).isFalse();
    assertThat(result.rewrittenManifests().iterator().hasNext()).isFalse();

    table.refresh();
    List<ManifestFile> manifestsAfterAction = table.currentSnapshot().allManifests(table.io());

    assertThat(manifestsBeforeAction).isEqualTo(manifestsAfterAction);
  }

  @TestTemplate
  public void testRemoveMissingFilesNoMissingFiles() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    writeRecords(records1);
    table.refresh();

    List<ManifestFile> manifestsBeforeAction = table.currentSnapshot().allManifests(table.io());

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeMissingFiles()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.addedManifests().iterator().hasNext()).isFalse();
    assertThat(result.rewrittenManifests().iterator().hasNext()).isFalse();

    table.refresh();
    List<ManifestFile> manifestsAfterAction = table.currentSnapshot().allManifests(table.io());

    assertThat(manifestsBeforeAction).isEqualTo(manifestsAfterAction);
  }

  @TestTemplate
  public void testRemoveDuplicatesSmallManifestsNonPartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    writeRecords(records1);
    table.refresh();

    DataFile duplicate = Iterables.getLast(table.currentSnapshot().addedDataFiles(table.io()));
    table.newFastAppend().appendFile(duplicate).commit();

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    Dataset<Row> validateDuplicatesExist = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> hasDuplicates =
        validateDuplicatesExist
            .sort("c1", "c2")
            .as(Encoders.bean(ThreeColumnRecord.class))
            .collectAsList();
    assertThat(hasDuplicates).as("Rows must contain duplicates").isNotEqualTo(expectedRecords);

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    List<Boolean> manifestFileStartsWithDeduped = Lists.newArrayList();
    result
        .addedManifests()
        .forEach(
            manifest -> manifestFileStartsWithDeduped.add(manifest.path().contains("deduped-m")));
    assertThat(result.rewrittenManifests()).as("Action should rewrite 2 manifests").hasSize(2);
    assertThat(result.addedManifests()).as("Action should add 2 manifests").hasSize(2);
    assertManifestsLocation(result.addedManifests());
    assertThat(manifestFileStartsWithDeduped)
        .as("Rewritten files should start with deduped-m")
        .contains(true, true);

    List<Row> finalFiles =
        SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.ENTRIES).collectAsList();
    assertThat(finalFiles).isNotEmpty();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(newManifests).as("Should have 2 manifests after deduping").hasSize(2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> recordsPostDeduping =
        resultDF.sort("c1", "c2").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    assertThat(recordsPostDeduping).as("Rows must match").isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testRemoveDuplicatesSmallManifestsNonPartitionedTableDryRun() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    writeRecords(records1);

    DataFile duplicate = Iterables.getLast(table.currentSnapshot().addedDataFiles(table.io()));
    table.newFastAppend().appendFile(duplicate).commit();

    table.refresh();

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .dryRun()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 2 manifests").hasSize(2);
    assertThat(result.addedManifests()).as("Action should add 2 manifests").hasSize(2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> recordsPostDeduping =
        resultDF.sort("c1", "c2").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    List<ThreeColumnRecord> recordsWithoutDupes = Lists.newArrayList();
    recordsWithoutDupes.addAll(records1);
    // deduplication did not happen due to dryRun()
    assertThat(recordsPostDeduping).as("Rows must match").isNotEqualTo(recordsWithoutDupes);
  }

  @TestTemplate
  public void testRemoveDuplicatesSmallManifestsPartitionedTable() {
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
            new ThreeColumnRecord(3, "EEEEEEEEEE", "EEEE"), // becomes a duplicate
            new ThreeColumnRecord(3, "FFFFFFFFFF", "FFFF"));
    writeRecords(records3);

    List<ThreeColumnRecord> records4 =
        Lists.newArrayList(
            new ThreeColumnRecord(4, "GGGGGGGGGG", "GGGG"),
            new ThreeColumnRecord(4, "HHHHHHHHHG", "HHHH"));
    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    writeRecords(records4);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).as("Should have 4 manifests before rewrite").hasSize(4);

    DataFile duplicate = Iterables.getLast(table.currentSnapshot().addedDataFiles(table.io()));

    table.newFastAppend().appendFile(duplicate).commit();
    table.refresh();

    List<ThreeColumnRecord> validateRecordsContainDuplicates =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation)
            .sort("c1", "c2")
            .as(Encoders.bean(ThreeColumnRecord.class))
            .collectAsList();

    assertThat(expectedRecords).isNotEqualTo(validateRecordsContainDuplicates.size());

    SparkActions actions = SparkActions.get();

    // we will expect to have 2 manifests with 4 entries in each after rewrite
    long manifestEntrySizeBytes = computeManifestEntrySizeBytes(manifests);
    long targetManifestSizeBytes = (long) (1.05 * 4 * manifestEntrySizeBytes);

    table
        .updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, String.valueOf(targetManifestSizeBytes))
        .commit();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 2 manifests").hasSize(2);
    assertThat(result.addedManifests()).as("Action should add 2 manifests").hasSize(2);
    assertManifestsLocation(result.addedManifests());

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());

    assertThat(newManifests)
        .as("Should have 5 manifests after repairing: 3 with no problems plus 2 rewritten")
        .hasSize(5);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.sort("c1", "c2").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    assertThat(actualRecords).as("Rows must match").isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testRemoveDuplicatesDuplicateFilesSameManifest() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    DataFile datafile = newDataFile(table, TestHelpers.Row.of(new Object[] {null}));

    // append the same file three times in the same manifest
    List<DataFile> dataFiles = Lists.newArrayList(datafile, datafile, datafile);
    ManifestFile appendManifestSameFile = writeManifest(table, dataFiles);
    table.newFastAppend().appendManifest(appendManifestSameFile).commit();

    // cause a duplicate in another file
    ManifestFile appendManifestDuplicate =
        writeManifest(
            table,
            Lists.newArrayList(
                datafile, newDataFile(table, TestHelpers.Row.of(new Object[] {null}))));
    table.newFastAppend().appendManifest(appendManifestDuplicate).commit();

    table.refresh();
    // ensure test is setup properly
    List<Integer> addedFilesCheck = Lists.newArrayList();
    table
        .snapshots()
        .forEach(
            snapshot -> addedFilesCheck.add(Iterables.size(snapshot.addedDataFiles(table.io()))));
    assertThat(addedFilesCheck).isEqualTo(Lists.newArrayList(3, 2));

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 1 manifests").hasSize(2);
    assertThat(result.addedManifests()).as("Action should add 1 manifests").hasSize(2);

    // we end up with two manifests:
    // the first manifest only contains duplicates, so it was removed entirely
    // the second manifest gets rewritten as it contained an additional row beside the duplicate
    // that was also in the first manifest
    // a new manifest containing the duplicate
    // so each data manifest should have 1 existing file only
    assertThat(table.currentSnapshot().dataManifests(table.io()).get(0).existingFilesCount())
        .isEqualTo(1);
    assertThat(table.currentSnapshot().dataManifests(table.io()).get(0).addedFilesCount())
        .isEqualTo(0);
    assertThat(table.currentSnapshot().dataManifests(table.io()).get(0).deletedFilesCount())
        .isEqualTo(0);

    assertThat(table.currentSnapshot().dataManifests(table.io()).get(1).existingFilesCount())
        .isEqualTo(1);
    assertThat(table.currentSnapshot().dataManifests(table.io()).get(1).addedFilesCount())
        .isEqualTo(0);
    assertThat(table.currentSnapshot().dataManifests(table.io()).get(1).deletedFilesCount())
        .isEqualTo(0);

    assertThat(table.currentSnapshot().dataManifests(table.io()).size()).isEqualTo(2);
  }

  @TestTemplate
  public void testRemoveDuplicatesLargeManifestsEvolvedUnpartitionedV1Table() throws IOException {
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

    // insert 40 duplicates
    List<DataFile> duplicates = dataFiles.stream().limit(20).collect(Collectors.toList());
    ManifestFile appendDuplicates1x = writeManifest(table, duplicates);
    table.newFastAppend().appendManifest(appendDuplicates1x).commit();
    ManifestFile appendDuplicates2x = writeManifest(table, duplicates);
    table.newFastAppend().appendManifest(appendDuplicates2x).commit();

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .option(RepairManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    // original manifest, duplicate manifest, second duplicate manifest
    assertThat(result.rewrittenManifests()).hasSize(3);

    assertThat(result.addedManifests()).hasSize(4);
    assertManifestsLocation(result.addedManifests());

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSizeGreaterThanOrEqualTo(4);
  }
  //
  @TestTemplate
  public void testRemoveDuplicatesSmallDeleteManifestsNonPartitionedTable() throws IOException {
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

    table.refresh();

    DataFile duplicateDataFile =
        Iterables.getLast(table.currentSnapshot().addedDataFiles(table.io()));
    table.newFastAppend().appendFile(duplicateDataFile).commit();

    // commit a position delete file to remove records where c1 = 1 OR c1 = 2
    List<Pair<CharSequence, Long>> posDeletes = generatePosDeletes("c1 = 1 OR c1 = 2");
    Pair<DeleteFile, CharSequenceSet> posDeleteWriteResult = writePosDeletes(table, posDeletes);
    table
        .newRowDelta()
        .addDeletes(posDeleteWriteResult.first())
        .validateDataFilesExist(posDeleteWriteResult.second())
        .commit();

    // duplicate position delete file
    table
        .newRowDelta()
        .addDeletes(posDeleteWriteResult.first())
        .validateDataFilesExist(posDeleteWriteResult.second())
        .commit();

    // commit an equality delete file to remove all records where c1 = 3
    DeleteFile eqDeleteFile = writeEqDeletes(table, "c1", 3, 5);
    table.newRowDelta().addDeletes(eqDeleteFile).commit();

    // add a file that would get filtered out if the equality delete is duplicated _after_ this
    // commit
    writeRecords(Lists.newArrayList(new ThreeColumnRecord(3, "SHOULD_NOT_BE_FILTERED", "CCCC")));

    // duplicate the equality delete file
    table.newRowDelta().addDeletes(eqDeleteFile).commit();

    // a duplicate file after all the delete files
    table.newFastAppend().appendFile(duplicateDataFile).commit();

    table.refresh();

    List<ManifestFile> originalManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(originalManifests).hasSize(8);

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .option(RepairManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).hasSize(7);
    assertThat(result.addedManifests()).hasSize(4);
    assertManifestsLocation(result.addedManifests());

    List<ThreeColumnRecord> expectedRecords =
        Lists.newArrayList(
            new ThreeColumnRecord(3, "SHOULD_NOT_BE_FILTERED", "CCCC"),
            new ThreeColumnRecord(4, "DDDDDDDDDD", "DDDD"));
    assertThat(actualRecords()).isEqualTo(expectedRecords);

    // All data file manifests have been replaced, even the duplicated one committed after the
    // delete files
    // the min sequence number should be the minimum of the file containing the dupe, which is 1L
    // the last datafile is the one that should not be filtered of 6L which is unchanged (never
    // rewritten)
    List<Long> dataManifests =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .map(ManifestFile::minSequenceNumber)
            .collect(Collectors.toList());
    assertThat(dataManifests).isEqualTo(Lists.newArrayList(1L, 1L, 6L));
  }

  @TestTemplate
  public void testRemoveDuplicatesDeleteManifestsPartitionedTable() throws IOException {
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

    // duplicate the data file
    DataFile duplicateDataFile =
        Iterables.getLast(table.currentSnapshot().addedDataFiles(table.io()));
    table.newFastAppend().appendFile(duplicateDataFile).commit();

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

    // duplicate the datafile a second time after all the deletes
    table.newFastAppend().appendFile(duplicateDataFile).commit();

    // if the wrong equality delete is kept based on sequence number, this would get filtered out
    writeRecords(Lists.newArrayList(new ThreeColumnRecord(3, "SHOULD_NOT_FILTER", "CCCC")));

    // duplicate one of the equality delete files
    table.newRowDelta().addDeletes(eqDeleteFile1).commit();

    // duplicate one of the equality delete files again
    table.newRowDelta().addDeletes(eqDeleteFile1).commit();

    // duplicate one of the position delete files
    table
        .newRowDelta()
        .addDeletes(positionDeleteWriteResult2.first())
        .validateDataFilesExist(positionDeleteWriteResult2.second())
        .commit();

    // the table must have 4 data manifest (two of which ahve dupes) and 7 delete manifests (3 of
    // which have dupes)
    List<ManifestFile> originalManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(originalManifests).hasSize(11);

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .option(RepairManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).hasSize(8);
    assertThat(result.addedManifests()).hasSize(6);
    assertManifestsLocation(result.addedManifests());

    // there were 6 delete manifests but now just 4 since the two duplicates have been removed
    List<ManifestFile> deleteManifests = table.currentSnapshot().deleteManifests(table.io());
    assertThat(deleteManifests).hasSize(4);

    // the table must produce expected records after the rewrite
    List<ThreeColumnRecord> expectedRecords =
        Lists.newArrayList(
            new ThreeColumnRecord(3, "SHOULD_NOT_FILTER", "CCCC"),
            new ThreeColumnRecord(5, "EEEEEEEEEE", "EEEE"));
    assertThat(actualRecords()).isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testNoDuplicatesIfDataFileAddedDeletedAddedAgain() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // add the file
    // delete the file
    // add the file again
    // should not count as a duplicate
    List<DataFile> targetDatafile =
        Lists.newArrayList(newDataFile(table, TestHelpers.Row.of(new Object[] {null})));
    ManifestFile appendManifest = writeManifest(table, targetDatafile);
    table.newFastAppend().appendManifest(appendManifest).commit();
    table.newDelete().deleteFile(targetDatafile.get(0)).commit();
    table.newFastAppend().appendFile(targetDatafile.get(0)).commit();

    table.refresh();

    // 3 snapshots: add, delete, add again
    // but only two manifests, the delete call removes the first one
    assertThat(Iterables.size(table.snapshots())).isEqualTo(3);
    List<ManifestFile> originalManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(originalManifests).hasSize(2);

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 0 manifests").hasSize(0);
    assertThat(result.addedManifests()).as("Action should add 0 manifests").hasSize(0);
  }

  @TestTemplate
  public void testRewriteDuplicatesAfterDeletesIfReal() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // add the file
    // delete the file
    // add the file again
    // should not count as a duplicate
    List<DataFile> targetDatafile =
        Lists.newArrayList(newDataFile(table, TestHelpers.Row.of(new Object[] {null})));
    ManifestFile appendManifest = writeManifest(table, targetDatafile);
    table.newFastAppend().appendManifest(appendManifest).commit();
    table.newDelete().deleteFile(targetDatafile.get(0)).commit();
    table.newFastAppend().appendFile(targetDatafile.get(0)).commit();
    // duplicate the data file again
    table.newFastAppend().appendFile(targetDatafile.get(0)).commit();

    table.refresh();

    assertThat(Iterables.size(table.snapshots())).isEqualTo(4);
    List<ManifestFile> originalManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(originalManifests).hasSize(3);

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 2 manifests").hasSize(2);
    assertThat(result.addedManifests()).as("Action should add 1 manifests").hasSize(1);
  }

  @TestTemplate
  public void testMissingFilesRemoved() {
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

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).as("Should have 2 manifests before rewrite").hasSize(2);

    DataFile dataFileToDelete =
        Iterables.getLast(table.currentSnapshot().addedDataFiles(table.io()));

    // this file contains this row ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD"));
    File fileToDelete = new File(dataFileToDelete.path().toString().substring(5));

    if (fileToDelete.exists()) {
      if (!fileToDelete.delete()) {
        throw new RuntimeException("could not delete file during test");
      }
    } else {
      throw new RuntimeException("file was not created during test");
    }

    table.refresh();

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeMissingFiles()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 1 manifests").hasSize(1);
    assertThat(result.addedManifests()).as("Action should add 0 manifests").hasSize(0);
    assertThat(result.duplicateFilesRemoved()).isEqualTo(0L);
    assertThat(result.missingFilesRemoved()).isEqualTo(1);
    assertManifestsLocation(result.addedManifests());

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());

    assertThat(newManifests).as("Should have 2 manifests after repairing").hasSize(2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.sort("c1", "c2").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.add(new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"));
    // is missing row ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD"))
    assertThat(actualRecords).isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testMissingFilesRemovedDryRun() {
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

    table.refresh();

    DataFile dataFileToDelete =
        Iterables.getLast(table.currentSnapshot().addedDataFiles(table.io()));

    // this file contains this row ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD"));
    File fileToDelete = new File(dataFileToDelete.path().toString().substring(5));

    if (fileToDelete.exists()) {
      if (!fileToDelete.delete()) {
        throw new RuntimeException("could not delete file during test");
      }
    } else {
      throw new RuntimeException("file was not created during test");
    }

    table.refresh();

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeMissingFiles()
            .dryRun()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 1 manifests").hasSize(1);
    assertThat(result.addedManifests()).as("Action should add 0 manifests").hasSize(0);
    assertThat(result.duplicateFilesRemoved()).isEqualTo(0L);
    assertThat(result.missingFilesRemoved()).isEqualTo(1);

    table.refresh();

    // since the file was never removed an exception is thrown when trying to read the table
    assertThatThrownBy(() -> spark.read().format("iceberg").load(tableLocation).collectAsList())
        .isInstanceOf(org.apache.spark.SparkException.class);
  }

  @TestTemplate
  public void testMissingFilesRemovedDuplicates() {
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

    List<ThreeColumnRecord> records3 =
        Lists.newArrayList(new ThreeColumnRecord(3, "EEEEEEEEEE", "EEEE"));

    writeRecords(records3);

    List<ThreeColumnRecord> records4 =
        Lists.newArrayList(
            new ThreeColumnRecord(3, "FFFFFFFFFF", "FFFF"), // this gets deleted
            new ThreeColumnRecord(4, "GGGGGGGGGG", "GGGG")); // this does not

    writeRecords(records4);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).as("Should have 4 manifests before rewrite").hasSize(4);

    List<DataFile> dataFiles = Lists.newArrayList();
    table
        .snapshots()
        .forEach(snapshot -> snapshot.addedDataFiles(table.io()).forEach(dataFiles::add));

    DataFile duplicateFile = dataFiles.get(1);
    table.newFastAppend().appendFile(duplicateFile).commit();
    table.newFastAppend().appendFile(duplicateFile).commit();
    table.newFastAppend().appendFile(duplicateFile).commit();

    DataFile dataFileToDelete = dataFiles.get(dataFiles.size() - 2);

    assertThat(duplicateFile.path().toString()).isNotEqualTo(dataFileToDelete.path().toString());

    // this file contains this row ThreeColumnRecord(3, "FFFFFFFFFF", "FFFF");
    File fileToDelete = new File(dataFileToDelete.path().toString().substring(5));

    // duplicate append the file that will be missing twice
    table.newFastAppend().appendFile(dataFileToDelete).commit();
    table.newFastAppend().appendFile(dataFileToDelete).commit();

    table.refresh();

    if (fileToDelete.exists()) {
      if (!fileToDelete.delete()) {
        throw new RuntimeException("could not delete file during test");
      }
    } else {
      throw new RuntimeException("file was not created during test");
    }

    table.refresh();

    SparkActions actions = SparkActions.get();

    RepairManifests.Result result =
        actions
            .repairManifests(table)
            .removeDuplicateCommittedFiles()
            .removeMissingFiles()
            .option(RewriteManifestsSparkAction.USE_CACHING, useCaching)
            .execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 7 manifests").hasSize(7);
    assertThat(result.addedManifests()).as("Action should add 3 manifests").hasSize(3);
    assertThat(result.duplicateFilesRemoved()).isEqualTo(5L);
    assertThat(result.missingFilesRemoved()).isEqualTo(3L);

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());

    assertThat(newManifests).as("Should have 5 manifests after repairing").hasSize(5);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.sort("c1", "c2").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.add(new ThreeColumnRecord(3, "EEEEEEEEEE", "EEEE"));
    expectedRecords.add(new ThreeColumnRecord(4, "GGGGGGGGGG", "GGGG"));
    // is missing row  ThreeColumnRecord(3, "FFFFFFFFFF", "FFFF")
    assertThat(actualRecords).isEqualTo(expectedRecords);
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
