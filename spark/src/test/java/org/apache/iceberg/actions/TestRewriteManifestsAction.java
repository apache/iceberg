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

package org.apache.iceberg.actions;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Conversions.fromByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public abstract class TestRewriteManifestsAction extends SparkTestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );

  @Parameterized.Parameters(name = "snapshotIdInheritanceEnabled = {0}")
  public static Object[] parameters() {
    return new Object[] { "true", "false" };
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private final String snapshotIdInheritanceEnabled;
  private String tableLocation = null;

  public TestRewriteManifestsAction(String snapshotIdInheritanceEnabled) {
    this.snapshotIdInheritanceEnabled = snapshotIdInheritanceEnabled;
  }

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testRewriteManifestsEmptyTable() throws IOException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    Assert.assertNull("Table must be empty", table.currentSnapshot());

    Actions actions = Actions.forTable(table);

    actions.rewriteManifests()
        .rewriteIf(manifest -> true)
        .stagingLocation(temp.newFolder().toString())
        .execute();

    Assert.assertNull("Table must stay empty", table.currentSnapshot());
  }

  @Test
  public void testRewriteSmallManifestsNonPartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "AAAA"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
        new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests before rewrite", 2, manifests.size());

    Actions actions = Actions.forTable(table);

    RewriteManifestsActionResult result = actions.rewriteManifests()
        .rewriteIf(manifest -> true)
        .execute();

    Assert.assertEquals("Action should rewrite 2 manifests", 2, result.deletedManifests().size());
    Assert.assertEquals("Action should add 1 manifests", 1, result.addedManifests().size());

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 1 manifests after rewrite", 1, newManifests.size());

    Assert.assertEquals(4, (long) newManifests.get(0).existingFilesCount());
    Assert.assertFalse(newManifests.get(0).hasAddedFiles());
    Assert.assertFalse(newManifests.get(0).hasDeletedFiles());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteSmallManifestsPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "AAAA"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
        new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    List<ThreeColumnRecord> records3 = Lists.newArrayList(
        new ThreeColumnRecord(3, "EEEEEEEEEE", "EEEE"),
        new ThreeColumnRecord(3, "FFFFFFFFFF", "FFFF")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
        new ThreeColumnRecord(4, "GGGGGGGGGG", "GGGG"),
        new ThreeColumnRecord(4, "HHHHHHHHHG", "HHHH")
    );
    writeRecords(records4);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 4 manifests before rewrite", 4, manifests.size());

    Actions actions = Actions.forTable(table);

    // we will expect to have 2 manifests with 4 entries in each after rewrite
    long manifestEntrySizeBytes = computeManifestEntrySizeBytes(manifests);
    long targetManifestSizeBytes = (long) (1.05 * 4 * manifestEntrySizeBytes);

    table.updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, String.valueOf(targetManifestSizeBytes))
        .commit();

    RewriteManifestsActionResult result = actions.rewriteManifests()
        .rewriteIf(manifest -> true)
        .execute();

    Assert.assertEquals("Action should rewrite 4 manifests", 4, result.deletedManifests().size());
    Assert.assertEquals("Action should add 2 manifests", 2, result.addedManifests().size());

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests after rewrite", 2, newManifests.size());

    Assert.assertEquals(4, (long) newManifests.get(0).existingFilesCount());
    Assert.assertFalse(newManifests.get(0).hasAddedFiles());
    Assert.assertFalse(newManifests.get(0).hasDeletedFiles());

    Assert.assertEquals(4, (long) newManifests.get(1).existingFilesCount());
    Assert.assertFalse(newManifests.get(1).hasAddedFiles());
    Assert.assertFalse(newManifests.get(1).hasDeletedFiles());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteImportedManifests() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c3")
        .build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "AAAA"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    File parquetTableDir = temp.newFolder("parquet_table");
    String parquetTableLocation = parquetTableDir.toURI().toString();

    try {
      Dataset<Row> inputDF = spark.createDataFrame(records, ThreeColumnRecord.class);
      inputDF.select("c1", "c2", "c3")
          .write()
          .format("parquet")
          .mode("overwrite")
          .option("path", parquetTableLocation)
          .partitionBy("c3")
          .saveAsTable("parquet_table");

      File stagingDir = temp.newFolder("staging-dir");
      SparkTableUtil.importSparkTable(spark, new TableIdentifier("parquet_table"), table, stagingDir.toString());

      Snapshot snapshot = table.currentSnapshot();

      Actions actions = Actions.forTable(table);

      RewriteManifestsActionResult result = actions.rewriteManifests()
          .rewriteIf(manifest -> true)
          .stagingLocation(temp.newFolder().toString())
          .execute();

      Assert.assertEquals("Action should rewrite all manifests", snapshot.allManifests(), result.deletedManifests());
      Assert.assertEquals("Action should add 1 manifest", 1, result.addedManifests().size());

    } finally {
      spark.sql("DROP TABLE parquet_table");
    }
  }

  @Test
  public void testRewriteLargeManifestsPartitionedTable() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c3")
        .build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // all records belong to the same partition
    List<ThreeColumnRecord> records = Lists.newArrayList();
    for (int i = 0; i < 50; i++) {
      records.add(new ThreeColumnRecord(i, String.valueOf(i), "0"));
    }
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    // repartition to create separate files
    writeDF(df.repartition(50, df.col("c1")));

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 1 manifests before rewrite", 1, manifests.size());

    // set the target manifest size to a small value to force splitting records into multiple files
    table.updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, String.valueOf(manifests.get(0).length() / 2))
        .commit();

    Actions actions = Actions.forTable(table);

    RewriteManifestsActionResult result = actions.rewriteManifests()
        .rewriteIf(manifest -> true)
        .stagingLocation(temp.newFolder().toString())
        .execute();

    Assert.assertEquals("Action should rewrite 1 manifest", 1, result.deletedManifests().size());
    Assert.assertEquals("Action should add 2 manifests", 2, result.addedManifests().size());

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests after rewrite", 2, newManifests.size());

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testRewriteManifestsWithPredicate() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, snapshotIdInheritanceEnabled);
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "AAAA"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
        new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests before rewrite", 2, manifests.size());

    Actions actions = Actions.forTable(table);

    // rewrite only the first manifest without caching
    RewriteManifestsActionResult result = actions.rewriteManifests()
        .rewriteIf(manifest -> manifest.path().equals(manifests.get(0).path()))
        .stagingLocation(temp.newFolder().toString())
        .useCaching(false)
        .execute();

    Assert.assertEquals("Action should rewrite 1 manifest", 1, result.deletedManifests().size());
    Assert.assertEquals("Action should add 1 manifests", 1, result.addedManifests().size());

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests after rewrite", 2, newManifests.size());

    Assert.assertFalse("First manifest must be rewritten", newManifests.contains(manifests.get(0)));
    Assert.assertTrue("Second manifest must not be rewritten", newManifests.contains(manifests.get(1)));

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteManifestRepairMetricsUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"),
            new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
            new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests before rewrite", 2, manifests.size());

    List<SparkDataFile> filesBefore = getManifestEntries(table);
    assertLowerBounds(filesBefore, 1, Optional.empty(), 4,
            "Should have no lower bounds before repair");
    assertUpperBounds(filesBefore, 1, Optional.empty(), 4,
            "Should have no upper bounds before repair");
    table.updateProperties().set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(16)").commit();

    Actions actions = Actions.forTable(table);

    RewriteManifestsActionResult result = actions.rewriteManifests()
            .rewriteIf(manifest -> true)
            .repair(RewriteManifests.RepairMode.REPAIR_ENTRIES)
            .execute();

    Assert.assertEquals("Action should rewrite 2 manifests", 2, result.deletedManifests().size());
    Assert.assertEquals("Action should add 1 manifests", 1, result.addedManifests().size());
    Assert.assertEquals("Action should have repaired 1 manifest", 1, result.repairedManifests().size());
    Set<String> expectedRepairedFields = Sets.newHashSet(
        "lower_bounds", "upper_bounds", "null_value_counts", "value_counts");
    assertRepairedFields(result.repairedManifests(), expectedRepairedFields);

    table.refresh();
    List<ManifestFile> newManifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 1 manifests after rewrite", 1, newManifests.size());

    Assert.assertEquals(4, (long) newManifests.get(0).existingFilesCount());
    Assert.assertFalse(newManifests.get(0).hasAddedFiles());
    Assert.assertFalse(newManifests.get(0).hasDeletedFiles());

    List<SparkDataFile> files = getManifestEntries(table);
    assertLowerBounds(files, 1, Optional.of(1), 2,
            "Should have correct lower bounds after repair");
    assertLowerBounds(files, 1, Optional.of(2), 2,
            "Should have correct lower bounds after repair");
    assertUpperBounds(files, 1, Optional.of(1), 2,
            "Should have correct lower bounds after repair");
    assertUpperBounds(files, 1, Optional.of(2), 2,
            "Should have correct lower bounds after repair");


    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
            .as(Encoders.bean(ThreeColumnRecord.class))
            .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteManifestsRepairMetricsPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
            .identity("c1")
            .truncate("c2", 2)
            .build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"),
            new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
            new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    List<ThreeColumnRecord> records3 = Lists.newArrayList(
            new ThreeColumnRecord(3, "EEEEEEEEEE", "EEEE"),
            new ThreeColumnRecord(3, "FFFFFFFFFF", "FFFF")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
            new ThreeColumnRecord(4, "GGGGGGGGGG", "GGGG"),
            new ThreeColumnRecord(4, "HHHHHHHHHG", "HHHH")
    );
    writeRecords(records4);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 4 manifests before rewrite", 4, manifests.size());
    List<SparkDataFile> filesBefore = getManifestEntries(table);
    assertLowerBounds(filesBefore, 1, Optional.empty(), 8,
            "Should have no lower bounds before repair");
    assertUpperBounds(filesBefore, 1, Optional.empty(), 8,
            "Should have no upper bounds before repair");

    Actions actions = Actions.forTable(table);

    // we will expect to have 2 manifests with 4 entries in each after rewrite
    // we will expect metrics after the rewrite
    long manifestEntrySizeBytes = computeManifestEntrySizeBytes(manifests);
    long targetManifestSizeBytes = (long) (1.05 * 4 * manifestEntrySizeBytes);

    table.updateProperties()
            .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, String.valueOf(targetManifestSizeBytes))
            .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(16)")
            .commit();

    RewriteManifestsActionResult result = actions.rewriteManifests()
            .rewriteIf(manifest -> true)
            .repair(RewriteManifests.RepairMode.REPAIR_ENTRIES)
            .execute();

    Assert.assertEquals("Action should rewrite 4 manifests", 4, result.deletedManifests().size());
    Assert.assertEquals("Action should add 2 manifests", 2, result.addedManifests().size());
    Assert.assertEquals("Action should have repaired 2 manifest", 2, result.repairedManifests().size());
    Set<String> expectedRepairedFields = Sets.newHashSet(
        "lower_bounds", "upper_bounds", "null_value_counts", "value_counts");
    assertRepairedFields(result.repairedManifests(), expectedRepairedFields);

    Assert.assertEquals("Action should have repaired metrics",
            expectedRepairedFields, result.repairedManifests().get(1).fields());
    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests after rewrite", 2, newManifests.size());

    Assert.assertEquals(4, (long) newManifests.get(0).existingFilesCount());
    Assert.assertFalse(newManifests.get(0).hasAddedFiles());
    Assert.assertFalse(newManifests.get(0).hasDeletedFiles());

    Assert.assertEquals(4, (long) newManifests.get(1).existingFilesCount());
    Assert.assertFalse(newManifests.get(1).hasAddedFiles());
    Assert.assertFalse(newManifests.get(1).hasDeletedFiles());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
            .as(Encoders.bean(ThreeColumnRecord.class))
            .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRepairFileSizeAndRecordCountUnpartitioned() throws Exception {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<Record> records1 = Lists.newArrayList(
            createGenericRecord(1, null, "AAAA"),
            createGenericRecord(1, "BBBBBBBBBB", "BBBB")
    );
    List<Record> records2 = Lists.newArrayList(
            createGenericRecord(2, "CCCCCCCCCC", "CCCC"),
            createGenericRecord(2, "DDDDDDDDDD", "DDDD")
    );
    File f1 = writeToFile(records1, FileFormat.PARQUET);
    File f2 = writeToFile(records2, FileFormat.PARQUET);

    // DataFiles have wrong file size and record count
    DataFile df1 = DataFiles.builder(spec)
            .withPath(f1.getPath())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withFormat(FileFormat.PARQUET)
            .build();
    DataFile df2 = DataFiles.builder(spec)
            .withPath(f2.getPath())
            .withFileSizeInBytes(20)
            .withRecordCount(2)
            .withFormat(FileFormat.PARQUET)
            .build();

    table.newFastAppend().appendFile(df1).commit();
    table.newFastAppend().appendFile(df2).commit();
    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 2 manifests before rewrite", 2, manifests.size());
    List<SparkDataFile> entriesBefore = getManifestEntries(table);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
            10, 1);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
            20, 2);

    Actions actions = Actions.forTable(table);
    RewriteManifestsActionResult result = actions.rewriteManifests()
            .rewriteIf(manifest -> true)
            .repair(RewriteManifests.RepairMode.REPAIR_ENTRIES)
            .execute();

    Assert.assertEquals("Action should rewrite 2 manifests", 2, result.deletedManifests().size());
    Assert.assertEquals("Action should add 1 manifests", 1, result.addedManifests().size());
    Assert.assertEquals("Action should have repaired 1 manifest", 1, result.repairedManifests().size());
    Set<String> expectedRepairedFields = Sets.newHashSet(
            "lower_bounds", "upper_bounds",
            "null_value_counts", "value_counts", "nan_value_counts",
            "column_sizes", "file_size_in_bytes", "record_count");
    assertRepairedFields(result.repairedManifests(), expectedRepairedFields);

    List<SparkDataFile> entriesAfter = getManifestEntries(table);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
            entriesAfter, f1.length(), 2);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
            entriesAfter, f2.length(), 2);

    List<ThreeColumnRecord> expectedRecords = Stream.concat(records1.stream(), records2.stream())
            .map(r -> new ThreeColumnRecord(
                    (Integer) r.getField("c1"),
                    (String) r.getField("c2"),
                    (String) r.getField("c3")))
            .collect(Collectors.toList());
    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
            .as(Encoders.bean(ThreeColumnRecord.class))
            .collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRepairFileSizeAndRecordCountPartitioned() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
            .identity("c1")
            .build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<Record> records1 = Lists.newArrayList(
            createGenericRecord(1, null, "AAAA"),
            createGenericRecord(1, "BBBBBBBBBB", "BBBB")
    );
    List<Record> records2 = Lists.newArrayList(
            createGenericRecord(2, "CCCCCCCCCC", "CCCC"),
            createGenericRecord(2, "DDDDDDDDDD", "DDDD")
    );
    List<Record> records3 = Lists.newArrayList(
            createGenericRecord(3, "EEEEEEEEEE", "EEEE"),
            createGenericRecord(3, "FFFFFFFFFF", "FFFF")
    );
    List<Record> records4 = Lists.newArrayList(
            createGenericRecord(4, "GGGGGGGGGG", "GGGG"),
            createGenericRecord(4, "HHHHHHHHHG", "HHHH")
    );
    File f1 = writeToFile(records1, FileFormat.PARQUET);
    File f2 = writeToFile(records2, FileFormat.PARQUET);
    File f3 = writeToFile(records3, FileFormat.PARQUET);
    File f4 = writeToFile(records4, FileFormat.PARQUET);

    // DataFiles have wrong file size and record count
    DataFile df1 = DataFiles.builder(spec)
            .withPath(f1.getPath())
            .withFileSizeInBytes(10)
            .withPartitionPath("c1=1")
            .withRecordCount(1)
            .withFormat(FileFormat.PARQUET)
            .build();
    DataFile df2 = DataFiles.builder(spec)
            .withPath(f2.getPath())
            .withFileSizeInBytes(20)
            .withPartitionPath("c1=2")
            .withRecordCount(2)
            .withFormat(FileFormat.PARQUET)
            .build();
    DataFile df3 = DataFiles.builder(spec)
            .withPath(f3.getPath())
            .withFileSizeInBytes(30)
            .withPartitionPath("c1=3")
            .withRecordCount(3)
            .withFormat(FileFormat.PARQUET)
            .build();
    DataFile df4 = DataFiles.builder(spec)
            .withPath(f4.getPath())
            .withFileSizeInBytes(40)
            .withPartitionPath("c1=4")
            .withRecordCount(4)
            .withFormat(FileFormat.PARQUET)
            .build();

    table.newFastAppend().appendFile(df1).commit();
    table.newFastAppend().appendFile(df2).commit();
    table.newFastAppend().appendFile(df3).commit();
    table.newFastAppend().appendFile(df4).commit();
    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();
    Assert.assertEquals("Should have 4 manifests before rewrite", 4, manifests.size());
    List<SparkDataFile> entriesBefore = getManifestEntries(table);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
            10, 1);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
            20, 2);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
            30, 3);
    assertFileSizeRecordCount("Should have initial file size and record count before repair", entriesBefore,
            40, 4);

    // we will expect to have 2 manifests with 2 entries in each after rewrite
    long manifestEntrySizeBytes = computeManifestEntrySizeBytes(manifests);
    long targetManifestSizeBytes = (long) (1.05 * 2 * manifestEntrySizeBytes);

    table.updateProperties()
            .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, String.valueOf(targetManifestSizeBytes))
            .commit();

    Actions actions = Actions.forTable(table);
    RewriteManifestsActionResult result = actions.rewriteManifests()
            .rewriteIf(manifest -> true)
            .repair(RewriteManifests.RepairMode.REPAIR_ENTRIES)
            .execute();

    Assert.assertEquals("Action should rewrite 4 manifests", 4, result.deletedManifests().size());
    Assert.assertEquals("Action should add 2 manifests", 2, result.addedManifests().size());
    Assert.assertEquals("Action should have repaired 2 manifest", 2, result.repairedManifests().size());
    Set<String> expectedRepairedFields = Sets.newHashSet(
            "lower_bounds", "upper_bounds",
            "null_value_counts", "value_counts", "nan_value_counts",
            "column_sizes", "file_size_in_bytes", "record_count");
    assertRepairedFields(result.repairedManifests(), expectedRepairedFields);

    List<SparkDataFile> entriesAfter = getManifestEntries(table);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
            entriesAfter, f1.length(), 2);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
            entriesAfter, f2.length(), 2);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
            entriesAfter, f3.length(), 2);
    assertFileSizeRecordCount("Should have fixed file size and record count after repair",
            entriesAfter, f4.length(), 2);

    List<ThreeColumnRecord> expectedRecords = Stream.concat(
            Stream.concat(records1.stream(), records2.stream()),
            Stream.concat(records3.stream(), records4.stream()))
            .map(r -> new ThreeColumnRecord(
                    (Integer) r.getField("c1"),
                    (String) r.getField("c2"),
                    (String) r.getField("c3")))
            .collect(Collectors.toList());
    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
            .as(Encoders.bean(ThreeColumnRecord.class))
            .collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  private void writeRecords(List<ThreeColumnRecord> records) {
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    writeDF(df);
  }

  private void writeDF(Dataset<Row> df) {
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
  }

  private Record createGenericRecord(int c1, String c2, String c3) {
    GenericRecord result = GenericRecord.create(SCHEMA);
    result.setField("c1", c1);
    result.setField("c2", c2);
    result.setField("c3", c3);
    return result;
  }

  private File writeToFile(List<Record> records, FileFormat fileFormat) throws IOException {
    File file = temp.newFile();
    Assert.assertTrue(file.delete());

    GenericAppenderFactory factory = new GenericAppenderFactory(SCHEMA);
    try (FileAppender<Record> appender = factory.newAppender(Files.localOutput(file), fileFormat)) {
      appender.addAll(records);
    }
    return file;
  }

  private long computeManifestEntrySizeBytes(List<ManifestFile> manifests) {
    long totalSize = 0L;
    int numEntries = 0;

    for (ManifestFile manifest : manifests) {
      totalSize += manifest.length();
      numEntries += manifest.addedFilesCount() + manifest.existingFilesCount() + manifest.deletedFilesCount();
    }

    return totalSize / numEntries;
  }

  protected List<SparkDataFile> getManifestEntries(Table table) {
    Dataset<Row> df = spark.read().format("iceberg").load(table.name() + "#entries");
    StructType sparkType = (StructType) df.schema().apply("data_file").dataType();
    Types.StructType dataFileType = DataFile.getType(table.spec().partitionType());
    List<Row> rows = df.collectAsList();
    return rows.stream().map(r -> {
      SparkDataFile wrapper = new SparkDataFile(dataFileType, sparkType);
      return wrapper.wrap(r.getStruct(3));
    }).collect(Collectors.toList());
  }

  protected void assertLowerBounds(List<SparkDataFile> manifestEntries,
                                  int fieldId,
                                  Optional<Integer> expectedBound, int expectedCount,
                                  String message) {
    List<Integer> lowerBounds = manifestEntries.stream().map(f ->
            (Integer) fromByteBuffer(Types.IntegerType.get(), f.lowerBounds().get(fieldId)))
            .collect(Collectors.toList());
    if (expectedBound.isPresent()) {
      Assert.assertEquals(message, expectedCount,
              lowerBounds.stream().filter(b -> b == expectedBound.get()).count());
    } else {
      Assert.assertEquals(message, expectedCount,
              lowerBounds.stream().filter(b -> b == null).count());
    }
  }

  private void assertUpperBounds(List<SparkDataFile> manifestEntries, int fieldId,
                                   Optional<Integer> expectedBound, int expectedCount,
                                   String message) {
    List<Integer> upperBounds = manifestEntries.stream().map(f ->
            (Integer) fromByteBuffer(Types.IntegerType.get(), f.upperBounds().get(fieldId)))
            .collect(Collectors.toList());
    if (expectedBound.isPresent()) {
      Assert.assertEquals(message, expectedCount,
              upperBounds.stream().filter(b -> b == expectedBound.get()).count());
    } else {
      Assert.assertEquals(message, expectedCount,
              upperBounds.stream().filter(b -> b == null).count());
    }
  }

  private void assertFileSizeRecordCount(String message,
                                         List<SparkDataFile> manifestEntries,
                                         long fileSize,
                                         long recordCount) {
    Assert.assertTrue(manifestEntries.stream().filter(mf ->
            mf.fileSizeInBytes() == fileSize && mf.recordCount() == recordCount).count() > 0);
  }

  private void assertRepairedFields(List<RewriteManifests.Result.RepairedManifest> repairedManifests,
                                    Set<String> expectedRepairedFields) {
    repairedManifests.stream().forEach(e ->
            Assert.assertEquals("Action should have repaired metrics", expectedRepairedFields,
                    e.fields()));
  }
}

