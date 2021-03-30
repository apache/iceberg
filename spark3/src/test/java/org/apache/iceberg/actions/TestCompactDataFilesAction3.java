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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.actions.CompactDataFilesAction;
import org.apache.iceberg.spark.actions.compaction.BinningCompactionStrategy;
import org.apache.iceberg.spark.actions.compaction.SparkBinningCompactionStrategy;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

/**
 * This is just WIP while waiting for V2Actions api to settle
 */
public class TestCompactDataFilesAction3 extends SparkTestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testRewriteDataFilesEmptyTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    Assert.assertNull("Table must be empty", table.currentSnapshot());

    new CompactDataFilesAction(spark, table).execute();

    Assert.assertNull("Table must stay empty", table.currentSnapshot());
  }

  @Test
  public void testRewriteDataFilesUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
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


    shouldPlanFiles(table, 4);

    withDataUnchanged( () -> {
      Actions actions = Actions.forTable(table);
      CompactDataFiles.Result result = new CompactDataFilesAction(spark, table).execute();

      Assert.assertEquals("Action should run 1 Job", 1, result.resultMap().size());
      CompactDataFiles.CompactionResult jobResult = result.resultMap().values().iterator().next();
      Assert.assertEquals("Action should remove 4 datafiles", 4, jobResult.numFilesRemoved());
      Assert.assertEquals("Action should add 1 datafile", 1, jobResult.numFilesAdded());
      return;
    });

    shouldPlanFiles(table, 1);
  }

  @Test
  public void testRewriteDataFilesPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"),
        new ThreeColumnRecord(1, "AAAAAAAAAA", "CCCC")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "DDDD")
    );
    writeRecords(records2);

    List<ThreeColumnRecord> records3 = Lists.newArrayList(
        new ThreeColumnRecord(2, "AAAAAAAAAA", "EEEE"),
        new ThreeColumnRecord(2, "AAAAAAAAAA", "GGGG")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
        new ThreeColumnRecord(2, "BBBBBBBBBB", "FFFF"),
        new ThreeColumnRecord(2, "BBBBBBBBBB", "HHHH")
    );
    writeRecords(records4);

    shouldPlanFiles(table, 8);

    withDataUnchanged(() -> {
      // TODO we need HMS Catalog so we can do concurrent operations here
      CompactDataFiles.Result result = new CompactDataFilesAction(spark, table).parallelism(1).execute();
      Assert.assertEquals("Action should run 4 Jobs", 4, result.resultMap().size());

      result.resultMap().values().forEach( jobResult -> {
        Assert.assertEquals("Action should remove 2 datafiles", 2, jobResult.numFilesRemoved());
        Assert.assertEquals("Action should add 1 datafile", 1, jobResult.numFilesAdded());
      });
    });

    shouldPlanFiles(table, 4);
  }

  @Test
  public void testRewriteDataFilesWithFilter() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"),
        new ThreeColumnRecord(1, "AAAAAAAAAA", "CCCC")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "DDDD")
    );
    writeRecords(records2);

    List<ThreeColumnRecord> records3 = Lists.newArrayList(
        new ThreeColumnRecord(2, "AAAAAAAAAA", "EEEE"),
        new ThreeColumnRecord(2, "AAAAAAAAAA", "GGGG")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
        new ThreeColumnRecord(2, "BBBBBBBBBB", "FFFF"),
        new ThreeColumnRecord(2, "BBBBBBBBBB", "HHHH")
    );
    writeRecords(records4);

   shouldPlanFiles(table, 8);

   withDataUnchanged(() -> {
     CompactDataFiles.Result result = new CompactDataFilesAction(spark, table)
         .filter(Expressions.equal("c1", 1))
         .filter(Expressions.startsWith("c2", "AA"))
         .execute();

     Assert.assertEquals("Action should run 1 Job", 1, result.resultMap().size());
     CompactDataFiles.CompactionResult jobResult = result.resultMap().values().iterator().next();
     Assert.assertEquals("Action should remove 2 datafiles", 2, jobResult.numFilesRemoved());
     Assert.assertEquals("Action should add 1 datafile", 1, jobResult.numFilesAdded());
   });

   shouldPlanFiles(table, 7);
  }

  @Test
  public void testRewriteLargeTableHasResiduals() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "100");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // all records belong to the same partition
    List<ThreeColumnRecord> records = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      records.add(new ThreeColumnRecord(i, String.valueOf(i), String.valueOf(i % 4)));
    }
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    writeDF(df);

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan()
        .ignoreResiduals()
        .filter(Expressions.equal("c3", "0"))
        .planFiles();
    for (FileScanTask task : tasks) {
      Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), task.residual());
    }

    shouldPlanFiles(table, 2);

    withDataUnchanged(() -> {
      CompactDataFiles.Result result = new CompactDataFilesAction(spark, table)
          .filter(Expressions.equal("c3", "0"))
          .execute();
      Assert.assertEquals("Action should run 1 Job", 1, result.resultMap().size());
      CompactDataFiles.CompactionResult jobResult = result.resultMap().values().iterator().next();
      Assert.assertEquals("Action should remove 2 datafiles", 2, jobResult.numFilesRemoved());
      Assert.assertEquals("Action should add 1 datafile", 1, jobResult.numFilesAdded());
    });
  }

  /**
   * Creates one large file and 2 small files. The large file should be split into 4 pieces, 3 pieces each being
   * large enough to make their own target size file, the last piece needs to be combined with the 2 small files
   * to make a target size file.
   */
  @Test
  public void testSplitLargeFile() throws AnalysisException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);
    Assert.assertNull("Table must be empty", table.currentSnapshot());

    // Reader can only split tasks on row-groups and only checks for rollovers on 1000 Row Intervals
    table.updateProperties().set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "2000").commit();

    List<ThreeColumnRecord> records1 = Lists.newArrayList();
    IntStream.range(0, 10000).forEach(i -> records1.add(new ThreeColumnRecord(i, "foo" + i, "bar" + i)));
    Dataset<Row> df = spark.createDataFrame(records1, ThreeColumnRecord.class).repartition(1);
    writeDF(df);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(1, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    shouldPlanFiles(table, 3);

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    DataFile maxSizeFile = Collections.max(dataFiles, Comparator.comparingLong(DataFile::fileSizeInBytes));
    Assert.assertEquals("Should have 3 files before rewrite", 3, dataFiles.size());

    withDataUnchanged(() -> {
      long targetSizeInBytes = (maxSizeFile.fileSizeInBytes() - 10) / 4;
      CompactDataFiles.Result result = new CompactDataFilesAction(spark, table)
          .option(BinningCompactionStrategy.TARGET_SIZE_OPTION, String.valueOf(targetSizeInBytes))
          .option(BinningCompactionStrategy.TARGET_THRESHOLD_OPTION, String.valueOf(5))
          .execute();

      Assert.assertEquals("Action should run 1 Job", 1, result.resultMap().size());
      CompactDataFiles.CompactionResult jobResult = result.resultMap().values().iterator().next();
      Assert.assertEquals("Action should remove 3 datafiles", 3, jobResult.numFilesRemoved());
      Assert.assertEquals("Action should add 4 datafile", 4, jobResult.numFilesAdded());
    });

    shouldPlanFiles(table, 4);
  }

  private void shouldPlanFiles(Table table, int files) {
    table.refresh();
    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    String message = String.format("Should have %d data files when scanning", files);
    Assert.assertEquals(message, files, dataFiles.size());
  }

  private void withDataUnchanged(Runnable test) {
    Dataset<ThreeColumnRecord> tableDF = spark.read().format("iceberg")
        .load(tableLocation).sort("c1", "c2", "c3")
        .as(Encoders.bean(ThreeColumnRecord.class));

    List<ThreeColumnRecord> originalRecords = tableDF.collectAsList();

    test.run();

    List<ThreeColumnRecord> postTestRecords = tableDF.collectAsList();

    Assert.assertEquals("Row values were changed, but they should be identical",
        originalRecords, postTestRecords);
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
}
