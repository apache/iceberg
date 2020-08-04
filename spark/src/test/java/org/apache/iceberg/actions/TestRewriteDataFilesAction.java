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
import java.util.List;
import java.util.Map;
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
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public abstract class TestRewriteDataFilesAction extends SparkTestBase {

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

    Actions actions = Actions.forTable(table);

    actions.rewriteDataFiles().execute();

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

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 4 data files before rewrite", 4, dataFiles.size());

    Actions actions = Actions.forTable(table);

    RewriteDataFilesActionResult result = actions.rewriteDataFiles().execute();
    Assert.assertEquals("Action should rewrite 4 data files", 4, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFiles().size());

    table.refresh();

    CloseableIterable<FileScanTask> tasks1 = table.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 1 data files before rewrite", 1, dataFiles1.size());

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

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 8 data files before rewrite", 8, dataFiles.size());

    Actions actions = Actions.forTable(table);

    RewriteDataFilesActionResult result = actions.rewriteDataFiles().execute();
    Assert.assertEquals("Action should rewrite 8 data files", 8, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 4 data file", 4, result.addedDataFiles().size());

    table.refresh();

    CloseableIterable<FileScanTask> tasks1 = table.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 4 data files before rewrite", 4, dataFiles1.size());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2", "c3")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
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

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 8 data files before rewrite", 8, dataFiles.size());

    Actions actions = Actions.forTable(table);

    RewriteDataFilesActionResult result = actions
        .rewriteDataFiles()
        .filter(Expressions.equal("c1", 1))
        .filter(Expressions.startsWith("c2", "AA"))
        .execute();
    Assert.assertEquals("Action should rewrite 2 data files", 2, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFiles().size());

    table.refresh();

    CloseableIterable<FileScanTask> tasks1 = table.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 7 data files before rewrite", 7, dataFiles1.size());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2", "c3")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
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
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 2 data files before rewrite", 2, dataFiles.size());

    Actions actions = Actions.forTable(table);

    RewriteDataFilesActionResult result = actions
        .rewriteDataFiles()
        .filter(Expressions.equal("c3", "0"))
        .execute();
    Assert.assertEquals("Action should rewrite 2 data files", 2, result.deletedDataFiles().size());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFiles().size());

    table.refresh();

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", records, actualRecords);
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
