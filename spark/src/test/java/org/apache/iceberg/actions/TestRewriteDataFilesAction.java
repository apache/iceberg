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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseCombinedScanTask;
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
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
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

  @Test
  public void testBaseCombinedScanTaskKryoSerialization() throws Exception {
    BaseCombinedScanTask scanTask = prepareBaseCombinedScanTaskForSerDeTest();

    File data = temp.newFile();
    Assert.assertTrue(data.delete());
    Kryo kryo = new KryoSerializer(new SparkConf()).newKryo();

    try (Output out = new Output(new FileOutputStream(data))) {
      kryo.writeClassAndObject(out, scanTask);
    }

    try (Input in = new Input(new FileInputStream(data))) {
      Object obj = kryo.readClassAndObject(in);
      Assert.assertTrue("Should be a BaseCombinedScanTask", obj instanceof BaseCombinedScanTask);
      checkBaseCombinedScanTask(scanTask, (BaseCombinedScanTask) obj);
    }
  }

  @Test
  public void testBaseCombinedScanTaskJavaSerialization() throws Exception {
    BaseCombinedScanTask scanTask = prepareBaseCombinedScanTaskForSerDeTest();

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(scanTask);
    }

    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      Object obj = in.readObject();
      Assert.assertTrue("Should be a BaseCombinedScanTask", obj instanceof BaseCombinedScanTask);
      checkBaseCombinedScanTask(scanTask, (BaseCombinedScanTask) obj);
    }
  }

  private BaseCombinedScanTask prepareBaseCombinedScanTaskForSerDeTest() {
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
    return new BaseCombinedScanTask(Lists.newArrayList(tasks));
  }

  private void checkBaseCombinedScanTask(BaseCombinedScanTask expected, BaseCombinedScanTask actual) {
    List<FileScanTask> expectedTasks = getFileScanTasksInFilePathOrder(expected);
    List<FileScanTask> actualTasks = getFileScanTasksInFilePathOrder(actual);

    Assert.assertEquals("The number of file scan tasks should match",
            expectedTasks.size(), actualTasks.size());

    for (int i = 0; i < expectedTasks.size(); i++) {
      FileScanTask expectedTask = expectedTasks.get(i);
      FileScanTask actualTask = actualTasks.get(i);
      checkFileScanTask(expectedTask, actualTask);
    }
  }

  private List<FileScanTask> getFileScanTasksInFilePathOrder(BaseCombinedScanTask task) {
    return task.files().stream()
            // use file path + start position to differentiate the tasks
            .sorted(Comparator.comparing(o -> o.file().path().toString() + "##" + o.start()))
            .collect(Collectors.toList());
  }

  private void checkFileScanTask(FileScanTask expected, FileScanTask actual) {
    checkDataFile(expected.file(), actual.file());

    // PartitionSpec implements its own equals method
    Assert.assertEquals("PartitionSpec doesn't match", expected.spec(), actual.spec());

    Assert.assertEquals("starting position doesn't match", expected.start(), actual.start());

    Assert.assertEquals("the number of bytes to scan doesn't match", expected.start(), actual.start());

    // simplify comparison on residual expression via comparing toString
    Assert.assertEquals("Residual expression doesn't match",
            expected.residual().toString(), actual.residual().toString());
  }

  // TODO: this is a copy of TestDataFileSerialization.checkDataFile, deduplicate
  private void checkDataFile(DataFile expected, DataFile actual) {
    Assert.assertEquals("Should match the serialized record path",
            expected.path(), actual.path());
    Assert.assertEquals("Should match the serialized record format",
            expected.format(), actual.format());
    Assert.assertEquals("Should match the serialized record partition",
            expected.partition().get(0, Object.class), actual.partition().get(0, Object.class));
    Assert.assertEquals("Should match the serialized record count",
            expected.recordCount(), actual.recordCount());
    Assert.assertEquals("Should match the serialized record size",
            expected.fileSizeInBytes(), actual.fileSizeInBytes());
    Assert.assertEquals("Should match the serialized record value counts",
            expected.valueCounts(), actual.valueCounts());
    Assert.assertEquals("Should match the serialized record null value counts",
            expected.nullValueCounts(), actual.nullValueCounts());
    Assert.assertEquals("Should match the serialized record lower bounds",
            expected.lowerBounds(), actual.lowerBounds());
    Assert.assertEquals("Should match the serialized record upper bounds",
            expected.upperBounds(), actual.upperBounds());
    Assert.assertEquals("Should match the serialized record key metadata",
            expected.keyMetadata(), actual.keyMetadata());
    Assert.assertEquals("Should match the serialized record offsets",
            expected.splitOffsets(), actual.splitOffsets());
    Assert.assertEquals("Should match the serialized record offsets",
            expected.keyMetadata(), actual.keyMetadata());
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
