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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.TableProperties.DROP_PARTITION_DELETE_ENABLED;
import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles.Result;
import org.apache.iceberg.actions.SizeBasedFileRewriter;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestRewriteDeleteFilesAction extends SparkTestBase {

  private static final int SCALE = 400000;

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  PartitionSpec partitionSpecC1 = PartitionSpec.builderFor(SCHEMA).identity("c1").build();

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  private Result rewriteTable(Table table) {
    return actions()
        .rewriteDataFiles(table)
        .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE - 1))
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .execute();
  }

  @Test
  public void testRewritePartitionDeletesShouldNotRetain() throws IOException {
    // TODO: another case with global equality deletes
    BaseTable table = createV2PartitionTable(partitionSpecC1);
    table.updateProperties().set(DROP_PARTITION_DELETE_ENABLED, "true").commit();
    // write partition 1 data sequence 1
    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    writeRecords(records1);

    // write partition 2 data sequence 2
    List<ThreeColumnRecord> records2 =
        Lists.newArrayList(
            new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD"));
    writeRecords(records2);

    // write equality deletes sequence 3
    writeEqDeleteRecord(table, "c1", 1, "c3", "AAAA");
    // write equality deletes sequence 4
    writeEqDeleteRecord(table, "c1", 2, "c3", "CCCC");

    // only rewrite c1=1 partition
    // expecting to see partition 1 data rewritten and sequence raise to 5
    // this time the partition 1 delete file sees a min sequence number of 1
    // its corresponding delete file with sequence number of 3, so delete file retained
    actions()
        .rewriteDataFiles(table)
        .filter(Expressions.equal("c1", 1))
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .execute();

    // only rewrite c1=1 partition
    // partition c1 = 1 partition has the new data file and min sequence should be 5
    // the old delete file in partition 1 now never applies to partition, hence should be removed
    actions()
        .rewriteDataFiles(table)
        .filter(Expressions.equal("c1", 1))
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .execute();

    // because partition c1 = 1 is rewritten, so expecting its delete files removed
    table.refresh();
    Map<String, String> commitSummary = table.currentSnapshot().summary();
    Assert.assertEquals("1", commitSummary.get("total-equality-deletes"));
  }

  private void writeEqDeleteRecord(
      BaseTable table, String partCol, Object partVal, String delCol, Object delVal) {
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField(delCol).fieldId());
    Schema eqDeleteRowSchema = table.schema().select(delCol);
    Record partitionRecord =
        GenericRecord.create(table.schema().select(partCol))
            .copy(ImmutableMap.of(partCol, partVal));
    Record record = GenericRecord.create(eqDeleteRowSchema).copy(ImmutableMap.of(delCol, delVal));
    writeEqDeleteRecord(table, equalityFieldIds, partitionRecord, eqDeleteRowSchema, record);
  }

  private void writeEqDeleteRecord(
      BaseTable table,
      List<Integer> equalityFieldIds,
      Record partitionRecord,
      Schema eqDeleteRowSchema,
      Record deleteRecord) {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(
            table.schema(),
            table.spec(),
            ArrayUtil.toIntArray(equalityFieldIds),
            eqDeleteRowSchema,
            null);

    EncryptedOutputFile file =
        createEncryptedOutputFile(createPartitionKey(table, partitionRecord), fileFactory);
    EqualityDeleteWriter<Record> eqDeleteWriter =
        appenderFactory.newEqDeleteWriter(
            file, FileFormat.PARQUET, createPartitionKey(table, partitionRecord));

    try (EqualityDeleteWriter<Record> clsEqDeleteWriter = eqDeleteWriter) {
      // delete c3=AAAA
      clsEqDeleteWriter.write(deleteRecord);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    table.newRowDelta().addDeletes(eqDeleteWriter.toDeleteFile()).commit();
  }

  private void writePosDeleteRecord(
      BaseTable table, String partCol, Object partVal, CharSequence path, int pos) {
    Record partitionRecord =
        GenericRecord.create(table.schema().select(partCol))
            .copy(ImmutableMap.of(partCol, partVal));
    writePosDeleteRecord(table, partitionRecord, path, pos);
  }

  private void writePosDeleteRecord(
      BaseTable table, Record partitionRecord, CharSequence path, int pos) {
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec(), null, null, null);
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
    EncryptedOutputFile file =
        createEncryptedOutputFile(createPartitionKey(table, partitionRecord), fileFactory);
    PositionDeleteWriter<Record> posDeleteWriter =
        appenderFactory.newPosDeleteWriter(
            file, FileFormat.PARQUET, createPartitionKey(table, partitionRecord));
    PositionDelete positionDeleteRecord = PositionDelete.create().set(path, pos, null);
    try (PositionDeleteWriter<Record> clsPosDeleteWriter = posDeleteWriter) {
      clsPosDeleteWriter.write(positionDeleteRecord);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    table.newRowDelta().addDeletes(posDeleteWriter.toDeleteFile()).commit();
    table.refresh();
  }

  private BaseTable createV2PartitionTable(PartitionSpec spec) {
    Map<String, String> options = Maps.newHashMap();
    BaseTable table = (BaseTable) TABLES.create(SCHEMA, spec, options, tableLocation);
    TableOperations ops = table.operations();
    ops.commit(ops.current(), ops.current().upgradeToFormatVersion(2));
    return table;
  }

  protected List<Object[]> currentData() {
    return rowsToJava(
        spark.read().format("iceberg").load(tableLocation).sort("c1", "c2", "c3").collectAsList());
  }

  protected long testDataSize(Table table) {
    return Streams.stream(table.newScan().planFiles()).mapToLong(FileScanTask::length).sum();
  }

  protected void shouldHaveFiles(Table table, int numExpected) {
    table.refresh();
    int numFiles = Iterables.size(table.newScan().planFiles());
    Assert.assertEquals("Did not have the expected number of files", numExpected, numFiles);
  }

  protected Table createTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);
    table
        .updateProperties()
        .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(20 * 1024))
        .commit();
    Assert.assertNull("Table must be empty", table.currentSnapshot());
    table.refresh();
    return table;
  }

  /**
   * Create a table with a certain number of files, returns the size of a file
   *
   * @param files number of files to create
   * @return the created table
   */
  protected Table createTable(int files) {
    Table table = createTable();
    writeRecords(files, SCALE);
    return table;
  }

  protected Table createTablePartitioned(
      int partitions, int files, int numRecords, Map<String, String> options) {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").truncate("c2", 2).build();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);
    Assert.assertNull("Table must be empty", table.currentSnapshot());
    table.refresh();
    writeRecords(files, numRecords, partitions);
    return table;
  }

  protected Table createTablePartitioned(int partitions, int files) {
    return createTablePartitioned(partitions, files, SCALE, Maps.newHashMap());
  }

  private void writeRecords(int files, int numRecords) {
    writeRecords(files, numRecords, 0);
  }

  private void writeRecords(List<ThreeColumnRecord> records) {
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    writeDF(df);
  }

  private PartitionKey createPartitionKey(Table table, Record record) {
    if (table.spec().isUnpartitioned()) {
      return null;
    }

    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    partitionKey.partition(record);

    return partitionKey;
  }

  private EncryptedOutputFile createEncryptedOutputFile(
      PartitionKey partition, OutputFileFactory fileFactory) {
    if (partition == null) {
      return fileFactory.newOutputFile();
    } else {
      return fileFactory.newOutputFile(partition);
    }
  }

  private void writeRecords(int files, int numRecords, int partitions) {
    List<ThreeColumnRecord> records = Lists.newArrayList();
    int rowDimension = (int) Math.ceil(Math.sqrt(numRecords));
    List<Pair<Integer, Integer>> data =
        IntStream.range(0, rowDimension)
            .boxed()
            .flatMap(x -> IntStream.range(0, rowDimension).boxed().map(y -> Pair.of(x, y)))
            .collect(Collectors.toList());
    Collections.shuffle(data, new Random(42));
    if (partitions > 0) {
      data.forEach(
          i ->
              records.add(
                  new ThreeColumnRecord(
                      i.first() % partitions, "foo" + i.first(), "bar" + i.second())));
    } else {
      data.forEach(
          i ->
              records.add(new ThreeColumnRecord(i.first(), "foo" + i.first(), "bar" + i.second())));
    }
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).repartition(files);
    writeDF(df);
  }

  private void writeDF(Dataset<Row> df) {
    df.select("c1", "c2", "c3")
        .sortWithinPartitions("c1", "c2")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
  }

  private List<DeleteFile> writePosDeletesToFile(
      Table table, DataFile dataFile, int outputDeleteFiles) {
    return writePosDeletes(
        table, dataFile.partition(), dataFile.path().toString(), outputDeleteFiles);
  }

  private List<DeleteFile> writePosDeletes(
      Table table, StructLike partition, String path, int outputDeleteFiles) {
    List<DeleteFile> results = Lists.newArrayList();
    int rowPosition = 0;
    for (int file = 0; file < outputDeleteFiles; file++) {
      OutputFile outputFile =
          table
              .io()
              .newOutputFile(
                  table.locationProvider().newDataLocation(UUID.randomUUID().toString()));
      EncryptedOutputFile encryptedOutputFile =
          EncryptedFiles.encryptedOutput(outputFile, EncryptionKeyMetadata.EMPTY);

      GenericAppenderFactory appenderFactory =
          new GenericAppenderFactory(table.schema(), table.spec(), null, null, null);
      PositionDeleteWriter<Record> posDeleteWriter =
          appenderFactory
              .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "full")
              .newPosDeleteWriter(encryptedOutputFile, FileFormat.PARQUET, partition);

      PositionDelete<Record> posDelete = PositionDelete.create();
      posDeleteWriter.write(posDelete.set(path, rowPosition, null));
      try {
        posDeleteWriter.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      results.add(posDeleteWriter.toDeleteFile());
      rowPosition++;
    }

    return results;
  }

  private SparkActions actions() {
    return SparkActions.get();
  }
}
