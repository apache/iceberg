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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.FileGroupRewriteResult;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.Result;
import org.apache.iceberg.actions.SizeBasedFileRewriter;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.source.FourColumnRecord;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

public class TestRewritePositionDeleteFilesAction extends SparkCatalogTestBase {

  private static final String TABLE_NAME = "test_table";
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  private static final Map<String, String> CATALOG_PROPS =
      ImmutableMap.of(
          "type", "hive",
          "default-namespace", "default",
          "cache-enabled", "false");

  private static final int SCALE = 4000;
  private static final int DELETES_SCALE = 1000;

  @Parameterized.Parameters(
      name =
          "formatVersion = {0}, catalogName = {1}, implementation = {2}, config = {3}, fileFormat = {4}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        CATALOG_PROPS,
        FileFormat.PARQUET
      }
    };
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private final FileFormat format;

  public TestRewritePositionDeleteFilesAction(
      String catalogName, String implementation, Map<String, String> config, FileFormat format) {
    super(catalogName, implementation, config);
    this.format = format;
  }

  @After
  public void cleanup() {
    validationCatalog.dropTable(TableIdentifier.of("default", TABLE_NAME));
  }

  @Test
  public void testEmptyTable() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME), SCHEMA, spec, tableProperties());

    Result result = SparkActions.get(spark).rewritePositionDeletes(table).execute();
    Assert.assertEquals("No rewritten delete files", 0, result.rewrittenDeleteFilesCount());
    Assert.assertEquals("No added delete files", 0, result.addedDeleteFilesCount());
  }

  @Test
  public void testUnpartitioned() throws Exception {
    Table table = createTableUnpartitioned(2, SCALE);
    List<DataFile> dataFiles = dataFiles(table);
    List<DeleteFile> deleteFiles = writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(2, dataFiles.size());
    Assert.assertEquals(2, deleteFiles.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(2000, expectedRecords.size());
    Assert.assertEquals(2000, expectedDeletes.size());

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Expected 1 new delete file", 1, newDeleteFiles.size());
    assertLocallySorted(newDeleteFiles);
    assertNotContains(deleteFiles, newDeleteFiles);
    checkResult(result, deleteFiles, newDeleteFiles, 1);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testRewriteAll() throws Exception {
    Table table = createTablePartitioned(4, 2, SCALE);

    List<DataFile> dataFiles = dataFiles(table);
    List<DeleteFile> deleteFiles = writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(4, dataFiles.size());
    Assert.assertEquals(8, deleteFiles.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(12000, expectedRecords.size());
    Assert.assertEquals(4000, expectedDeletes.size());

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE - 1))
            .execute();

    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 4 delete files", 4, newDeleteFiles.size());
    assertNotContains(deleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, deleteFiles, newDeleteFiles, 4);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testRewriteToSmallerTarget() throws Exception {
    Table table = createTablePartitioned(4, 2, SCALE);

    List<DataFile> dataFiles = dataFiles(table);
    List<DeleteFile> deleteFiles = writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(4, dataFiles.size());
    Assert.assertEquals(8, deleteFiles.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(12000, expectedRecords.size());
    Assert.assertEquals(4000, expectedDeletes.size());

    long avgSize = size(deleteFiles) / deleteFiles.size();

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(SizeBasedFileRewriter.TARGET_FILE_SIZE_BYTES, String.valueOf(avgSize / 2))
            .execute();
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 8 new delete files", 8, newDeleteFiles.size());
    assertNotContains(deleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, deleteFiles, newDeleteFiles, 4);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testRemoveDanglingDeletes() throws Exception {
    Table table = createTablePartitioned(4, 2, SCALE);

    List<DataFile> dataFiles = dataFiles(table);
    List<DeleteFile> deleteFiles = writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(4, dataFiles.size());
    Assert.assertEquals(8, deleteFiles.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(12000, expectedRecords.size());
    Assert.assertEquals(4000, expectedDeletes.size());

    SparkActions.get(spark)
        .rewriteDataFiles(table)
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .execute();

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 0 new delete files", 0, newDeleteFiles.size());
    assertNotContains(deleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, deleteFiles, newDeleteFiles, 4);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    Assert.assertEquals("Should be no new position deletes", 0, actualDeletes.size());
  }

  @Test
  public void testSomePartitionsDanglingDeletes() throws Exception {
    Table table = createTablePartitioned(4, 2, SCALE);

    List<DataFile> dataFiles = dataFiles(table);
    List<DeleteFile> deleteFiles = writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(4, dataFiles.size());
    Assert.assertEquals(8, deleteFiles.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(12000, expectedRecords.size());
    Assert.assertEquals(4000, expectedDeletes.size());

    // Rewrite half the data files
    Expression filter = Expressions.or(Expressions.equal("c1", 0), Expressions.equal("c1", 1));
    SparkActions.get(spark)
        .rewriteDataFiles(table)
        .filter(filter)
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .execute();

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 2 new delete files", 2, newDeleteFiles.size());
    assertNotContains(deleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, deleteFiles, newDeleteFiles, 4);

    // As only half the files have been rewritten,
    // we expect to retain position deletes only for those not rewritten
    expectedDeletes =
        expectedDeletes.stream()
            .filter(
                r -> {
                  Object[] partition = (Object[]) r[3];
                  return partition[0] == (Integer) 2 || partition[0] == (Integer) 3;
                })
            .collect(Collectors.toList());

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testPartitionEvolutionAdd() throws Exception {
    Table table = createTableUnpartitioned(2, SCALE);
    List<DataFile> unpartitionedDataFiles = dataFiles(table);
    List<DeleteFile> unpartitionedDeleteFiles =
        writePosDeletesForFiles(table, 2, DELETES_SCALE, unpartitionedDataFiles);
    Assert.assertEquals(2, unpartitionedDataFiles.size());
    Assert.assertEquals(2, unpartitionedDeleteFiles.size());

    List<Object[]> expectedUnpartitionedDeletes = deleteRecords(table);
    List<Object[]> expectedUnpartitionedRecords = records(table);
    Assert.assertEquals(2000, expectedUnpartitionedRecords.size());
    Assert.assertEquals(2000, expectedUnpartitionedDeletes.size());

    table.updateSpec().addField("c1").commit();
    writeRecords(table, 2, SCALE, 2);
    List<DataFile> partitionedDataFiles = except(dataFiles(table), unpartitionedDataFiles);
    List<DeleteFile> partitionedDeleteFiles =
        writePosDeletesForFiles(table, 2, DELETES_SCALE, partitionedDataFiles);
    Assert.assertEquals(2, partitionedDataFiles.size());
    Assert.assertEquals(4, partitionedDeleteFiles.size());

    List<Object[]> expectedDeletes = deleteRecords(table);
    List<Object[]> expectedRecords = records(table);
    Assert.assertEquals(4000, expectedDeletes.size());
    Assert.assertEquals(8000, expectedRecords.size());

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();

    List<DeleteFile> rewrittenDeleteFiles =
        Stream.concat(unpartitionedDeleteFiles.stream(), partitionedDeleteFiles.stream())
            .collect(Collectors.toList());
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 3 new delete files", 3, newDeleteFiles.size());
    assertNotContains(rewrittenDeleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, rewrittenDeleteFiles, newDeleteFiles, 3);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testPartitionEvolutionRemove() throws Exception {
    Table table = createTablePartitioned(2, 2, SCALE);
    List<DataFile> dataFilesUnpartitioned = dataFiles(table);
    List<DeleteFile> deleteFilesUnpartitioned =
        writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFilesUnpartitioned);
    Assert.assertEquals(2, dataFilesUnpartitioned.size());
    Assert.assertEquals(4, deleteFilesUnpartitioned.size());

    table.updateSpec().removeField("c1").commit();

    writeRecords(table, 2, SCALE);
    List<DataFile> dataFilesPartitioned = except(dataFiles(table), dataFilesUnpartitioned);
    List<DeleteFile> deleteFilesPartitioned =
        writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFilesPartitioned);
    Assert.assertEquals(2, dataFilesPartitioned.size());
    Assert.assertEquals(2, deleteFilesPartitioned.size());

    List<Object[]> expectedRecords = records(table);
    List<Object[]> expectedDeletes = deleteRecords(table);
    Assert.assertEquals(4000, expectedDeletes.size());
    Assert.assertEquals(8000, expectedRecords.size());

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();

    List<DeleteFile> expectedRewritten =
        Stream.concat(deleteFilesUnpartitioned.stream(), deleteFilesPartitioned.stream())
            .collect(Collectors.toList());
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 3 new delete files", 3, newDeleteFiles.size());
    assertNotContains(expectedRewritten, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, expectedRewritten, newDeleteFiles, 3);

    List<Object[]> actualRecords = records(table);
    List<Object[]> actualDeletes = deleteRecords(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertEquals("Position deletes must match", expectedDeletes, actualDeletes);
  }

  @Test
  public void testSchemaEvolution() throws Exception {
    Table table = createTablePartitioned(2, 2, SCALE);
    List<DataFile> dataFiles = dataFiles(table);
    List<DeleteFile> deleteFiles = writePosDeletesForFiles(table, 2, DELETES_SCALE, dataFiles);
    Assert.assertEquals(2, dataFiles.size());
    Assert.assertEquals(4, deleteFiles.size());

    table.updateSchema().addColumn("c4", Types.StringType.get()).commit();
    writeNewSchemaRecords(table, 2, SCALE, 2, 2);

    int newColId = table.schema().findField("c4").fieldId();
    List<DataFile> newSchemaDataFiles =
        dataFiles(table).stream()
            .filter(f -> f.upperBounds().containsKey(newColId))
            .collect(Collectors.toList());
    List<DeleteFile> newSchemaDeleteFiles =
        writePosDeletesForFiles(table, 2, DELETES_SCALE, newSchemaDataFiles);
    Assert.assertEquals(4, newSchemaDeleteFiles.size());

    table.refresh();
    List<Object[]> expectedDeletes = deleteRecords(table);
    List<Object[]> expectedRecords = records(table);
    Assert.assertEquals(4000, expectedDeletes.size()); // 4 files * 1000 per file
    Assert.assertEquals(12000, expectedRecords.size()); // 4 * 4000 - 4000

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();

    List<DeleteFile> rewrittenDeleteFiles =
        Stream.concat(deleteFiles.stream(), newSchemaDeleteFiles.stream())
            .collect(Collectors.toList());
    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    Assert.assertEquals("Should have 2 new delete files", 4, newDeleteFiles.size());
    assertNotContains(rewrittenDeleteFiles, newDeleteFiles);
    assertLocallySorted(newDeleteFiles);
    checkResult(result, rewrittenDeleteFiles, newDeleteFiles, 4);

    List<Object[]> actualRecords = records(table);
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  private Table createTablePartitioned(int partitions, int files, int numRecords) {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME), SCHEMA, spec, tableProperties());

    writeRecords(table, files, numRecords, partitions);
    return table;
  }

  private Table createTableUnpartitioned(int files, int numRecords) {
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME),
            SCHEMA,
            PartitionSpec.unpartitioned(),
            tableProperties());

    writeRecords(table, files, numRecords);
    return table;
  }

  private Map<String, String> tableProperties() {
    return ImmutableMap.of(
        TableProperties.DEFAULT_WRITE_METRICS_MODE,
        "full",
        TableProperties.FORMAT_VERSION,
        "2",
        TableProperties.DEFAULT_FILE_FORMAT,
        format.toString());
  }

  private void writeRecords(Table table, int files, int numRecords) {
    writeRecords(table, files, numRecords, 1);
  }

  private void writeRecords(Table table, int files, int numRecords, int numPartitions) {
    writeRecordsWithPartitions(
        table,
        files,
        numRecords,
        IntStream.range(0, numPartitions).mapToObj(ImmutableList::of).collect(Collectors.toList()));
  }

  private void writeRecordsWithPartitions(
      Table table, int files, int numRecords, List<List<Integer>> partitions) {
    int partitionTypeSize = table.spec().partitionType().fields().size();
    Assert.assertTrue(
        "This method currently supports only two columns as partition columns",
        partitionTypeSize <= 2);
    BiFunction<Integer, List<Integer>, ThreeColumnRecord> recordFunction =
        (i, partValues) -> {
          switch (partitionTypeSize) {
            case (0):
              return new ThreeColumnRecord(i, String.valueOf(i), String.valueOf(i));
            case (1):
              return new ThreeColumnRecord(partValues.get(0), String.valueOf(i), String.valueOf(i));
            case (2):
              return new ThreeColumnRecord(
                  partValues.get(0), String.valueOf(partValues.get(1)), String.valueOf(i));
            default:
              throw new ValidationException(
                  "This method currently supports only two columns as partition columns");
          }
        };
    List<ThreeColumnRecord> records =
        partitions.stream()
            .flatMap(
                partition ->
                    IntStream.range(0, numRecords)
                        .mapToObj(i -> recordFunction.apply(i, partition)))
            .collect(Collectors.toList());
    spark
        .createDataFrame(records, ThreeColumnRecord.class)
        .repartition(files)
        .write()
        .format("iceberg")
        .mode("append")
        .save(name(table));
    table.refresh();
  }

  private void writeNewSchemaRecords(
      Table table, int files, int numRecords, int startingPartition, int partitions) {
    List<FourColumnRecord> records =
        IntStream.range(startingPartition, startingPartition + partitions)
            .boxed()
            .flatMap(
                partition ->
                    IntStream.range(0, numRecords)
                        .mapToObj(
                            i ->
                                new FourColumnRecord(
                                    partition,
                                    String.valueOf(i),
                                    String.valueOf(i),
                                    String.valueOf(i))))
            .collect(Collectors.toList());
    spark
        .createDataFrame(records, FourColumnRecord.class)
        .repartition(files)
        .write()
        .format("iceberg")
        .mode("append")
        .save(name(table));
  }

  private List<Object[]> records(Table table) {
    return rowsToJava(
        spark.read().format("iceberg").load(name(table)).sort("c1", "c2", "c3").collectAsList());
  }

  private List<Object[]> deleteRecords(Table table) {
    String[] additionalFields;
    // do not select delete_file_path for comparison
    // as delete files have been rewritten
    if (table.spec().isUnpartitioned()) {
      additionalFields = new String[] {"pos", "row"};
    } else {
      additionalFields = new String[] {"pos", "row", "partition", "spec_id"};
    }
    return rowsToJava(
        spark
            .read()
            .format("iceberg")
            .load(name(table) + ".position_deletes")
            .select("file_path", additionalFields)
            .sort("file_path", "pos")
            .collectAsList());
  }

  private List<DeleteFile> writePosDeletesForFiles(
      Table table, int deleteFilesPerPartition, int deletesPerDataFile, List<DataFile> files)
      throws IOException {

    Map<StructLike, List<DataFile>> filesByPartition =
        files.stream().collect(Collectors.groupingBy(ContentFile::partition));
    List<DeleteFile> results =
        Lists.newArrayListWithCapacity(deleteFilesPerPartition * filesByPartition.size());

    for (Map.Entry<StructLike, List<DataFile>> filesByPartitionEntry :
        filesByPartition.entrySet()) {

      StructLike partition = filesByPartitionEntry.getKey();
      List<DataFile> partitionFiles = filesByPartitionEntry.getValue();

      int deletesForPartition = partitionFiles.size() * deletesPerDataFile;
      Assert.assertEquals(
          "Number of delete files per partition should be "
              + "evenly divisible by requested deletes per data file times number of data files in this partition",
          0,
          deletesForPartition % deleteFilesPerPartition);
      int deleteFileSize = deletesForPartition / deleteFilesPerPartition;

      int counter = 0;
      List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
      for (DataFile partitionFile : partitionFiles) {
        for (int deletePos = 0; deletePos < deletesPerDataFile; deletePos++) {
          deletes.add(Pair.of(partitionFile.path(), (long) deletePos));
          counter++;
          if (counter == deleteFileSize) {
            // Dump to file and reset variables
            OutputFile output = Files.localOutput(temp.newFile());
            results.add(FileHelpers.writeDeleteFile(table, output, partition, deletes).first());
            counter = 0;
            deletes.clear();
          }
        }
      }
    }

    RowDelta rowDelta = table.newRowDelta();
    results.forEach(rowDelta::addDeletes);
    rowDelta.commit();

    return results;
  }

  private List<DataFile> dataFiles(Table table) {
    CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles();
    return Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
  }

  private List<DeleteFile> deleteFiles(Table table) {
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);
    CloseableIterable<ScanTask> tasks = deletesTable.newBatchScan().planFiles();
    return Lists.newArrayList(
        CloseableIterable.transform(tasks, t -> ((PositionDeletesScanTask) t).file()));
  }

  private <T extends ContentFile<?>> List<T> except(List<T> first, List<T> second) {
    Set<String> secondPaths =
        second.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
    return first.stream()
        .filter(f -> !secondPaths.contains(f.path().toString()))
        .collect(Collectors.toList());
  }

  private void assertNotContains(List<DeleteFile> original, List<DeleteFile> rewritten) {
    Set<String> originalPaths =
        original.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
    Set<String> rewrittenPaths =
        rewritten.stream().map(f -> f.path().toString()).collect(Collectors.toSet());
    rewrittenPaths.retainAll(originalPaths);
    Assert.assertEquals(0, rewrittenPaths.size());
  }

  private void assertLocallySorted(List<DeleteFile> deleteFiles) {
    for (DeleteFile deleteFile : deleteFiles) {
      Dataset<Row> deletes =
          spark.read().format("iceberg").load("default." + TABLE_NAME + ".position_deletes");
      deletes.filter(deletes.col("delete_file_path").equalTo(deleteFile.path().toString()));
      List<Row> rows = deletes.collectAsList();
      Assert.assertTrue("Empty delete file found", rows.size() > 0);
      int lastPos = 0;
      String lastPath = "";
      for (Row row : rows) {
        String path = row.getAs("file_path");
        long pos = row.getAs("pos");
        if (path.compareTo(lastPath) < 0) {
          Assert.fail(String.format("File_path not sorted, Found %s after %s", path, lastPath));
        } else if (path.equals(lastPath)) {
          Assert.assertTrue("Pos not sorted", pos >= lastPos);
        }
      }
    }
  }

  private String name(Table table) {
    String[] splits = table.name().split("\\.");
    Assert.assertEquals(3, splits.length);
    return String.format("%s.%s", splits[1], splits[2]);
  }

  private long size(List<DeleteFile> deleteFiles) {
    return deleteFiles.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();
  }

  private void checkResult(
      Result result,
      List<DeleteFile> rewrittenDeletes,
      List<DeleteFile> newDeletes,
      int expectedGroups) {
    Assert.assertEquals(
        "Expected rewritten delete file count does not match",
        rewrittenDeletes.size(),
        result.rewrittenDeleteFilesCount());
    Assert.assertEquals(
        "Expected new delete file count does not match",
        newDeletes.size(),
        result.addedDeleteFilesCount());
    Assert.assertEquals(
        "Expected rewritten delete byte count does not match",
        size(rewrittenDeletes),
        result.rewrittenBytesCount());
    Assert.assertEquals(
        "Expected new delete byte count does not match",
        size(newDeletes),
        result.addedBytesCount());

    Assert.assertEquals(
        "Expected rewrite group count does not match",
        expectedGroups,
        result.rewriteResults().size());
    Assert.assertEquals(
        "Expected rewritten delete file count in all groups to match",
        rewrittenDeletes.size(),
        result.rewriteResults().stream()
            .mapToInt(FileGroupRewriteResult::rewrittenDeleteFilesCount)
            .sum());
    Assert.assertEquals(
        "Expected added delete file count in all groups to match",
        newDeletes.size(),
        result.rewriteResults().stream()
            .mapToInt(FileGroupRewriteResult::addedDeleteFilesCount)
            .sum());
    Assert.assertEquals(
        "Expected rewritten delete bytes in all groups to match",
        size(rewrittenDeletes),
        result.rewriteResults().stream()
            .mapToLong(FileGroupRewriteResult::rewrittenBytesCount)
            .sum());
    Assert.assertEquals(
        "Expected added delete bytes in all groups to match",
        size(newDeletes),
        result.rewriteResults().stream().mapToLong(FileGroupRewriteResult::addedBytesCount).sum());
  }
}
