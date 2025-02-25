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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RemoveMissingFiles;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestRemoveMissingFilesAction extends TestBaseWithCatalog {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "c1", Types.IntegerType.get()),
          Types.NestedField.optional(2, "c2", Types.StringType.get()),
          Types.NestedField.optional(3, "c3", Types.StringType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("c3").build();

  private List<ThreeColumnRecord> records1 =
      Lists.newArrayList(
          new ThreeColumnRecord(1, "a", "A"),
          new ThreeColumnRecord(2, "aa", "A"),
          new ThreeColumnRecord(3, "aaa", "A"));
  private List<ThreeColumnRecord> records2 =
      Lists.newArrayList(
          new ThreeColumnRecord(4, "b", "B"),
          new ThreeColumnRecord(5, "bb", "B"),
          new ThreeColumnRecord(6, "bbb", "B"));
  private List<ThreeColumnRecord> records3 =
      Lists.newArrayList(
          new ThreeColumnRecord(7, "ab", "A"),
          new ThreeColumnRecord(8, "aab", "A"),
          new ThreeColumnRecord(9, "aaab", "A"));

  @AfterEach
  public void dropTable() {
    validationCatalog.dropTable(tableIdent, true);
  }

  private Table createTable() throws NoSuchTableException {
    Map<String, String> properties =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.DELETE_MODE,
            RowLevelOperationMode.MERGE_ON_READ.modeName());
    Table table = validationCatalog.createTable(tableIdent, SCHEMA, SPEC, properties);

    Dataset<Row> df1 = spark.createDataFrame(records1, ThreeColumnRecord.class).coalesce(1);
    df1.writeTo(tableName).append();

    Dataset<Row> df2 = spark.createDataFrame(records2, ThreeColumnRecord.class).coalesce(1);
    df2.writeTo(tableName).append();

    table.refresh();
    return table;
  }

  private List<String> snapshotFiles(long snapshotId) {
    String query =
        String.format("SELECT file_path FROM %s.files VERSION AS OF %d", tableName, snapshotId);
    return spark.sql(query).as(Encoders.STRING()).collectAsList();
  }

  // In snapshot 1, we wrote one data file with the records in records1.
  // Delete this file from the storage and return its path.
  private String deleteDataFile(Table table) throws IOException {
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    List<String> snapshot1Files = snapshotFiles(snapshots.get(0).snapshotId());
    assertThat(snapshot1Files).hasSize(1);
    String pathString = snapshot1Files.get(0);
    Path dataFilePath = new Path(pathString);
    FileSystem fs = dataFilePath.getFileSystem(spark.sessionState().newHadoopConf());
    fs.delete(dataFilePath, false);
    return pathString;
  }

  // This is called after snapshot 3, when we have written 3 data files, containing the records in
  // records1, records2 and records3 respectively.
  // This creates one position delete file with deletes for two data files, and then deletes the
  // first of those data files. The path of the deleted data file is returned.
  private String deleteSomeRowsAndDeleteDataFile(Table table) throws IOException {
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    assertThat(snapshots).hasSize(3);
    List<String> snapshot1Files = snapshotFiles(snapshots.get(0).snapshotId());
    assertThat(snapshot1Files).hasSize(1);
    String dataFile1 = snapshot1Files.get(0);
    List<String> snapshot2Files = snapshotFiles(snapshots.get(1).snapshotId());
    List<String> snapshot3Files = snapshotFiles(snapshots.get(2).snapshotId());
    List<String> snapshot3OnlyFiles =
        snapshot3Files.stream()
            .filter(f -> !snapshot2Files.contains(f))
            .collect(Collectors.toList());
    assertThat(snapshot3OnlyFiles).hasSize(1);
    String dataFile3 = snapshot3OnlyFiles.get(0);
    List<String> fileLocations = ImmutableList.of(dataFile1, dataFile3);
    List<DataFile> dataFiles =
        TestHelpers.dataFiles(table).stream()
            .filter(f -> fileLocations.contains(f.location()))
            .collect(Collectors.toList());
    // both data files are in the same partition
    StructLike partition = dataFiles.get(0).partition();
    String fileLocation =
        table.locationProvider().newDataLocation(SPEC, partition, "test.deletes.parquet");

    // delete row 3 and row 8
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    deletes.add(Pair.of(dataFile1, 2L));
    deletes.add(Pair.of(dataFile3, 1L));

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table, table.io().newOutputFile(fileLocation), partition, deletes);
    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    // Now delete data file 1
    Path dataFilePath = new Path(dataFile1);
    FileSystem fs = dataFilePath.getFileSystem(spark.sessionState().newHadoopConf());
    fs.delete(dataFilePath, false);
    return dataFile1;
  }

  // This is called after we wrote one delete
  private String deleteDeleteFile(Table table) throws IOException {
    String query = String.format("SELECT file_path FROM %s.files WHERE content = 1", tableName);
    List<String> deleteFiles = spark.sql(query).as(Encoders.STRING()).collectAsList();
    assertThat(deleteFiles.size()).as("There is one delete file").isEqualTo(1);
    String pathString = deleteFiles.get(0);
    Path deleteFilePath = new Path(pathString);
    FileSystem fs = deleteFilePath.getFileSystem(spark.sessionState().newHadoopConf());
    fs.delete(deleteFilePath, false);
    return pathString;
  }

  @TestTemplate
  public void testMissingDataFile() throws Exception {
    Table table = createTable();
    String pathString = deleteDataFile(table);

    SparkActions actions = SparkActions.get(spark);
    RemoveMissingFiles.Result result = actions.removeMissingFiles(table).execute();
    List<String> removedDataFiles = Lists.newArrayList(result.removedDataFiles());
    assertThat(removedDataFiles.size()).as("Removed one data file").isEqualTo(1);
    assertThat(removedDataFiles.get(0))
        .as("Removed data file is the missing file")
        .isEqualTo(pathString);

    Dataset<Row> resultDF = spark.sql(String.format("SELECT * FROM %s ORDER by c1", tableName));
    List<ThreeColumnRecord> actualRecords =
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    assertThat(actualRecords).as("Rows must match").isEqualTo(records2);
  }

  @TestTemplate
  public void testMissingDataFileWithDelete() throws Exception {
    Table table = createTable();
    spark.sql(String.format("DELETE from %s WHERE c1 = 3", tableName));

    table.refresh();
    String pathString = deleteDataFile(table);

    SparkActions actions = SparkActions.get(spark);
    RemoveMissingFiles.Result result = actions.removeMissingFiles(table).execute();
    List<String> removedDataFiles = Lists.newArrayList(result.removedDataFiles());
    assertThat(removedDataFiles.size()).as("Removed one data file").isEqualTo(1);
    assertThat(removedDataFiles.get(0))
        .as("Removed data file is the missing file")
        .isEqualTo(pathString);
    List<String> removedDeleteFiles = Lists.newArrayList(result.removedDeleteFiles());
    assertThat(removedDeleteFiles.size()).as("Removed no delete files").isEqualTo(0);

    Dataset<Row> resultDF = spark.sql(String.format("SELECT * FROM %s ORDER by c1", tableName));
    List<ThreeColumnRecord> actualRecords =
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    assertThat(actualRecords).as("Rows must match").isEqualTo(records2);
  }

  @TestTemplate
  public void testDanglingDeletes() throws Exception {
    Table table = createTable();
    Dataset<Row> df3 = spark.createDataFrame(records3, ThreeColumnRecord.class).coalesce(1);
    df3.writeTo(tableName).append();

    table.refresh();
    // delete rows 3 and 8
    String pathString = deleteSomeRowsAndDeleteDataFile(table);

    SparkActions actions = SparkActions.get(spark);
    RemoveMissingFiles.Result result = actions.removeMissingFiles(table).execute();
    List<String> removedDataFiles = Lists.newArrayList(result.removedDataFiles());
    assertThat(removedDataFiles.size()).as("Removed one data file").isEqualTo(1);
    assertThat(removedDataFiles.get(0))
        .as("Removed data file is the missing file")
        .isEqualTo(pathString);
    // The delete file now has a dangling delete (referencing the removed data file) along with a
    // valid delete (referencing the data file for snapshot 3). It is not removed.
    List<String> removedDeleteFiles = Lists.newArrayList(result.removedDeleteFiles());
    assertThat(removedDeleteFiles.size()).as("Removed no delete files").isEqualTo(0);

    // Dangling deletes do not prevent the table from being read
    Dataset<Row> resultDF = spark.sql(String.format("SELECT c1 FROM %s ORDER by c1", tableName));
    List<Integer> actualRecords = resultDF.as(Encoders.INT()).collectAsList();
    // rows 1, 2, 3 are removed from the table; 4, 5, 6, 7, 9 remain (row 8 has been deleted)
    assertThat(actualRecords).as("Rows must match").isEqualTo(ImmutableList.of(4, 5, 6, 7, 9));
  }

  @TestTemplate
  public void testMissingDeleteFile() throws Exception {
    Table table = createTable();
    spark.sql(String.format("DELETE from %s WHERE c1 = 6", tableName));

    table.refresh();
    String pathString = deleteDeleteFile(table);

    SparkActions actions = SparkActions.get(spark);
    RemoveMissingFiles.Result result = actions.removeMissingFiles(table).execute();
    List<String> removedDataFiles = Lists.newArrayList(result.removedDataFiles());
    assertThat(removedDataFiles.size()).as("Removed no data files").isEqualTo(0);
    List<String> removedDeleteFiles = Lists.newArrayList(result.removedDeleteFiles());
    assertThat(removedDeleteFiles.size()).as("Removed one delete file").isEqualTo(1);
    assertThat(removedDeleteFiles.get(0))
        .as("Removed delete file is the missing file")
        .isEqualTo(pathString);

    // After removing the missing delete file, the deleted row is "undeleted"
    Dataset<Row> resultDF = spark.sql(String.format("SELECT * FROM %s ORDER by c1", tableName));
    List<ThreeColumnRecord> actualRecords =
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    List<ThreeColumnRecord> expectedRecords =
        Stream.of(records1, records2).flatMap(List::stream).collect(Collectors.toList());
    assertThat(actualRecords).as("Rows must match").isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testNoMissingFiles() throws Exception {
    Table table = createTable();
    spark.sql(String.format("DELETE from %s WHERE c1 = 6", tableName));
    table.refresh();

    SparkActions actions = SparkActions.get(spark);
    RemoveMissingFiles.Result result = actions.removeMissingFiles(table).execute();
    List<String> removedDataFiles = Lists.newArrayList(result.removedDataFiles());
    assertThat(removedDataFiles.size()).as("Removed no data files").isEqualTo(0);
    List<String> removedDeleteFiles = Lists.newArrayList(result.removedDeleteFiles());
    assertThat(removedDeleteFiles.size()).as("Removed no delete files").isEqualTo(0);
  }
}
