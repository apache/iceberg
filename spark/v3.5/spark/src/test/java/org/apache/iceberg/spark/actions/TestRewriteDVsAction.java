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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.Result;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestRewriteDVsAction extends CatalogTestBase {

  private static final String TABLE_NAME = "test_table";
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties()
      }
    };
  }

  @AfterEach
  public void cleanup() {
    validationCatalog.dropTable(TableIdentifier.of("default", TABLE_NAME));
  }

  @TestTemplate
  public void rewriteDVsOnEmptyTable() {
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME),
            SCHEMA,
            PartitionSpec.unpartitioned(),
            tableProperties());

    Result result = SparkActions.get(spark).rewriteDVs(table).execute();
    assertThat(result.rewrittenDeleteFilesCount()).as("No rewritten delete files").isZero();
    assertThat(result.addedDeleteFilesCount()).as("No added delete files").isZero();
  }

  @TestTemplate
  public void rewriteDVsOnV2Table() {
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME),
            SCHEMA,
            PartitionSpec.unpartitioned(),
            tableProperties(2));

    writeRecords(table, 1, 10);

    assertThatThrownBy(SparkActions.get(spark).rewriteDVs(table)::execute)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot rewrite DVs for v2 table");
  }

  @TestTemplate
  public void dvCompactionWithHighDeleteRatioOnUnpartitionedTable() throws IOException {
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME),
            SCHEMA,
            PartitionSpec.unpartitioned(),
            tableProperties(3));

    // write 10 records per data file
    writeRecords(table, 10, 100);

    dvCompactionWithHighDeleteRatio(table);
  }

  @TestTemplate
  public void dvCompactionWithHighDeleteRatioOnPartitionedTable() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME), SCHEMA, spec, tableProperties(3));

    // write 10 records per data file
    writeRecords(table, 10, 10, 10);

    dvCompactionWithHighDeleteRatio(table);
  }

  private void dvCompactionWithHighDeleteRatio(Table table) throws IOException {
    assertThat(records(table)).hasSize(100);
    assertThat(deleteRecords(table)).hasSize(0);

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, p -> null);

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    for (int i = 0; i < dataFiles.size(); i++) {
      DataFile dataFile = dataFiles.get(i);
      for (int j = 0; j < dataFile.recordCount(); j++) {
        if (i + 1 < dataFiles.size()) {
          writer.delete(dataFile.location(), j, table.spec(), dataFile.partition());
        } else if (j < 5) {
          // only delete 5 records from the last data file
          writer.delete(dataFile.location(), j, table.spec(), dataFile.partition());
        }
      }
    }

    writer.close();
    List<DeleteFile> deleteFiles = writer.result().deleteFiles();
    RowDelta rowDelta = table.newRowDelta();
    deleteFiles.forEach(rowDelta::addDeletes);
    rowDelta.commit();

    assertThat(records(table)).hasSize(5);
    assertThat(deleteRecords(table)).hasSize(95);

    RowDelta delta = table.newRowDelta();
    // expire 9 out of 10 delete files
    TestHelpers.deleteFiles(table).stream()
        .filter(del -> del.recordCount() == 10)
        .forEach(delta::removeDeletes);
    delta.commit();

    assertThat(records(table)).hasSize(95);
    assertThat(deleteRecords(table)).hasSize(5);

    String puffinLocationBeforeRewrite = deleteFiles.get(0).location();
    assertThat(
            Puffin.read(Files.localInput(puffinLocationBeforeRewrite))
                .build()
                .fileMetadata()
                .blobs())
        .hasSize(10);

    Result result = SparkActions.get(spark).rewriteDVs(table).execute();

    assertThat(result.rewrittenDeleteFilesCount()).isEqualTo(1);
    assertThat(result.addedDeleteFilesCount()).isEqualTo(1);

    Set<DeleteFile> files = TestHelpers.deleteFiles(table);
    assertThat(files).hasSize(1);

    // live ratio was 10%, so a new Puffin file has been written with only a single DV
    String puffinLocationAfterRewrite = Iterables.getOnlyElement(files).location();
    assertThat(puffinLocationAfterRewrite).isNotEqualTo(puffinLocationBeforeRewrite);
    assertThat(
            Puffin.read(Files.localInput(puffinLocationAfterRewrite))
                .build()
                .fileMetadata()
                .blobs())
        .hasSize(1);
  }

  @TestTemplate
  public void dvCompactionWithLowLiveRatio() throws IOException {
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of("default", TABLE_NAME),
            SCHEMA,
            PartitionSpec.unpartitioned(),
            tableProperties(3));

    // write 10 records per data file
    writeRecords(table, 10, 100);

    assertThat(records(table)).hasSize(100);
    assertThat(deleteRecords(table)).hasSize(0);

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, p -> null);

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    for (DataFile dataFile : dataFiles) {
      for (int j = 0; j < dataFile.recordCount(); j++) {
        if (j < 5) {
          // only delete 5 records from each data file
          writer.delete(dataFile.location(), j, table.spec(), dataFile.partition());
        }
      }
    }

    writer.close();
    List<DeleteFile> deleteFiles = writer.result().deleteFiles();
    RowDelta rowDelta = table.newRowDelta();
    deleteFiles.forEach(rowDelta::addDeletes);
    rowDelta.commit();

    assertThat(records(table)).hasSize(50);
    assertThat(deleteRecords(table)).hasSize(50);

    RowDelta delta = table.newRowDelta();
    // expire 5 out of 10 delete files
    ImmutableList.copyOf(TestHelpers.deleteFiles(table))
        .subList(0, 5)
        .forEach(delta::removeDeletes);
    delta.commit();

    assertThat(records(table)).hasSize(75);
    assertThat(deleteRecords(table)).hasSize(25);

    Set<DeleteFile> currentDeleteFiles = TestHelpers.deleteFiles(table);
    assertThat(currentDeleteFiles).hasSize(5);
    String puffinLocationBeforeRewrite = currentDeleteFiles.stream().findFirst().get().location();
    assertThat(
            Puffin.read(Files.localInput(puffinLocationBeforeRewrite))
                .build()
                .fileMetadata()
                .blobs())
        .hasSize(10);

    Result result =
        SparkActions.get(spark)
            .rewriteDVs(table)
            .option(SparkBinPackDVRewriter.DELETE_RATIO_THRESHOLD, "0.7")
            .execute();

    assertThat(result.rewrittenDeleteFilesCount()).isEqualTo(0);
    assertThat(result.addedDeleteFilesCount()).isEqualTo(0);

    Set<DeleteFile> filesAfterRewrite = TestHelpers.deleteFiles(table);
    assertThat(filesAfterRewrite).hasSize(5);

    // live ratio was 50%, so no new Puffin file should have been written
    String puffinLocationAfterRewrite = filesAfterRewrite.stream().findFirst().get().location();
    assertThat(puffinLocationAfterRewrite).isEqualTo(puffinLocationBeforeRewrite);
    assertThat(
            Puffin.read(Files.localInput(puffinLocationAfterRewrite))
                .build()
                .fileMetadata()
                .blobs())
        .hasSize(10);
  }

  private Map<String, String> tableProperties() {
    return tableProperties(3);
  }

  private Map<String, String> tableProperties(int formatVersion) {
    return ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
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
    assertThat(partitionTypeSize)
        .as("This method currently supports only two columns as partition columns")
        .isLessThanOrEqualTo(2);

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

  private String name(Table table) {
    String[] splits = table.name().split("\\.");

    assertThat(splits).hasSize(3);
    return String.format("%s.%s", splits[1], splits[2]);
  }
}
