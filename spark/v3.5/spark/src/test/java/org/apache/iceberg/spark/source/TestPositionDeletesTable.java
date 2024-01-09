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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.PositionDeletesRewriteCoordinator;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkStructLike;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPositionDeletesTable extends CatalogTestBase {

  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));
  private static final Map<String, String> CATALOG_PROPS =
      ImmutableMap.of(
          "type", "hive",
          "default-namespace", "default",
          "cache-enabled", "false");
  private static final List<String> NON_PATH_COLS =
      ImmutableList.of("file_path", "pos", "row", "partition", "spec_id");

  @Parameter(index = 3)
  private FileFormat format;

  @Parameters(name = "catalogName = {1}, implementation = {2}, config = {3}, fileFormat = {4}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        CATALOG_PROPS,
        FileFormat.PARQUET
      },
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        CATALOG_PROPS,
        FileFormat.AVRO
      },
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        CATALOG_PROPS,
        FileFormat.ORC
      },
    };
  }

  @TempDir private Path temp;

  @TestTemplate
  public void testNullRows() throws IOException {
    String tableName = "null_rows";
    Table tab = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());

    DataFile dFile = dataFile(tab);
    tab.newAppend().appendFile(dFile).commit();

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    deletes.add(Pair.of(dFile.path(), 0L));
    deletes.add(Pair.of(dFile.path(), 1L));
    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            tab,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes);
    tab.newRowDelta().addDeletes(posDeletes.first()).commit();

    StructLikeSet actual = actual(tableName, tab);

    List<PositionDelete<?>> expectedDeletes =
        Lists.newArrayList(positionDelete(dFile.path(), 0L), positionDelete(dFile.path(), 1L));
    StructLikeSet expected =
        expected(tab, expectedDeletes, null, posDeletes.first().path().toString());

    assertThat(actual).as("Position Delete table should contain expected rows").isEqualTo(expected);
    dropTable(tableName);
  }

  @TestTemplate
  public void testPartitionedTable() throws IOException {
    // Create table with two partitions
    String tableName = "partitioned_table";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, spec);

    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");

    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    // Add position deletes for both partitions
    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileB, "b");

    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Select deletes from one partition
    StructLikeSet actual = actual(tableName, tab, "row.data='b'");
    GenericRecord partitionB = GenericRecord.create(tab.spec().partitionType());
    partitionB.setField("data", "b");
    StructLikeSet expected =
        expected(tab, deletesB.first(), partitionB, deletesB.second().path().toString());

    assertThat(actual).as("Position Delete table should contain expected rows").isEqualTo(expected);
    dropTable(tableName);
  }

  @TestTemplate
  public void testSelect() throws IOException {
    // Create table with two partitions
    String tableName = "select";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, spec);

    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");

    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    // Add position deletes for both partitions
    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileB, "b");

    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Select certain columns
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load("default." + tableName + ".position_deletes")
            .withColumn("input_file", functions.input_file_name())
            .select("row.id", "pos", "delete_file_path", "input_file");
    List<Object[]> actual = rowsToJava(df.collectAsList());

    // Select cols from expected delete values
    List<Object[]> expected = Lists.newArrayList();
    BiFunction<PositionDelete<?>, DeleteFile, Object[]> toRow =
        (delete, file) -> {
          int rowData = delete.get(2, GenericRecord.class).get(0, Integer.class);
          long pos = delete.get(1, Long.class);
          return row(rowData, pos, file.path().toString(), file.path().toString());
        };
    expected.addAll(
        deletesA.first().stream()
            .map(d -> toRow.apply(d, deletesA.second()))
            .collect(Collectors.toList()));
    expected.addAll(
        deletesB.first().stream()
            .map(d -> toRow.apply(d, deletesB.second()))
            .collect(Collectors.toList()));

    // Sort and compare
    Comparator<Object[]> comp =
        (o1, o2) -> {
          int result = Integer.compare((int) o1[0], (int) o2[0]);
          if (result != 0) {
            return result;
          } else {
            return ((String) o1[2]).compareTo((String) o2[2]);
          }
        };
    actual.sort(comp);
    expected.sort(comp);
    assertThat(actual)
        .as("Position Delete table should contain expected rows")
        .usingRecursiveComparison()
        .isEqualTo(expected);
    dropTable(tableName);
  }

  @TestTemplate
  public void testSplitTasks() throws IOException {
    String tableName = "big_table";
    Table tab = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());
    tab.updateProperties().set("read.split.target-size", "100").commit();
    int records = 500;

    GenericRecord record = GenericRecord.create(tab.schema());
    List<org.apache.iceberg.data.Record> dataRecords = Lists.newArrayList();
    for (int i = 0; i < records; i++) {
      dataRecords.add(record.copy("id", i, "data", String.valueOf(i)));
    }
    DataFile dFile =
        FileHelpers.writeDataFile(
            tab,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            org.apache.iceberg.TestHelpers.Row.of(),
            dataRecords);
    tab.newAppend().appendFile(dFile).commit();

    List<PositionDelete<?>> deletes = Lists.newArrayList();
    for (long i = 0; i < records; i++) {
      deletes.add(positionDelete(tab.schema(), dFile.path(), i, (int) i, String.valueOf(i)));
    }
    DeleteFile posDeletes =
        FileHelpers.writePosDeleteFile(
            tab,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes);
    tab.newRowDelta().addDeletes(posDeletes).commit();

    Table deleteTable =
        MetadataTableUtils.createMetadataTableInstance(tab, MetadataTableType.POSITION_DELETES);

    if (format.equals(FileFormat.AVRO)) {
      assertThat(deleteTable.newBatchScan().planTasks())
          .as("Position delete scan should produce more than one split")
          .hasSizeGreaterThan(1);
    } else {
      assertThat(deleteTable.newBatchScan().planTasks())
          .as("Position delete scan should produce one split")
          .hasSize(1);
    }

    StructLikeSet actual = actual(tableName, tab);
    StructLikeSet expected = expected(tab, deletes, null, posDeletes.path().toString());

    assertThat(actual).as("Position Delete table should contain expected rows").isEqualTo(expected);
    dropTable(tableName);
  }

  @TestTemplate
  public void testPartitionFilter() throws IOException {
    // Create table with two partitions
    String tableName = "partition_filter";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, spec);
    Table deletesTab =
        MetadataTableUtils.createMetadataTableInstance(tab, MetadataTableType.POSITION_DELETES);

    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    // Add position deletes for both partitions
    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileA, "b");

    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Prepare expected values
    GenericRecord partitionRecordTemplate = GenericRecord.create(tab.spec().partitionType());
    Record partitionA = partitionRecordTemplate.copy("data", "a");
    Record partitionB = partitionRecordTemplate.copy("data", "b");
    StructLikeSet expectedA =
        expected(tab, deletesA.first(), partitionA, deletesA.second().path().toString());
    StructLikeSet expectedB =
        expected(tab, deletesB.first(), partitionB, deletesB.second().path().toString());
    StructLikeSet allExpected = StructLikeSet.create(deletesTab.schema().asStruct());
    allExpected.addAll(expectedA);
    allExpected.addAll(expectedB);

    // Select deletes from all partitions
    StructLikeSet actual = actual(tableName, tab);
    assertThat(actual)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(allExpected);

    // Select deletes from one partition
    StructLikeSet actual2 = actual(tableName, tab, "partition.data = 'a' AND pos >= 0");

    assertThat(actual2)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedA);
    dropTable(tableName);
  }

  @TestTemplate
  public void testPartitionTransformFilter() throws IOException {
    // Create table with two partitions
    String tableName = "partition_filter";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate("data", 1).build();
    Table tab = createTable(tableName, SCHEMA, spec);
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(tab, MetadataTableType.POSITION_DELETES);

    DataFile dataFileA = dataFile(tab, new Object[] {"aa"}, new Object[] {"a"});
    DataFile dataFileB = dataFile(tab, new Object[] {"bb"}, new Object[] {"b"});
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    // Add position deletes for both partitions
    Pair<List<PositionDelete<?>>, DeleteFile> deletesA =
        deleteFile(tab, dataFileA, new Object[] {"aa"}, new Object[] {"a"});
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB =
        deleteFile(tab, dataFileA, new Object[] {"bb"}, new Object[] {"b"});
    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Prepare expected values
    GenericRecord partitionRecordTemplate = GenericRecord.create(tab.spec().partitionType());
    Record partitionA = partitionRecordTemplate.copy("data_trunc", "a");
    Record partitionB = partitionRecordTemplate.copy("data_trunc", "b");
    StructLikeSet expectedA =
        expected(tab, deletesA.first(), partitionA, deletesA.second().path().toString());
    StructLikeSet expectedB =
        expected(tab, deletesB.first(), partitionB, deletesB.second().path().toString());
    StructLikeSet allExpected = StructLikeSet.create(deletesTable.schema().asStruct());
    allExpected.addAll(expectedA);
    allExpected.addAll(expectedB);

    // Select deletes from all partitions
    StructLikeSet actual = actual(tableName, tab);
    assertThat(actual)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(allExpected);

    // Select deletes from one partition
    StructLikeSet actual2 = actual(tableName, tab, "partition.data_trunc = 'a' AND pos >= 0");

    assertThat(actual2)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedA);
    dropTable(tableName);
  }

  @TestTemplate
  public void testPartitionEvolutionReplace() throws Exception {
    // Create table with spec (data)
    String tableName = "partition_evolution";
    PartitionSpec originalSpec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, originalSpec);
    int dataSpec = tab.spec().specId();

    // Add files with old spec (data)
    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();
    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileA, "b");
    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Switch partition spec from (data) to (id)
    tab.updateSpec().removeField("data").addField("id").commit();

    // Add data and delete files with new spec (id)
    DataFile dataFile10 = dataFile(tab, 10);
    DataFile dataFile99 = dataFile(tab, 99);
    tab.newAppend().appendFile(dataFile10).appendFile(dataFile99).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletes10 = deleteFile(tab, dataFile10, 10);
    Pair<List<PositionDelete<?>>, DeleteFile> deletes99 = deleteFile(tab, dataFile10, 99);
    tab.newRowDelta().addDeletes(deletes10.second()).addDeletes(deletes99.second()).commit();

    // Query partition of old spec
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    Record partitionA = partitionRecordTemplate.copy("data", "a");
    StructLikeSet expectedA =
        expected(tab, deletesA.first(), partitionA, dataSpec, deletesA.second().path().toString());
    StructLikeSet actualA = actual(tableName, tab, "partition.data = 'a' AND pos >= 0");
    assertThat(actualA)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedA);

    // Query partition of new spec
    Record partition10 = partitionRecordTemplate.copy("id", 10);
    StructLikeSet expected10 =
        expected(
            tab,
            deletes10.first(),
            partition10,
            tab.spec().specId(),
            deletes10.second().path().toString());
    StructLikeSet actual10 = actual(tableName, tab, "partition.id = 10 AND pos >= 0");

    assertThat(actual10)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expected10);
    dropTable(tableName);
  }

  @TestTemplate
  public void testPartitionEvolutionAdd() throws Exception {
    // Create unpartitioned table
    String tableName = "partition_evolution_add";
    Table tab = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());
    int specId0 = tab.spec().specId();

    // Add files with unpartitioned spec
    DataFile dataFileUnpartitioned = dataFile(tab);
    tab.newAppend().appendFile(dataFileUnpartitioned).commit();
    Pair<List<PositionDelete<?>>, DeleteFile> deletesUnpartitioned =
        deleteFile(tab, dataFileUnpartitioned);
    tab.newRowDelta().addDeletes(deletesUnpartitioned.second()).commit();

    // Switch partition spec to (data)
    tab.updateSpec().addField("data").commit();
    int specId1 = tab.spec().specId();

    // Add files with new spec (data)
    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileB, "b");
    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Select deletes from new spec (data)
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    Record partitionA = partitionRecordTemplate.copy("data", "a");
    StructLikeSet expectedA =
        expected(tab, deletesA.first(), partitionA, specId1, deletesA.second().path().toString());
    StructLikeSet actualA = actual(tableName, tab, "partition.data = 'a' AND pos >= 0");
    assertThat(actualA)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedA);

    // Select deletes from 'unpartitioned' data
    Record unpartitionedRecord = partitionRecordTemplate.copy("data", null);
    StructLikeSet expectedUnpartitioned =
        expected(
            tab,
            deletesUnpartitioned.first(),
            unpartitionedRecord,
            specId0,
            deletesUnpartitioned.second().path().toString());
    StructLikeSet actualUnpartitioned =
        actual(tableName, tab, "partition.data IS NULL and pos >= 0");

    assertThat(actualUnpartitioned)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedUnpartitioned);
    dropTable(tableName);
  }

  @TestTemplate
  public void testPartitionEvolutionRemove() throws Exception {
    // Create table with spec (data)
    String tableName = "partition_evolution_remove";
    PartitionSpec originalSpec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, originalSpec);
    int specId0 = tab.spec().specId();

    // Add files with spec (data)
    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileB, "b");
    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Remove partition field
    tab.updateSpec().removeField("data").commit();
    int specId1 = tab.spec().specId();

    // Add unpartitioned files
    DataFile dataFileUnpartitioned = dataFile(tab);
    tab.newAppend().appendFile(dataFileUnpartitioned).commit();
    Pair<List<PositionDelete<?>>, DeleteFile> deletesUnpartitioned =
        deleteFile(tab, dataFileUnpartitioned);
    tab.newRowDelta().addDeletes(deletesUnpartitioned.second()).commit();

    // Select deletes from (data) spec
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    Record partitionA = partitionRecordTemplate.copy("data", "a");
    StructLikeSet expectedA =
        expected(tab, deletesA.first(), partitionA, specId0, deletesA.second().path().toString());
    StructLikeSet actualA = actual(tableName, tab, "partition.data = 'a' AND pos >= 0");
    assertThat(actualA)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedA);

    // Select deletes from 'unpartitioned' spec
    Record unpartitionedRecord = partitionRecordTemplate.copy("data", null);
    StructLikeSet expectedUnpartitioned =
        expected(
            tab,
            deletesUnpartitioned.first(),
            unpartitionedRecord,
            specId1,
            deletesUnpartitioned.second().path().toString());
    StructLikeSet actualUnpartitioned =
        actual(tableName, tab, "partition.data IS NULL and pos >= 0");

    assertThat(actualUnpartitioned)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedUnpartitioned);
    dropTable(tableName);
  }

  @TestTemplate
  public void testSpecIdFilter() throws Exception {
    // Create table with spec (data)
    String tableName = "spec_id_filter";
    Table tab = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());
    int unpartitionedSpec = tab.spec().specId();

    // Add data file and delete
    DataFile dataFileUnpartitioned = dataFile(tab);
    tab.newAppend().appendFile(dataFileUnpartitioned).commit();
    Pair<List<PositionDelete<?>>, DeleteFile> deletesUnpartitioned =
        deleteFile(tab, dataFileUnpartitioned);
    tab.newRowDelta().addDeletes(deletesUnpartitioned.second()).commit();

    // Switch partition spec to (data) and add files
    tab.updateSpec().addField("data").commit();
    int dataSpec = tab.spec().specId();

    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileB, "b");
    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Select deletes from 'unpartitioned'
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    StructLikeSet expectedUnpartitioned =
        expected(
            tab,
            deletesUnpartitioned.first(),
            partitionRecordTemplate,
            unpartitionedSpec,
            deletesUnpartitioned.second().path().toString());
    StructLikeSet actualUnpartitioned =
        actual(tableName, tab, String.format("spec_id = %d", unpartitionedSpec));
    assertThat(actualUnpartitioned)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedUnpartitioned);

    // Select deletes from 'data' partition spec
    StructLike partitionA = partitionRecordTemplate.copy("data", "a");
    StructLike partitionB = partitionRecordTemplate.copy("data", "b");
    StructLikeSet expected =
        expected(tab, deletesA.first(), partitionA, dataSpec, deletesA.second().path().toString());
    expected.addAll(
        expected(tab, deletesB.first(), partitionB, dataSpec, deletesB.second().path().toString()));

    StructLikeSet actual = actual(tableName, tab, String.format("spec_id = %d", dataSpec));
    assertThat(actual).as("Position Delete table should contain expected rows").isEqualTo(expected);
    dropTable(tableName);
  }

  @TestTemplate
  public void testSchemaEvolutionAdd() throws Exception {
    // Create table with original schema
    String tableName = "schema_evolution_add";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, spec);

    // Add files with original schema
    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileB, "b");
    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Add files with new schema
    tab.updateSchema()
        .addColumn("new_col_1", Types.IntegerType.get())
        .addColumn("new_col_2", Types.IntegerType.get())
        .commit();

    // Add files with new schema
    DataFile dataFileC = dataFile(tab, "c");
    DataFile dataFileD = dataFile(tab, "d");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesC = deleteFile(tab, dataFileC, "c");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesD = deleteFile(tab, dataFileD, "d");
    tab.newRowDelta().addDeletes(deletesC.second()).addDeletes(deletesD.second()).commit();

    // Select deletes from old schema
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    Record partitionA = partitionRecordTemplate.copy("data", "a");
    // pad expected delete rows with null values for new columns
    List<PositionDelete<?>> expectedDeletesA = deletesA.first();
    expectedDeletesA.forEach(
        d -> {
          GenericRecord nested = d.get(2, GenericRecord.class);
          GenericRecord padded = GenericRecord.create(tab.schema().asStruct());
          padded.set(0, nested.get(0));
          padded.set(1, nested.get(1));
          padded.set(2, null);
          padded.set(3, null);
          d.set(2, padded);
        });
    StructLikeSet expectedA =
        expected(tab, expectedDeletesA, partitionA, deletesA.second().path().toString());
    StructLikeSet actualA = actual(tableName, tab, "partition.data = 'a' AND pos >= 0");
    assertThat(actualA)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedA);

    // Select deletes from new schema
    Record partitionC = partitionRecordTemplate.copy("data", "c");
    StructLikeSet expectedC =
        expected(tab, deletesC.first(), partitionC, deletesC.second().path().toString());
    StructLikeSet actualC = actual(tableName, tab, "partition.data = 'c' and pos >= 0");

    assertThat(actualC)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedC);
    dropTable(tableName);
  }

  @TestTemplate
  public void testSchemaEvolutionRemove() throws Exception {
    // Create table with original schema
    String tableName = "schema_evolution_remove";
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "new_col_1", Types.IntegerType.get()),
            Types.NestedField.optional(4, "new_col_2", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(oldSchema).identity("data").build();
    Table tab = createTable(tableName, oldSchema, spec);

    // Add files with original schema
    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileB, "b");
    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Add files with new schema
    tab.updateSchema().deleteColumn("new_col_1").deleteColumn("new_col_2").commit();

    // Add files with new schema
    DataFile dataFileC = dataFile(tab, "c");
    DataFile dataFileD = dataFile(tab, "d");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesC = deleteFile(tab, dataFileC, "c");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesD = deleteFile(tab, dataFileD, "d");
    tab.newRowDelta().addDeletes(deletesC.second()).addDeletes(deletesD.second()).commit();

    // Select deletes from old schema
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    Record partitionA = partitionRecordTemplate.copy("data", "a");
    // remove deleted columns from expected result
    List<PositionDelete<?>> expectedDeletesA = deletesA.first();
    expectedDeletesA.forEach(
        d -> {
          GenericRecord nested = d.get(2, GenericRecord.class);
          GenericRecord padded = GenericRecord.create(tab.schema().asStruct());
          padded.set(0, nested.get(0));
          padded.set(1, nested.get(1));
          d.set(2, padded);
        });
    StructLikeSet expectedA =
        expected(tab, expectedDeletesA, partitionA, deletesA.second().path().toString());
    StructLikeSet actualA = actual(tableName, tab, "partition.data = 'a' AND pos >= 0");
    assertThat(actualA)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedA);

    // Select deletes from new schema
    Record partitionC = partitionRecordTemplate.copy("data", "c");
    StructLikeSet expectedC =
        expected(tab, deletesC.first(), partitionC, deletesC.second().path().toString());
    StructLikeSet actualC = actual(tableName, tab, "partition.data = 'c' and pos >= 0");

    assertThat(actualC)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedC);
    dropTable(tableName);
  }

  @TestTemplate
  public void testWrite() throws IOException, NoSuchTableException {
    String tableName = "test_write";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, spec);

    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    // Add position deletes for both partitions
    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileB, "b");

    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    Table posDeletesTable =
        MetadataTableUtils.createMetadataTableInstance(tab, MetadataTableType.POSITION_DELETES);
    String posDeletesTableName = catalogName + ".default." + tableName + ".position_deletes";
    for (String partValue : ImmutableList.of("a", "b")) {
      try (CloseableIterable<ScanTask> tasks = tasks(posDeletesTable, "data", partValue)) {
        String fileSetID = UUID.randomUUID().toString();
        stageTask(tab, fileSetID, tasks);

        Dataset<Row> scanDF =
            spark
                .read()
                .format("iceberg")
                .option(SparkReadOptions.SCAN_TASK_SET_ID, fileSetID)
                .option(SparkReadOptions.FILE_OPEN_COST, Integer.MAX_VALUE)
                .load(posDeletesTableName);

        assertThat(scanDF.javaRDD().getNumPartitions()).isEqualTo(1);
        scanDF
            .writeTo(posDeletesTableName)
            .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
            .append();

        commit(tab, posDeletesTable, fileSetID, 1);
      }
    }

    // Prepare expected values (without 'delete_file_path' as these have been rewritten)
    GenericRecord partitionRecordTemplate = GenericRecord.create(tab.spec().partitionType());
    Record partitionA = partitionRecordTemplate.copy("data", "a");
    Record partitionB = partitionRecordTemplate.copy("data", "b");
    StructLikeSet expectedA = expected(tab, deletesA.first(), partitionA, null);
    StructLikeSet expectedB = expected(tab, deletesB.first(), partitionB, null);
    StructLikeSet allExpected =
        StructLikeSet.create(
            TypeUtil.selectNot(
                    posDeletesTable.schema(), ImmutableSet.of(MetadataColumns.FILE_PATH_COLUMN_ID))
                .asStruct());
    allExpected.addAll(expectedA);
    allExpected.addAll(expectedB);

    // Compare values without 'delete_file_path' as these have been rewritten
    StructLikeSet actual = actual(tableName, tab, null, NON_PATH_COLS);
    assertThat(actual)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(allExpected);
    dropTable(tableName);
  }

  @TestTemplate
  public void testWriteUnpartitionedNullRows() throws Exception {
    String tableName = "write_null_rows";
    Table tab = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());

    DataFile dFile = dataFile(tab);
    tab.newAppend().appendFile(dFile).commit();

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    deletes.add(Pair.of(dFile.path(), 0L));
    deletes.add(Pair.of(dFile.path(), 1L));
    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            tab,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes);
    tab.newRowDelta().addDeletes(posDeletes.first()).commit();

    Table posDeletesTable =
        MetadataTableUtils.createMetadataTableInstance(tab, MetadataTableType.POSITION_DELETES);
    String posDeletesTableName = catalogName + ".default." + tableName + ".position_deletes";
    try (CloseableIterable<ScanTask> tasks = posDeletesTable.newBatchScan().planFiles()) {
      String fileSetID = UUID.randomUUID().toString();
      stageTask(tab, fileSetID, tasks);

      Dataset<Row> scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, fileSetID)
              .option(SparkReadOptions.FILE_OPEN_COST, Integer.MAX_VALUE)
              .load(posDeletesTableName);
      assertThat(scanDF.javaRDD().getNumPartitions()).isEqualTo(1);
      scanDF
          .writeTo(posDeletesTableName)
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
          .append();

      commit(tab, posDeletesTable, fileSetID, 1);
    }

    // Compare values without 'delete_file_path' as these have been rewritten
    StructLikeSet actual =
        actual(tableName, tab, null, ImmutableList.of("file_path", "pos", "row", "spec_id"));

    List<PositionDelete<?>> expectedDeletes =
        Lists.newArrayList(positionDelete(dFile.path(), 0L), positionDelete(dFile.path(), 1L));
    StructLikeSet expected = expected(tab, expectedDeletes, null, null);

    assertThat(actual).as("Position Delete table should contain expected rows").isEqualTo(expected);
    dropTable(tableName);
  }

  @TestTemplate
  public void testWriteMixedRows() throws Exception {
    String tableName = "write_mixed_rows";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, spec);

    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    // Add a delete file with row and without row
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    deletes.add(Pair.of(dataFileA.path(), 0L));
    deletes.add(Pair.of(dataFileA.path(), 1L));
    Pair<DeleteFile, CharSequenceSet> deletesWithoutRow =
        FileHelpers.writeDeleteFile(
            tab,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of("a"),
            deletes);

    Pair<List<PositionDelete<?>>, DeleteFile> deletesWithRow = deleteFile(tab, dataFileB, "b");

    tab.newRowDelta()
        .addDeletes(deletesWithoutRow.first())
        .addDeletes(deletesWithRow.second())
        .commit();

    // rewrite delete files
    Table posDeletesTable =
        MetadataTableUtils.createMetadataTableInstance(tab, MetadataTableType.POSITION_DELETES);
    String posDeletesTableName = catalogName + ".default." + tableName + ".position_deletes";
    for (String partValue : ImmutableList.of("a", "b")) {
      try (CloseableIterable<ScanTask> tasks = tasks(posDeletesTable, "data", partValue)) {
        String fileSetID = UUID.randomUUID().toString();
        stageTask(tab, fileSetID, tasks);

        Dataset<Row> scanDF =
            spark
                .read()
                .format("iceberg")
                .option(SparkReadOptions.SCAN_TASK_SET_ID, fileSetID)
                .load(posDeletesTableName);
        assertThat(scanDF.javaRDD().getNumPartitions()).isEqualTo(1);
        scanDF
            .writeTo(posDeletesTableName)
            .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
            .append();

        commit(tab, posDeletesTable, fileSetID, 1);
      }
    }

    // Compare values without 'delete_file_path' as these have been rewritten
    StructLikeSet actual =
        actual(
            tableName,
            tab,
            null,
            ImmutableList.of("file_path", "pos", "row", "partition", "spec_id"));

    // Prepare expected values
    GenericRecord partitionRecordTemplate = GenericRecord.create(tab.spec().partitionType());
    Record partitionA = partitionRecordTemplate.copy("data", "a");
    Record partitionB = partitionRecordTemplate.copy("data", "b");
    StructLikeSet allExpected =
        StructLikeSet.create(
            TypeUtil.selectNot(
                    posDeletesTable.schema(), ImmutableSet.of(MetadataColumns.FILE_PATH_COLUMN_ID))
                .asStruct());
    allExpected.addAll(
        expected(
            tab,
            Lists.newArrayList(
                positionDelete(dataFileA.path(), 0L), positionDelete(dataFileA.path(), 1L)),
            partitionA,
            null));
    allExpected.addAll(expected(tab, deletesWithRow.first(), partitionB, null));

    assertThat(actual)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(allExpected);
    dropTable(tableName);
  }

  @TestTemplate
  public void testWritePartitionEvolutionAdd() throws Exception {
    // Create unpartitioned table
    String tableName = "write_partition_evolution_add";
    Table tab = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());
    int specId0 = tab.spec().specId();

    // Add files with unpartitioned spec
    DataFile dataFileUnpartitioned = dataFile(tab);
    tab.newAppend().appendFile(dataFileUnpartitioned).commit();
    Pair<List<PositionDelete<?>>, DeleteFile> deletesUnpartitioned =
        deleteFile(tab, dataFileUnpartitioned);
    tab.newRowDelta().addDeletes(deletesUnpartitioned.second()).commit();

    // Switch partition spec to (data)
    tab.updateSpec().addField("data").commit();
    int specId1 = tab.spec().specId();

    // Add files with new spec (data)
    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileB, "b");
    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    Table posDeletesTable =
        MetadataTableUtils.createMetadataTableInstance(tab, MetadataTableType.POSITION_DELETES);
    String posDeletesTableName = catalogName + ".default." + tableName + ".position_deletes";

    // Read/write back unpartitioned data
    try (CloseableIterable<ScanTask> tasks =
        posDeletesTable.newBatchScan().filter(Expressions.isNull("partition.data")).planFiles()) {
      String fileSetID = UUID.randomUUID().toString();
      stageTask(tab, fileSetID, tasks);

      Dataset<Row> scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, fileSetID)
              .option(SparkReadOptions.FILE_OPEN_COST, Integer.MAX_VALUE)
              .load(posDeletesTableName);
      assertThat(scanDF.javaRDD().getNumPartitions()).isEqualTo(1);
      scanDF
          .writeTo(posDeletesTableName)
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
          .append();

      commit(tab, posDeletesTable, fileSetID, 1);
    }

    // Select deletes from unpartitioned data
    // Compare values without 'delete_file_path' as these have been rewritten
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    Record unpartitionedRecord = partitionRecordTemplate.copy("data", null);
    StructLikeSet expectedUnpartitioned =
        expected(tab, deletesUnpartitioned.first(), unpartitionedRecord, specId0, null);
    StructLikeSet actualUnpartitioned =
        actual(tableName, tab, "partition.data IS NULL", NON_PATH_COLS);
    assertThat(actualUnpartitioned)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedUnpartitioned);

    // Read/write back new partition spec (data)
    for (String partValue : ImmutableList.of("a", "b")) {
      try (CloseableIterable<ScanTask> tasks = tasks(posDeletesTable, "data", partValue)) {
        String fileSetID = UUID.randomUUID().toString();
        stageTask(tab, fileSetID, tasks);

        Dataset<Row> scanDF =
            spark
                .read()
                .format("iceberg")
                .option(SparkReadOptions.SCAN_TASK_SET_ID, fileSetID)
                .option(SparkReadOptions.FILE_OPEN_COST, Integer.MAX_VALUE)
                .load(posDeletesTableName);
        assertThat(scanDF.javaRDD().getNumPartitions()).isEqualTo(1);
        scanDF
            .writeTo(posDeletesTableName)
            .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
            .append();

        // commit the rewrite
        commit(tab, posDeletesTable, fileSetID, 1);
      }
    }

    // Select deletes from new spec (data)
    Record partitionA = partitionRecordTemplate.copy("data", "a");
    Record partitionB = partitionRecordTemplate.copy("data", "b");
    StructLikeSet expectedAll =
        StructLikeSet.create(
            TypeUtil.selectNot(
                    posDeletesTable.schema(), ImmutableSet.of(MetadataColumns.FILE_PATH_COLUMN_ID))
                .asStruct());
    expectedAll.addAll(expected(tab, deletesA.first(), partitionA, specId1, null));
    expectedAll.addAll(expected(tab, deletesB.first(), partitionB, specId1, null));
    StructLikeSet actualAll =
        actual(tableName, tab, "partition.data = 'a' OR partition.data = 'b'", NON_PATH_COLS);
    assertThat(actualAll)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedAll);
    dropTable(tableName);
  }

  @TestTemplate
  public void testWritePartitionEvolutionDisallowed() throws Exception {
    // Create unpartitioned table
    String tableName = "write_partition_evolution_write";
    Table tab = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());

    // Add files with unpartitioned spec
    DataFile dataFileUnpartitioned = dataFile(tab);
    tab.newAppend().appendFile(dataFileUnpartitioned).commit();
    Pair<List<PositionDelete<?>>, DeleteFile> deletesUnpartitioned =
        deleteFile(tab, dataFileUnpartitioned);
    tab.newRowDelta().addDeletes(deletesUnpartitioned.second()).commit();

    Table posDeletesTable =
        MetadataTableUtils.createMetadataTableInstance(tab, MetadataTableType.POSITION_DELETES);
    String posDeletesTableName = catalogName + ".default." + tableName + ".position_deletes";

    Dataset<Row> scanDF;
    String fileSetID = UUID.randomUUID().toString();
    try (CloseableIterable<ScanTask> tasks = posDeletesTable.newBatchScan().planFiles()) {
      stageTask(tab, fileSetID, tasks);

      scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, fileSetID)
              .option(SparkReadOptions.FILE_OPEN_COST, Integer.MAX_VALUE)
              .load(posDeletesTableName);
      assertThat(scanDF.javaRDD().getNumPartitions()).isEqualTo(1);

      // Add partition field to render the original un-partitioned dataset un-commitable
      tab.updateSpec().addField("data").commit();
    }

    assertThatThrownBy(
            () ->
                scanDF
                    .writeTo(posDeletesTableName)
                    .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
                    .append())
        .isInstanceOf(AnalysisException.class)
        .hasMessage(
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for the table `"
                + catalogName
                + "`.`default`.`"
                + tableName
                + "`.`position_deletes`"
                + ": Cannot find data for the output column `partition`.");

    dropTable(tableName);
  }

  @TestTemplate
  public void testWriteSchemaEvolutionAdd() throws Exception {
    // Create table with original schema
    String tableName = "write_schema_evolution_add";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, spec);

    // Add files with original schema
    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileB, "b");
    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Add files with new schema
    tab.updateSchema()
        .addColumn("new_col_1", Types.IntegerType.get())
        .addColumn("new_col_2", Types.IntegerType.get())
        .commit();

    // Add files with new schema
    DataFile dataFileC = dataFile(tab, "c");
    DataFile dataFileD = dataFile(tab, "d");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesC = deleteFile(tab, dataFileC, "c");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesD = deleteFile(tab, dataFileD, "d");
    tab.newRowDelta().addDeletes(deletesC.second()).addDeletes(deletesD.second()).commit();

    Table posDeletesTable =
        MetadataTableUtils.createMetadataTableInstance(tab, MetadataTableType.POSITION_DELETES);
    String posDeletesTableName = catalogName + ".default." + tableName + ".position_deletes";

    // rewrite files of old schema
    try (CloseableIterable<ScanTask> tasks = tasks(posDeletesTable, "data", "a")) {
      String fileSetID = UUID.randomUUID().toString();
      stageTask(tab, fileSetID, tasks);

      Dataset<Row> scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, fileSetID)
              .option(SparkReadOptions.FILE_OPEN_COST, Integer.MAX_VALUE)
              .load(posDeletesTableName);

      assertThat(scanDF.javaRDD().getNumPartitions()).isEqualTo(1);
      scanDF
          .writeTo(posDeletesTableName)
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
          .append();

      commit(tab, posDeletesTable, fileSetID, 1);
    }

    // Select deletes from old schema
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    Record partitionA = partitionRecordTemplate.copy("data", "a");
    // pad expected delete rows with null values for new columns
    List<PositionDelete<?>> expectedDeletesA = deletesA.first();
    expectedDeletesA.forEach(
        d -> {
          GenericRecord nested = d.get(2, GenericRecord.class);
          GenericRecord padded = GenericRecord.create(tab.schema().asStruct());
          padded.set(0, nested.get(0));
          padded.set(1, nested.get(1));
          padded.set(2, null);
          padded.set(3, null);
          d.set(2, padded);
        });
    StructLikeSet expectedA = expected(tab, expectedDeletesA, partitionA, null);
    StructLikeSet actualA = actual(tableName, tab, "partition.data = 'a'", NON_PATH_COLS);
    assertThat(actualA)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedA);

    // rewrite files of new schema
    try (CloseableIterable<ScanTask> tasks = tasks(posDeletesTable, "data", "c")) {
      String fileSetID = UUID.randomUUID().toString();
      stageTask(tab, fileSetID, tasks);

      Dataset<Row> scanDF =
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SCAN_TASK_SET_ID, fileSetID)
              .option(SparkReadOptions.FILE_OPEN_COST, Integer.MAX_VALUE)
              .load(posDeletesTableName);

      assertThat(scanDF.javaRDD().getNumPartitions()).isEqualTo(1);
      scanDF
          .writeTo(posDeletesTableName)
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
          .append();

      commit(tab, posDeletesTable, fileSetID, 1);
    }

    // Select deletes from new schema
    Record partitionC = partitionRecordTemplate.copy("data", "c");
    StructLikeSet expectedC = expected(tab, deletesC.first(), partitionC, null);
    StructLikeSet actualC = actual(tableName, tab, "partition.data = 'c'", NON_PATH_COLS);

    assertThat(actualC)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedC);
    dropTable(tableName);
  }

  @TestTemplate
  public void testWriteSchemaEvolutionRemove() throws Exception {
    // Create table with original schema
    String tableName = "write_schema_evolution_remove";
    Schema oldSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "new_col_1", Types.IntegerType.get()),
            Types.NestedField.optional(4, "new_col_2", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(oldSchema).identity("data").build();
    Table tab = createTable(tableName, oldSchema, spec);

    // Add files with original schema
    DataFile dataFileA = dataFile(tab, "a");
    DataFile dataFileB = dataFile(tab, "b");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesB = deleteFile(tab, dataFileB, "b");
    tab.newRowDelta().addDeletes(deletesA.second()).addDeletes(deletesB.second()).commit();

    // Add files with new schema
    tab.updateSchema().deleteColumn("new_col_1").deleteColumn("new_col_2").commit();

    // Add files with new schema
    DataFile dataFileC = dataFile(tab, "c");
    DataFile dataFileD = dataFile(tab, "d");
    tab.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesC = deleteFile(tab, dataFileC, "c");
    Pair<List<PositionDelete<?>>, DeleteFile> deletesD = deleteFile(tab, dataFileD, "d");
    tab.newRowDelta().addDeletes(deletesC.second()).addDeletes(deletesD.second()).commit();

    Table posDeletesTable =
        MetadataTableUtils.createMetadataTableInstance(tab, MetadataTableType.POSITION_DELETES);
    String posDeletesTableName = catalogName + ".default." + tableName + ".position_deletes";

    // rewrite files
    for (String partValue : ImmutableList.of("a", "b", "c", "d")) {
      try (CloseableIterable<ScanTask> tasks = tasks(posDeletesTable, "data", partValue)) {
        String fileSetID = UUID.randomUUID().toString();
        stageTask(tab, fileSetID, tasks);

        Dataset<Row> scanDF =
            spark
                .read()
                .format("iceberg")
                .option(SparkReadOptions.SCAN_TASK_SET_ID, fileSetID)
                .option(SparkReadOptions.FILE_OPEN_COST, Integer.MAX_VALUE)
                .load(posDeletesTableName);
        assertThat(scanDF.javaRDD().getNumPartitions()).isEqualTo(1);
        scanDF
            .writeTo(posDeletesTableName)
            .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, fileSetID)
            .append();

        commit(tab, posDeletesTable, fileSetID, 1);
      }
    }

    // Select deletes from old schema
    GenericRecord partitionRecordTemplate = GenericRecord.create(Partitioning.partitionType(tab));
    Record partitionA = partitionRecordTemplate.copy("data", "a");
    // remove deleted columns from expected result
    List<PositionDelete<?>> expectedDeletesA = deletesA.first();
    expectedDeletesA.forEach(
        d -> {
          GenericRecord nested = d.get(2, GenericRecord.class);
          GenericRecord padded = GenericRecord.create(tab.schema().asStruct());
          padded.set(0, nested.get(0));
          padded.set(1, nested.get(1));
          d.set(2, padded);
        });
    StructLikeSet expectedA = expected(tab, expectedDeletesA, partitionA, null);
    StructLikeSet actualA = actual(tableName, tab, "partition.data = 'a'", NON_PATH_COLS);
    assertThat(actualA)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedA);

    // Select deletes from new schema
    Record partitionC = partitionRecordTemplate.copy("data", "c");
    StructLikeSet expectedC = expected(tab, deletesC.first(), partitionC, null);
    StructLikeSet actualC = actual(tableName, tab, "partition.data = 'c'", NON_PATH_COLS);

    assertThat(actualC)
        .as("Position Delete table should contain expected rows")
        .isEqualTo(expectedC);
    dropTable(tableName);
  }

  @TestTemplate
  public void testNormalWritesNotAllowed() throws IOException {
    String tableName = "test_normal_write_not_allowed";
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table tab = createTable(tableName, SCHEMA, spec);

    DataFile dataFileA = dataFile(tab, "a");
    tab.newAppend().appendFile(dataFileA).commit();

    Pair<List<PositionDelete<?>>, DeleteFile> deletesA = deleteFile(tab, dataFileA, "a");
    tab.newRowDelta().addDeletes(deletesA.second()).commit();

    String posDeletesTableName = catalogName + ".default." + tableName + ".position_deletes";

    Dataset<Row> scanDF = spark.read().format("iceberg").load(posDeletesTableName);

    assertThatThrownBy(() -> scanDF.writeTo(posDeletesTableName).append())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Can only write to " + posDeletesTableName + " via actions");

    dropTable(tableName);
  }

  private StructLikeSet actual(String tableName, Table table) {
    return actual(tableName, table, null, null);
  }

  private StructLikeSet actual(String tableName, Table table, String filter) {
    return actual(tableName, table, filter, null);
  }

  private StructLikeSet actual(String tableName, Table table, String filter, List<String> cols) {
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .load(catalogName + ".default." + tableName + ".position_deletes");
    if (filter != null) {
      df = df.filter(filter);
    }
    if (cols != null) {
      df = df.select(cols.get(0), cols.subList(1, cols.size()).toArray(new String[0]));
    }
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);
    Types.StructType projection = deletesTable.schema().asStruct();
    if (cols != null) {
      projection =
          Types.StructType.of(
              projection.fields().stream()
                  .filter(f -> cols.contains(f.name()))
                  .collect(Collectors.toList()));
    }
    Types.StructType finalProjection = projection;
    StructLikeSet set = StructLikeSet.create(projection);
    df.collectAsList()
        .forEach(
            row -> {
              SparkStructLike rowWrapper = new SparkStructLike(finalProjection);
              set.add(rowWrapper.wrap(row));
            });

    return set;
  }

  protected Table createTable(String name, Schema schema, PartitionSpec spec) {
    Map<String, String> properties =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            "2",
            TableProperties.DEFAULT_FILE_FORMAT,
            format.toString());
    return validationCatalog.createTable(
        TableIdentifier.of("default", name), schema, spec, properties);
  }

  protected void dropTable(String name) {
    validationCatalog.dropTable(TableIdentifier.of("default", name), false);
  }

  private PositionDelete<GenericRecord> positionDelete(CharSequence path, Long position) {
    PositionDelete<GenericRecord> posDelete = PositionDelete.create();
    posDelete.set(path, position, null);
    return posDelete;
  }

  private PositionDelete<GenericRecord> positionDelete(
      Schema tableSchema, CharSequence path, Long position, Object... values) {
    PositionDelete<GenericRecord> posDelete = PositionDelete.create();
    GenericRecord nested = GenericRecord.create(tableSchema);
    for (int i = 0; i < values.length; i++) {
      nested.set(i, values[i]);
    }
    posDelete.set(path, position, nested);
    return posDelete;
  }

  private StructLikeSet expected(
      Table testTable,
      List<PositionDelete<?>> deletes,
      StructLike partitionStruct,
      int specId,
      String deleteFilePath) {
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(
            testTable, MetadataTableType.POSITION_DELETES);
    Types.StructType posDeleteSchema = deletesTable.schema().asStruct();
    // Do not compare file paths
    if (deleteFilePath == null) {
      posDeleteSchema =
          TypeUtil.selectNot(
                  deletesTable.schema(), ImmutableSet.of(MetadataColumns.FILE_PATH_COLUMN_ID))
              .asStruct();
    }
    final Types.StructType finalSchema = posDeleteSchema;
    StructLikeSet set = StructLikeSet.create(posDeleteSchema);
    deletes.stream()
        .map(
            p -> {
              GenericRecord record = GenericRecord.create(finalSchema);
              record.setField("file_path", p.path());
              record.setField("pos", p.pos());
              record.setField("row", p.row());
              if (partitionStruct != null) {
                record.setField("partition", partitionStruct);
              }
              record.setField("spec_id", specId);
              if (deleteFilePath != null) {
                record.setField("delete_file_path", deleteFilePath);
              }
              return record;
            })
        .forEach(set::add);
    return set;
  }

  private StructLikeSet expected(
      Table testTable,
      List<PositionDelete<?>> deletes,
      StructLike partitionStruct,
      String deleteFilePath) {
    return expected(testTable, deletes, partitionStruct, testTable.spec().specId(), deleteFilePath);
  }

  private DataFile dataFile(Table tab, Object... partValues) throws IOException {
    return dataFile(tab, partValues, partValues);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private DataFile dataFile(Table tab, Object[] partDataValues, Object[] partFieldValues)
      throws IOException {
    GenericRecord record = GenericRecord.create(tab.schema());
    List<String> partitionFieldNames =
        tab.spec().fields().stream().map(PartitionField::name).collect(Collectors.toList());
    int idIndex = partitionFieldNames.indexOf("id");
    int dataIndex = partitionFieldNames.indexOf("data");
    Integer idPartition = idIndex != -1 ? (Integer) partDataValues[idIndex] : null;
    String dataPartition = dataIndex != -1 ? (String) partDataValues[dataIndex] : null;

    // fill columns with partition source fields, or preset values
    List<Record> records =
        Lists.newArrayList(
            record.copy(
                "id",
                idPartition != null ? idPartition : 29,
                "data",
                dataPartition != null ? dataPartition : "c"),
            record.copy(
                "id",
                idPartition != null ? idPartition : 43,
                "data",
                dataPartition != null ? dataPartition : "k"),
            record.copy(
                "id",
                idPartition != null ? idPartition : 61,
                "data",
                dataPartition != null ? dataPartition : "r"),
            record.copy(
                "id",
                idPartition != null ? idPartition : 89,
                "data",
                dataPartition != null ? dataPartition : "t"));

    // fill remaining columns with incremental values
    List<Types.NestedField> cols = tab.schema().columns();
    if (cols.size() > 2) {
      for (int i = 2; i < cols.size(); i++) {
        final int pos = i;
        records.forEach(r -> r.set(pos, pos));
      }
    }

    TestHelpers.Row partitionInfo = TestHelpers.Row.of(partFieldValues);
    return FileHelpers.writeDataFile(
        tab,
        Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
        partitionInfo,
        records);
  }

  private Pair<List<PositionDelete<?>>, DeleteFile> deleteFile(
      Table tab, DataFile dataFile, Object... partValues) throws IOException {
    return deleteFile(tab, dataFile, partValues, partValues);
  }

  private Pair<List<PositionDelete<?>>, DeleteFile> deleteFile(
      Table tab, DataFile dataFile, Object[] partDataValues, Object[] partFieldValues)
      throws IOException {
    List<PartitionField> partFields = tab.spec().fields();
    List<String> partitionFieldNames =
        partFields.stream().map(PartitionField::name).collect(Collectors.toList());
    int idIndex = partitionFieldNames.indexOf("id");
    int dataIndex = partitionFieldNames.indexOf("data");
    Integer idPartition = idIndex != -1 ? (Integer) partDataValues[idIndex] : null;
    String dataPartition = dataIndex != -1 ? (String) partDataValues[dataIndex] : null;

    // fill columns with partition source fields, or preset values
    List<PositionDelete<?>> deletes =
        Lists.newArrayList(
            positionDelete(
                tab.schema(),
                dataFile.path(),
                0L,
                idPartition != null ? idPartition : 29,
                dataPartition != null ? dataPartition : "c"),
            positionDelete(
                tab.schema(),
                dataFile.path(),
                1L,
                idPartition != null ? idPartition : 61,
                dataPartition != null ? dataPartition : "r"));

    // fill remaining columns with incremental values
    List<Types.NestedField> cols = tab.schema().columns();
    if (cols.size() > 2) {
      for (int i = 2; i < cols.size(); i++) {
        final int pos = i;
        deletes.forEach(d -> d.get(2, GenericRecord.class).set(pos, pos));
      }
    }

    TestHelpers.Row partitionInfo = TestHelpers.Row.of(partFieldValues);

    DeleteFile deleteFile =
        FileHelpers.writePosDeleteFile(
            tab,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            partitionInfo,
            deletes);
    return Pair.of(deletes, deleteFile);
  }

  private <T extends ScanTask> void stageTask(
      Table tab, String fileSetID, CloseableIterable<T> tasks) {
    ScanTaskSetManager taskSetManager = ScanTaskSetManager.get();
    taskSetManager.stageTasks(tab, fileSetID, Lists.newArrayList(tasks));
  }

  private void commit(
      Table baseTab,
      Table posDeletesTable,
      String fileSetID,
      int expectedSourceFiles,
      int expectedTargetFiles) {
    PositionDeletesRewriteCoordinator rewriteCoordinator = PositionDeletesRewriteCoordinator.get();
    Set<DeleteFile> rewrittenFiles =
        ScanTaskSetManager.get().fetchTasks(posDeletesTable, fileSetID).stream()
            .map(t -> ((PositionDeletesScanTask) t).file())
            .collect(Collectors.toSet());
    Set<DeleteFile> addedFiles = rewriteCoordinator.fetchNewFiles(posDeletesTable, fileSetID);

    // Assert new files and old files are equal in number but different in paths
    assertThat(rewrittenFiles).hasSize(expectedSourceFiles);
    assertThat(addedFiles).hasSize(expectedTargetFiles);

    List<String> sortedAddedFiles =
        addedFiles.stream().map(f -> f.path().toString()).sorted().collect(Collectors.toList());
    List<String> sortedRewrittenFiles =
        rewrittenFiles.stream().map(f -> f.path().toString()).sorted().collect(Collectors.toList());
    assertThat(sortedRewrittenFiles)
        .as("Lists should not be the same")
        .isNotEqualTo(sortedAddedFiles);

    baseTab
        .newRewrite()
        .rewriteFiles(ImmutableSet.of(), rewrittenFiles, ImmutableSet.of(), addedFiles)
        .commit();
  }

  private void commit(Table baseTab, Table posDeletesTable, String fileSetID, int expectedFiles) {
    commit(baseTab, posDeletesTable, fileSetID, expectedFiles, expectedFiles);
  }

  private CloseableIterable<ScanTask> tasks(
      Table posDeletesTable, String partitionColumn, String partitionValue) {

    Expression filter = Expressions.equal("partition." + partitionColumn, partitionValue);
    CloseableIterable<ScanTask> files = posDeletesTable.newBatchScan().filter(filter).planFiles();

    // take care of fail to filter in some partition evolution cases
    return CloseableIterable.filter(
        files,
        t -> {
          StructLike filePartition = ((PositionDeletesScanTask) t).partition();
          String filePartitionValue = filePartition.get(0, String.class);
          return filePartitionValue != null && filePartitionValue.equals(partitionValue);
        });
  }
}
