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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileGenerationUtil;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.RemoveDanglingDeleteFiles;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRemoveDanglingDeleteAction extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.StringType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("c1").build();

  static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=a") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DataFile FILE_A2 =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=a") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=b") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DataFile FILE_B2 =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=b") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DataFile FILE_C =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-c.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=c") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DataFile FILE_C2 =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-c.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=c") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DataFile FILE_D =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-d.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=d") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DataFile FILE_D2 =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-d.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=d") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_A_POS_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-a-pos-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=a") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_A2_POS_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-a2-pos-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=a") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_A_EQ_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofEqualityDeletes()
          .withPath("/path/to/data-a-eq-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=a") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_A2_EQ_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofEqualityDeletes()
          .withPath("/path/to/data-a2-eq-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=a") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_B_POS_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-b-pos-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=b") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_B2_POS_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-b2-pos-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=b") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_B_EQ_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofEqualityDeletes()
          .withPath("/path/to/data-b-eq-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=b") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_B2_EQ_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofEqualityDeletes()
          .withPath("/path/to/data-b2-eq-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("c1=b") // easy way to set partition data for now
          .withRecordCount(1)
          .build();

  static final DataFile FILE_UNPARTITIONED =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-unpartitioned.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_UNPARTITIONED_POS_DELETE =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofEqualityDeletes()
          .withPath("/path/to/data-unpartitioned-pos-deletes.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();
  static final DeleteFile FILE_UNPARTITIONED_EQ_DELETE =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofEqualityDeletes()
          .withPath("/path/to/data-unpartitioned-eq-deletes.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();

  @TempDir private File tableDir;
  @Parameter private int formatVersion;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> parameters() {
    return TestHelpers.V2_AND_ABOVE;
  }

  private String tableLocation = null;
  private Table table;

  @BeforeEach
  public void before() throws Exception {
    this.tableLocation = tableDir.toURI().toString();
  }

  @AfterEach
  public void after() {
    TABLES.dropTable(tableLocation);
  }

  private void setupPartitionedTable() {
    this.table =
        TABLES.create(
            SCHEMA,
            SPEC,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            tableLocation);
  }

  private void setupUnpartitionedTable() {
    this.table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            tableLocation);
  }

  private DeleteFile fileADeletes() {
    return formatVersion >= 3 ? FileGenerationUtil.generateDV(table, FILE_A) : FILE_A_POS_DELETES;
  }

  private DeleteFile fileA2Deletes() {
    return formatVersion >= 3 ? FileGenerationUtil.generateDV(table, FILE_A2) : FILE_A2_POS_DELETES;
  }

  private DeleteFile fileBDeletes() {
    return formatVersion >= 3 ? FileGenerationUtil.generateDV(table, FILE_B) : FILE_B_POS_DELETES;
  }

  private DeleteFile fileB2Deletes() {
    return formatVersion >= 3 ? FileGenerationUtil.generateDV(table, FILE_B2) : FILE_B2_POS_DELETES;
  }

  private DeleteFile fileUnpartitionedDeletes() {
    return formatVersion >= 3
        ? FileGenerationUtil.generateDV(table, FILE_UNPARTITIONED)
        : FILE_UNPARTITIONED_POS_DELETE;
  }

  @TestTemplate
  public void testPartitionedDeletesWithLesserSeqNo() {
    setupPartitionedTable();

    // Add Data Files
    table.newAppend().appendFile(FILE_B).appendFile(FILE_C).appendFile(FILE_D).commit();

    // Add Delete Files
    DeleteFile fileADeletes = fileADeletes();
    DeleteFile fileA2Deletes = fileA2Deletes();
    DeleteFile fileBDeletes = fileBDeletes();
    DeleteFile fileB2Deletes = fileB2Deletes();
    table
        .newRowDelta()
        .addDeletes(fileADeletes)
        .addDeletes(fileA2Deletes)
        .addDeletes(fileBDeletes)
        .addDeletes(fileB2Deletes)
        .addDeletes(FILE_A_EQ_DELETES)
        .addDeletes(FILE_A2_EQ_DELETES)
        .addDeletes(FILE_B_EQ_DELETES)
        .addDeletes(FILE_B2_EQ_DELETES)
        .commit();

    // Add More Data Files
    table
        .newAppend()
        .appendFile(FILE_A2)
        .appendFile(FILE_B2)
        .appendFile(FILE_C2)
        .appendFile(FILE_D2)
        .commit();

    List<Tuple2<Long, String>> actual = allEntries();
    List<Tuple2<Long, String>> expected =
        ImmutableList.of(
            Tuple2.apply(1L, FILE_B.location()),
            Tuple2.apply(1L, FILE_C.location()),
            Tuple2.apply(1L, FILE_D.location()),
            Tuple2.apply(2L, FILE_A_EQ_DELETES.location()),
            Tuple2.apply(2L, fileADeletes.location()),
            Tuple2.apply(2L, FILE_A2_EQ_DELETES.location()),
            Tuple2.apply(2L, fileA2Deletes.location()),
            Tuple2.apply(2L, FILE_B_EQ_DELETES.location()),
            Tuple2.apply(2L, fileBDeletes.location()),
            Tuple2.apply(2L, FILE_B2_EQ_DELETES.location()),
            Tuple2.apply(2L, fileB2Deletes.location()),
            Tuple2.apply(3L, FILE_A2.location()),
            Tuple2.apply(3L, FILE_B2.location()),
            Tuple2.apply(3L, FILE_C2.location()),
            Tuple2.apply(3L, FILE_D2.location()));
    assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

    RemoveDanglingDeleteFiles.Result result =
        SparkActions.get().removeDanglingDeleteFiles(table).execute();

    // All Delete files of the FILE A partition should be removed
    // because there are no data files in partition with a lesser sequence number

    Set<CharSequence> removedDeleteFiles =
        StreamSupport.stream(result.removedDeleteFiles().spliterator(), false)
            .map(DeleteFile::location)
            .collect(Collectors.toSet());
    assertThat(removedDeleteFiles)
        .as("Expected 4 delete files removed")
        .hasSize(4)
        .containsExactlyInAnyOrder(
            fileADeletes.location(),
            fileA2Deletes.location(),
            FILE_A_EQ_DELETES.location(),
            FILE_A2_EQ_DELETES.location());

    List<Tuple2<Long, String>> actualAfter = liveEntries();
    List<Tuple2<Long, String>> expectedAfter =
        ImmutableList.of(
            Tuple2.apply(1L, FILE_B.location()),
            Tuple2.apply(1L, FILE_C.location()),
            Tuple2.apply(1L, FILE_D.location()),
            Tuple2.apply(2L, FILE_B_EQ_DELETES.location()),
            Tuple2.apply(2L, fileBDeletes.location()),
            Tuple2.apply(2L, FILE_B2_EQ_DELETES.location()),
            Tuple2.apply(2L, fileB2Deletes.location()),
            Tuple2.apply(3L, FILE_A2.location()),
            Tuple2.apply(3L, FILE_B2.location()),
            Tuple2.apply(3L, FILE_C2.location()),
            Tuple2.apply(3L, FILE_D2.location()));
    assertThat(actualAfter).containsExactlyInAnyOrderElementsOf(expectedAfter);
  }

  @TestTemplate
  public void testPartitionedDeletesWithEqSeqNo() {
    setupPartitionedTable();

    // Add Data Files
    table.newAppend().appendFile(FILE_A).appendFile(FILE_C).appendFile(FILE_D).commit();

    // Add Data Files with EQ and POS deletes
    DeleteFile fileADeletes = fileADeletes();
    DeleteFile fileA2Deletes = fileA2Deletes();
    DeleteFile fileBDeletes = fileBDeletes();
    DeleteFile fileB2Deletes = fileB2Deletes();
    table
        .newRowDelta()
        .addRows(FILE_A2)
        .addRows(FILE_B2)
        .addRows(FILE_C2)
        .addRows(FILE_D2)
        .addDeletes(fileADeletes)
        .addDeletes(fileA2Deletes)
        .addDeletes(FILE_A_EQ_DELETES)
        .addDeletes(FILE_A2_EQ_DELETES)
        .addDeletes(fileBDeletes)
        .addDeletes(fileB2Deletes)
        .addDeletes(FILE_B_EQ_DELETES)
        .addDeletes(FILE_B2_EQ_DELETES)
        .commit();

    List<Tuple2<Long, String>> actual = allEntries();
    List<Tuple2<Long, String>> expected =
        ImmutableList.of(
            Tuple2.apply(1L, FILE_A.location()),
            Tuple2.apply(1L, FILE_C.location()),
            Tuple2.apply(1L, FILE_D.location()),
            Tuple2.apply(2L, FILE_A_EQ_DELETES.location()),
            Tuple2.apply(2L, fileADeletes.location()),
            Tuple2.apply(2L, FILE_A2.location()),
            Tuple2.apply(2L, FILE_A2_EQ_DELETES.location()),
            Tuple2.apply(2L, fileA2Deletes.location()),
            Tuple2.apply(2L, FILE_B_EQ_DELETES.location()),
            Tuple2.apply(2L, fileBDeletes.location()),
            Tuple2.apply(2L, FILE_B2.location()),
            Tuple2.apply(2L, FILE_B2_EQ_DELETES.location()),
            Tuple2.apply(2L, fileB2Deletes.location()),
            Tuple2.apply(2L, FILE_C2.location()),
            Tuple2.apply(2L, FILE_D2.location()));
    assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

    RemoveDanglingDeleteFiles.Result result =
        SparkActions.get().removeDanglingDeleteFiles(table).execute();

    // Eq Delete files of the FILE B partition should be removed
    // because there are no data files in partition with a lesser sequence number
    Set<CharSequence> removedDeleteFiles =
        StreamSupport.stream(result.removedDeleteFiles().spliterator(), false)
            .map(DeleteFile::location)
            .collect(Collectors.toSet());
    assertThat(removedDeleteFiles)
        .as("Expected two delete files removed")
        .hasSize(2)
        .containsExactlyInAnyOrder(FILE_B_EQ_DELETES.location(), FILE_B2_EQ_DELETES.location());

    List<Tuple2<Long, String>> actualAfter = liveEntries();
    List<Tuple2<Long, String>> expectedAfter =
        ImmutableList.of(
            Tuple2.apply(1L, FILE_A.location()),
            Tuple2.apply(1L, FILE_C.location()),
            Tuple2.apply(1L, FILE_D.location()),
            Tuple2.apply(2L, FILE_A_EQ_DELETES.location()),
            Tuple2.apply(2L, fileADeletes.location()),
            Tuple2.apply(2L, FILE_A2.location()),
            Tuple2.apply(2L, FILE_A2_EQ_DELETES.location()),
            Tuple2.apply(2L, fileA2Deletes.location()),
            Tuple2.apply(2L, fileBDeletes.location()),
            Tuple2.apply(2L, FILE_B2.location()),
            Tuple2.apply(2L, fileB2Deletes.location()),
            Tuple2.apply(2L, FILE_C2.location()),
            Tuple2.apply(2L, FILE_D2.location()));
    assertThat(actualAfter).containsExactlyInAnyOrderElementsOf(expectedAfter);
  }

  @TestTemplate
  public void testUnpartitionedTable() {
    setupUnpartitionedTable();

    table
        .newRowDelta()
        .addDeletes(fileUnpartitionedDeletes())
        .addDeletes(FILE_UNPARTITIONED_EQ_DELETE)
        .commit();
    table.newAppend().appendFile(FILE_UNPARTITIONED).commit();

    RemoveDanglingDeleteFiles.Result result =
        SparkActions.get().removeDanglingDeleteFiles(table).execute();
    assertThat(result.removedDeleteFiles()).as("No-op for unpartitioned tables").isEmpty();
  }

  @TestTemplate
  public void testPartitionedDeletesWithDanglingDvs() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    setupPartitionedTable();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_C).appendFile(FILE_D).commit();

    DeleteFile fileADeletes = fileADeletes();
    DeleteFile fileBDeletes = fileBDeletes();
    DeleteFile fileB2Deletes = fileB2Deletes();
    table
        .newRowDelta()
        .addRows(FILE_A)
        .addRows(FILE_C)
        .addDeletes(fileADeletes)
        // since FILE B doesn't exist, these delete files will be dangling
        .addDeletes(fileBDeletes)
        .addDeletes(fileB2Deletes)
        .commit();

    List<Tuple2<Long, String>> actual = allEntries();
    List<Tuple2<Long, String>> expected =
        ImmutableList.of(
            Tuple2.apply(1L, FILE_A.location()),
            Tuple2.apply(1L, FILE_C.location()),
            Tuple2.apply(1L, FILE_D.location()),
            Tuple2.apply(2L, FILE_A.location()),
            Tuple2.apply(2L, FILE_C.location()),
            Tuple2.apply(2L, fileB2Deletes.location()),
            Tuple2.apply(2L, fileBDeletes.location()),
            Tuple2.apply(2L, fileADeletes.location()));
    assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

    RemoveDanglingDeleteFiles.Result result =
        SparkActions.get().removeDanglingDeleteFiles(table).execute();

    // DVs of FILE B should be removed because they don't point to valid data files
    Set<CharSequence> removedDeleteFiles =
        Lists.newArrayList(result.removedDeleteFiles()).stream()
            .map(DeleteFile::location)
            .collect(Collectors.toSet());
    assertThat(removedDeleteFiles)
        .as("Expected two delete files to be removed")
        .hasSize(2)
        .containsExactlyInAnyOrder(fileBDeletes.location(), fileB2Deletes.location());

    List<Tuple2<Long, String>> actualAfter = liveEntries();
    List<Tuple2<Long, String>> expectedAfter =
        ImmutableList.of(
            Tuple2.apply(1L, FILE_A.location()),
            Tuple2.apply(1L, FILE_C.location()),
            Tuple2.apply(1L, FILE_D.location()),
            Tuple2.apply(2L, FILE_A.location()),
            Tuple2.apply(2L, FILE_C.location()),
            Tuple2.apply(2L, fileADeletes.location()));
    assertThat(actualAfter).containsExactlyInAnyOrderElementsOf(expectedAfter);
  }

  private List<Tuple2<Long, String>> liveEntries() {
    return spark
        .read()
        .format("iceberg")
        .load(tableLocation + "#entries")
        .filter("status < 2") // live files
        .select("sequence_number", "data_file.file_path")
        .sort("sequence_number", "data_file.file_path")
        .as(Encoders.tuple(Encoders.LONG(), Encoders.STRING()))
        .collectAsList();
  }

  private List<Tuple2<Long, String>> allEntries() {
    return spark
        .read()
        .format("iceberg")
        .load(tableLocation + "#entries")
        .select("sequence_number", "data_file.file_path")
        .sort("sequence_number", "data_file.file_path")
        .as(Encoders.tuple(Encoders.LONG(), Encoders.STRING()))
        .collectAsList();
  }
}
