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

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RemoveDanglingDeleteFiles;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

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

  @TempDir private Path temp;

  private String tableLocation = null;
  private Table table;

  @BeforeEach
  public void before() throws Exception {
    File tableDir = temp.resolve("junit").toFile();
    this.tableLocation = tableDir.toURI().toString();
  }

  @AfterEach
  public void after() {
    TABLES.dropTable(tableLocation);
  }

  private void setupPartitionedTable() {
    this.table =
        TABLES.create(
            SCHEMA, SPEC, ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"), tableLocation);
  }

  private void setupUnpartitionedTable() {
    this.table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"),
            tableLocation);
  }

  @Test
  public void testPartitionedDeletesWithLesserSeqNo() {
    setupPartitionedTable();

    // Add Data Files
    table.newAppend().appendFile(FILE_B).appendFile(FILE_C).appendFile(FILE_D).commit();

    // Add Delete Files
    table
        .newRowDelta()
        .addDeletes(FILE_A_POS_DELETES)
        .addDeletes(FILE_A2_POS_DELETES)
        .addDeletes(FILE_B_POS_DELETES)
        .addDeletes(FILE_B2_POS_DELETES)
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

    List<Tuple2<Long, String>> actual =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation + "#entries")
            .select("sequence_number", "data_file.file_path")
            .sort("sequence_number", "data_file.file_path")
            .as(Encoders.tuple(Encoders.LONG(), Encoders.STRING()))
            .collectAsList();
    List<Tuple2<Long, String>> expected =
        ImmutableList.of(
            Tuple2.apply(1L, FILE_B.path().toString()),
            Tuple2.apply(1L, FILE_C.path().toString()),
            Tuple2.apply(1L, FILE_D.path().toString()),
            Tuple2.apply(2L, FILE_A_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_A_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_A2_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_A2_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B2_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B2_POS_DELETES.path().toString()),
            Tuple2.apply(3L, FILE_A2.path().toString()),
            Tuple2.apply(3L, FILE_B2.path().toString()),
            Tuple2.apply(3L, FILE_C2.path().toString()),
            Tuple2.apply(3L, FILE_D2.path().toString()));
    assertThat(actual).isEqualTo(expected);

    RemoveDanglingDeleteFiles.Result result =
        SparkActions.get().removeDanglingDeleteFiles(table).execute();

    // All Delete files of the FILE A partition should be removed
    // because there are no data files in partition with a lesser sequence number

    Set<CharSequence> removedDeleteFiles =
        StreamSupport.stream(result.removedDeleteFiles().spliterator(), false)
            .map(DeleteFile::path)
            .collect(Collectors.toSet());
    assertThat(removedDeleteFiles)
        .as("Expected 4 delete files removed")
        .hasSize(4)
        .containsExactlyInAnyOrder(
            FILE_A_POS_DELETES.path(),
            FILE_A2_POS_DELETES.path(),
            FILE_A_EQ_DELETES.path(),
            FILE_A2_EQ_DELETES.path());

    List<Tuple2<Long, String>> actualAfter =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation + "#entries")
            .filter("status < 2") // live files
            .select("sequence_number", "data_file.file_path")
            .sort("sequence_number", "data_file.file_path")
            .as(Encoders.tuple(Encoders.LONG(), Encoders.STRING()))
            .collectAsList();
    List<Tuple2<Long, String>> expectedAfter =
        ImmutableList.of(
            Tuple2.apply(1L, FILE_B.path().toString()),
            Tuple2.apply(1L, FILE_C.path().toString()),
            Tuple2.apply(1L, FILE_D.path().toString()),
            Tuple2.apply(2L, FILE_B_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B2_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B2_POS_DELETES.path().toString()),
            Tuple2.apply(3L, FILE_A2.path().toString()),
            Tuple2.apply(3L, FILE_B2.path().toString()),
            Tuple2.apply(3L, FILE_C2.path().toString()),
            Tuple2.apply(3L, FILE_D2.path().toString()));
    assertThat(actualAfter).isEqualTo(expectedAfter);
  }

  @Test
  public void testPartitionedDeletesWithEqSeqNo() {
    setupPartitionedTable();

    // Add Data Files
    table.newAppend().appendFile(FILE_A).appendFile(FILE_C).appendFile(FILE_D).commit();

    // Add Data Files with EQ and POS deletes
    table
        .newRowDelta()
        .addRows(FILE_A2)
        .addRows(FILE_B2)
        .addRows(FILE_C2)
        .addRows(FILE_D2)
        .addDeletes(FILE_A_POS_DELETES)
        .addDeletes(FILE_A2_POS_DELETES)
        .addDeletes(FILE_A_EQ_DELETES)
        .addDeletes(FILE_A2_EQ_DELETES)
        .addDeletes(FILE_B_POS_DELETES)
        .addDeletes(FILE_B2_POS_DELETES)
        .addDeletes(FILE_B_EQ_DELETES)
        .addDeletes(FILE_B2_EQ_DELETES)
        .commit();

    List<Tuple2<Long, String>> actual =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation + "#entries")
            .select("sequence_number", "data_file.file_path")
            .sort("sequence_number", "data_file.file_path")
            .as(Encoders.tuple(Encoders.LONG(), Encoders.STRING()))
            .collectAsList();
    List<Tuple2<Long, String>> expected =
        ImmutableList.of(
            Tuple2.apply(1L, FILE_A.path().toString()),
            Tuple2.apply(1L, FILE_C.path().toString()),
            Tuple2.apply(1L, FILE_D.path().toString()),
            Tuple2.apply(2L, FILE_A_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_A_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_A2.path().toString()),
            Tuple2.apply(2L, FILE_A2_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_A2_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B2.path().toString()),
            Tuple2.apply(2L, FILE_B2_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B2_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_C2.path().toString()),
            Tuple2.apply(2L, FILE_D2.path().toString()));
    assertThat(actual).isEqualTo(expected);

    RemoveDanglingDeleteFiles.Result result =
        SparkActions.get().removeDanglingDeleteFiles(table).execute();

    // Eq Delete files of the FILE B partition should be removed
    // because there are no data files in partition with a lesser sequence number
    Set<CharSequence> removedDeleteFiles =
        StreamSupport.stream(result.removedDeleteFiles().spliterator(), false)
            .map(DeleteFile::path)
            .collect(Collectors.toSet());
    assertThat(removedDeleteFiles)
        .as("Expected two delete files removed")
        .hasSize(2)
        .containsExactlyInAnyOrder(FILE_B_EQ_DELETES.path(), FILE_B2_EQ_DELETES.path());

    List<Tuple2<Long, String>> actualAfter =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation + "#entries")
            .filter("status < 2") // live files
            .select("sequence_number", "data_file.file_path")
            .sort("sequence_number", "data_file.file_path")
            .as(Encoders.tuple(Encoders.LONG(), Encoders.STRING()))
            .collectAsList();
    List<Tuple2<Long, String>> expectedAfter =
        ImmutableList.of(
            Tuple2.apply(1L, FILE_A.path().toString()),
            Tuple2.apply(1L, FILE_C.path().toString()),
            Tuple2.apply(1L, FILE_D.path().toString()),
            Tuple2.apply(2L, FILE_A_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_A_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_A2.path().toString()),
            Tuple2.apply(2L, FILE_A2_EQ_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_A2_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_B2.path().toString()),
            Tuple2.apply(2L, FILE_B2_POS_DELETES.path().toString()),
            Tuple2.apply(2L, FILE_C2.path().toString()),
            Tuple2.apply(2L, FILE_D2.path().toString()));
    assertThat(actualAfter).isEqualTo(expectedAfter);
  }

  @Test
  public void testUnpartitionedTable() {
    setupUnpartitionedTable();

    table
        .newRowDelta()
        .addDeletes(FILE_UNPARTITIONED_POS_DELETE)
        .addDeletes(FILE_UNPARTITIONED_EQ_DELETE)
        .commit();
    table.newAppend().appendFile(FILE_UNPARTITIONED).commit();

    RemoveDanglingDeleteFiles.Result result =
        SparkActions.get().removeDanglingDeleteFiles(table).execute();
    assertThat(result.removedDeleteFiles()).as("No-op for unpartitioned tables").isEmpty();
  }
}
