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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileGenerationUtil;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionEntry;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.ComputePartitionStats;
import org.apache.iceberg.data.PartitionStatsUtil;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestComputePartitionStatsActionPerf extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("c1").build();

  private String tableLocation = null;

  @TempDir private Path temp;

  @BeforeEach
  public void setupTableLocation() throws Exception {
    File tableDir = temp.resolve("junit").toFile();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testPerf() {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);
    final int partitionCount = 20;
    final int datafilesPerPartitionCount = 10000;

    for (int partitionOrdinal = 0; partitionOrdinal < partitionCount; partitionOrdinal++) {
      StructLike partition = TestHelpers.Row.of(partitionOrdinal);

      AppendFiles appendFiles = table.newAppend();

      for (int fileOrdinal = 0; fileOrdinal < datafilesPerPartitionCount; fileOrdinal++) {
        DataFile dataFile = FileGenerationUtil.generateDataFile(table, partition);
        appendFiles.appendFile(dataFile);
      }

      appendFiles.commit();
    }

    List<String> validFiles =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation + "#files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();
    Assertions.assertThat(validFiles).hasSize(partitionCount * datafilesPerPartitionCount);
    Assertions.assertThat(table.partitionStatisticsFiles()).isEmpty();

    // -- local compute --------
    long base = System.currentTimeMillis();
    SparkActions actions = SparkActions.get();
    ComputePartitionStats.Result result =
        actions.computePartitionStatistics(table).localCompute(true).execute();
    Assertions.assertThat(table.partitionStatisticsFiles()).containsExactly(result.outputFile());
    System.out.println(
        "#### time taken for local compute in milli: " + (System.currentTimeMillis() - base));

    // read the partition entries from the stats file
    Schema schema =
        PartitionEntry.icebergSchema(Partitioning.partitionType(table.specs().values()));
    List<PartitionEntry> rows;
    try (CloseableIterable<PartitionEntry> recordIterator =
        PartitionStatsUtil.readPartitionStatsFile(
            schema, Files.localInput(result.outputFile().path()))) {
      rows = Lists.newArrayList(recordIterator);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Assertions.assertThat(rows.size()).isEqualTo(partitionCount);

    // ---- distributed compute ---
    table
        .updatePartitionStatistics()
        .removePartitionStatistics(result.outputFile().snapshotId())
        .commit();
    Assertions.assertThat(table.partitionStatisticsFiles()).isEmpty();

    base = System.currentTimeMillis();
    actions = SparkActions.get();
    result = actions.computePartitionStatistics(table).localCompute(false).execute();
    Assertions.assertThat(table.partitionStatisticsFiles()).containsExactly(result.outputFile());
    System.out.println(
        "#### time taken for distributed compute in milli: " + (System.currentTimeMillis() - base));

    // can't use PartitionStatsUtil.readPartitionStatsFile as it uses
    // ParquetAvroValueReaders$ReadBuilder
    // and since native parquet doesn't write column ids, reader throws NPE.
    List<Row> output =
        spark
            .read()
            .parquet(result.outputFile().path())
            .select("PARTITION_DATA", "DATA_RECORD_COUNT", "DATA_FILE_COUNT")
            .collectAsList();
    Assertions.assertThat(output.size()).isEqualTo(partitionCount);
  }
}
