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
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionEntry;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputePartitionStats;
import org.apache.iceberg.data.PartitionStatsUtil;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.assertj.core.api.Assertions;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestComputePartitionStatsAction extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("c2").identity("c3").build();

  @Parameters(name = "localCompute = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(true, false);
  }

  @Parameter private Boolean localCompute;

  private String tableLocation = null;

  @TempDir private Path temp;

  @BeforeEach
  public void setupTableLocation() throws Exception {
    File tableDir = temp.resolve("junit").toFile();
    this.tableLocation = tableDir.toURI().toString();
  }

  @TestTemplate
  public void testPartitionTable() {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    // foo, A -> 3 records
    // foo, B -> 1 record
    // bar, A -> 1 record
    // bar, B -> 2 records
    List<ThreeColumnRecord> records =
        ImmutableList.of(
            new ThreeColumnRecord(1, "foo", "A"),
            new ThreeColumnRecord(2, "foo", "B"),
            new ThreeColumnRecord(3, "foo", "A"),
            new ThreeColumnRecord(4, "bar", "B"),
            new ThreeColumnRecord(5, "bar", "A"),
            new ThreeColumnRecord(6, "bar", "B"),
            new ThreeColumnRecord(7, "foo", "A"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    // insert twice
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    List<String> validFiles =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation + "#files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();
    Assertions.assertThat(validFiles).as("Should be 8 valid files. 4 files per insert").hasSize(8);
    Assertions.assertThat(table.partitionStatisticsFiles()).isEmpty();

    SparkActions actions = SparkActions.get();
    ComputePartitionStats.Result result =
        actions.computePartitionStatistics(table).localCompute(localCompute).execute();
    Assertions.assertThat(table.partitionStatisticsFiles()).containsExactly(result.outputFile());

    if (localCompute) {
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

      Types.StructType partitionType =
          schema.findField(PartitionEntry.Column.PARTITION_DATA.name()).type().asStructType();
      Assertions.assertThat(rows)
          .extracting(
              PartitionEntry::partitionData,
              PartitionEntry::dataRecordCount,
              PartitionEntry::dataFileCount)
          .containsExactlyInAnyOrder(
              Tuple.tuple(partitionData(partitionType, "foo", "A"), 6L, 2),
              Tuple.tuple(partitionData(partitionType, "foo", "B"), 2L, 2),
              Tuple.tuple(partitionData(partitionType, "bar", "A"), 2L, 2),
              Tuple.tuple(partitionData(partitionType, "bar", "B"), 4L, 2));
    } else {
      // can't use PartitionStatsUtil.readPartitionStatsFile as it uses
      // ParquetAvroValueReaders$ReadBuilder
      // and since native parquet doesn't write column ids, reader throws NPE.
      List<Row> rows =
          spark
              .read()
              .parquet(result.outputFile().path())
              .select("PARTITION_DATA", "DATA_RECORD_COUNT", "DATA_FILE_COUNT")
              .collectAsList();
      Assertions.assertThat(rows)
          .extracting(
              row -> ((GenericRowWithSchema) row.get(0)).values()[0],
              row -> ((GenericRowWithSchema) row.get(0)).values()[1],
              row -> row.getLong(1),
              row -> row.getLong(2))
          .containsExactlyInAnyOrder(
              Tuple.tuple("foo", "A", 6L, 2L),
              Tuple.tuple("foo", "B", 2L, 2L),
              Tuple.tuple("bar", "A", 2L, 2L),
              Tuple.tuple("bar", "B", 4L, 2L));
      // TODO: while aggregating, few Int datatype auto casted to Long. Finalize the data types.
    }
  }

  private static PartitionData partitionData(Types.StructType partitionType, String c2, String c3) {
    PartitionData partitionData = new PartitionData(partitionType);
    partitionData.set(0, c2);
    partitionData.set(1, c3);
    return partitionData;
  }

  // TODO: add the testcase with delete files (pos and equality)
  //  and also validate each and every field of partition stats.
}
