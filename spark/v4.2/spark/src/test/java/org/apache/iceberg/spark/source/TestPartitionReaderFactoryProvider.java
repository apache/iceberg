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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestPartitionReaderFactoryProvider extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  @TempDir private File tableDir;
  @TempDir private File deleteDir;
  private String tableLocation;

  @BeforeEach
  public void before() {
    this.tableLocation = tableDir.toURI().toString();
    TestReaderFactoryProviders.Counting.reset();
  }

  @AfterEach
  public void after() {
    spark.conf().unset(SparkSQLProperties.PARTITION_READER_FACTORY_PROVIDER);
  }

  @Test
  public void testProviderNotUsedUnlessSelected() throws IOException {
    createTableWithDeletes();

    List<Row> rows = read(reader());

    assertThat(rows).hasSize(390);
    assertThat(TestReaderFactoryProviders.Counting.FACTORIES.get()).isZero();
  }

  @Test
  public void testProviderSelectedByReadOption() throws IOException {
    createTableWithDeletes();
    List<Row> expected = read(reader());

    List<Row> rows =
        read(
            reader()
                .option(
                    SparkReadOptions.PARTITION_READER_FACTORY_PROVIDER,
                    TestReaderFactoryProviders.Counting.NAME));

    assertThat(rows).containsExactlyElementsOf(expected);
    assertThat(TestReaderFactoryProviders.Counting.FACTORIES.get()).isPositive();
    assertThat(TestReaderFactoryProviders.Counting.COLUMNAR_READERS.get()).isPositive();
    assertThat(TestReaderFactoryProviders.Counting.ROW_READERS.get()).isZero();
    assertThat(TestReaderFactoryProviders.Counting.DELETE_FILES.get())
        .as("The partitions carry the tasks' delete files")
        .isPositive();
  }

  @Test
  public void testProviderSelectedBySessionForRowReads() throws IOException {
    createTableWithDeletes();
    List<Row> expected = read(reader().option(SparkReadOptions.VECTORIZATION_ENABLED, "false"));
    spark
        .conf()
        .set(
            SparkSQLProperties.PARTITION_READER_FACTORY_PROVIDER,
            TestReaderFactoryProviders.Counting.NAME);

    List<Row> rows = read(reader().option(SparkReadOptions.VECTORIZATION_ENABLED, "false"));

    assertThat(rows).containsExactlyElementsOf(expected);
    assertThat(TestReaderFactoryProviders.Counting.ROW_READERS.get()).isPositive();
    assertThat(TestReaderFactoryProviders.Counting.COLUMNAR_READERS.get()).isZero();
  }

  @Test
  public void testDecliningProvider() throws IOException {
    createTableWithDeletes();
    List<Row> expected = read(reader());

    List<Row> rows =
        read(
            reader()
                .option(
                    SparkReadOptions.PARTITION_READER_FACTORY_PROVIDER,
                    TestReaderFactoryProviders.Declining.NAME));

    assertThat(rows).containsExactlyElementsOf(expected);
  }

  @Test
  public void testUnknownProvider() throws IOException {
    createTableWithDeletes();

    assertThatThrownBy(
            () ->
                read(
                    reader().option(SparkReadOptions.PARTITION_READER_FACTORY_PROVIDER, "missing")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find partition reader factory provider missing")
        .hasMessageContaining(TestReaderFactoryProviders.Counting.NAME);
  }

  private DataFrameReader reader() {
    return spark.read().format("iceberg");
  }

  private List<Row> read(DataFrameReader reader) {
    return reader.load(tableLocation).sort("c1").collectAsList();
  }

  /** 400 rows in 4 files, and a position delete file removing 10 rows of the first. */
  private void createTableWithDeletes() throws IOException {
    Map<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, tableLocation);
    List<ThreeColumnRecord> records =
        IntStream.range(0, 400)
            .mapToObj(i -> new ThreeColumnRecord(i, "c2-" + (i % 7), "c3-" + i))
            .collect(Collectors.toList());
    spark
        .createDataFrame(records, ThreeColumnRecord.class)
        .repartition(4)
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
    table.refresh();

    DataFile first = Iterables.get(table.currentSnapshot().addedDataFiles(table.io()), 0);
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    for (long pos = 0; pos < 10; pos++) {
      deletes.add(Pair.of(first.location(), pos));
    }

    table
        .newRowDelta()
        .addDeletes(
            FileHelpers.writeDeleteFile(
                    table, Files.localOutput(new File(deleteDir, "deletes.parquet")), deletes)
                .first())
        .commit();
  }
}
