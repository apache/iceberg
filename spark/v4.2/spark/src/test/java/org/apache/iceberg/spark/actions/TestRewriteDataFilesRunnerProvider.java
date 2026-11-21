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

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRewriteDataFilesRunnerProvider extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  @TempDir private File tableDir;
  private String tableLocation;

  @BeforeEach
  public void before() {
    this.tableLocation = tableDir.toURI().toString();
    TestRewriteRunnerProviders.Recording.GROUPS.set(0);
    TestRewriteRunnerProviders.Declining.CALLS.set(0);
  }

  @AfterEach
  public void after() {
    spark.conf().unset(SparkSQLProperties.REWRITE_RUNNER_PROVIDER);
  }

  @Test
  public void testProviderNotUsedUnlessSelected() {
    Table table = createTable(4);
    List<Row> expected = currentData();

    RewriteDataFiles.Result result = rewrite(table).execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(4);
    assertThat(TestRewriteRunnerProviders.Recording.GROUPS.get()).isZero();
    assertThat(TestRewriteRunnerProviders.Declining.CALLS.get()).isZero();
    assertThat(currentData()).containsExactlyElementsOf(expected);
  }

  @Test
  public void testProviderSelectedByOption() {
    Table table = createTable(4);
    List<Row> expected = currentData();

    RewriteDataFiles.Result result =
        rewrite(table)
            .option(
                RewriteDataFilesSparkAction.REWRITE_RUNNER_PROVIDER,
                TestRewriteRunnerProviders.Recording.NAME)
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(4);
    assertThat(result.addedDataFilesCount()).isOne();
    assertThat(TestRewriteRunnerProviders.Recording.GROUPS.get()).isOne();
    assertThat(currentData()).containsExactlyElementsOf(expected);
  }

  @Test
  public void testProviderSelectedBySession() {
    Table table = createTable(4);
    List<Row> expected = currentData();
    spark
        .conf()
        .set(SparkSQLProperties.REWRITE_RUNNER_PROVIDER, TestRewriteRunnerProviders.Recording.NAME);

    rewrite(table).binPack().execute();

    assertThat(TestRewriteRunnerProviders.Recording.GROUPS.get()).isOne();
    assertThat(currentData()).containsExactlyElementsOf(expected);
  }

  @Test
  public void testDecliningProviderFallsBackToBinPack() {
    Table table = createTable(4);
    List<Row> expected = currentData();

    RewriteDataFiles.Result result =
        rewrite(table)
            .option(
                RewriteDataFilesSparkAction.REWRITE_RUNNER_PROVIDER,
                TestRewriteRunnerProviders.Declining.NAME)
            .execute();

    assertThat(TestRewriteRunnerProviders.Declining.CALLS.get()).isOne();
    assertThat(result.rewrittenDataFilesCount()).isEqualTo(4);
    assertThat(result.addedDataFilesCount()).isOne();
    assertThat(currentData()).containsExactlyElementsOf(expected);
  }

  @Test
  public void testSessionProviderIgnoredBySort() {
    Table table = createTable(4);
    List<Row> expected = currentData();
    spark
        .conf()
        .set(SparkSQLProperties.REWRITE_RUNNER_PROVIDER, TestRewriteRunnerProviders.Recording.NAME);

    rewrite(table)
        .sort(org.apache.iceberg.SortOrder.builderFor(SCHEMA).asc("c2").build())
        .execute();

    assertThat(TestRewriteRunnerProviders.Recording.GROUPS.get()).isZero();
    assertThat(currentData()).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void testProviderOptionRejectedForSort() {
    Table table = createTable(4);

    assertThatThrownBy(
            () ->
                rewrite(table)
                    .sort(org.apache.iceberg.SortOrder.builderFor(SCHEMA).asc("c2").build())
                    .option(
                        RewriteDataFilesSparkAction.REWRITE_RUNNER_PROVIDER,
                        TestRewriteRunnerProviders.Recording.NAME)
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("applies to bin-pack rewrites only");
  }

  @Test
  public void testUnknownProvider() {
    Table table = createTable(4);

    assertThatThrownBy(
            () ->
                rewrite(table)
                    .option(RewriteDataFilesSparkAction.REWRITE_RUNNER_PROVIDER, "missing")
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find rewrite runner provider missing")
        .hasMessageContaining(TestRewriteRunnerProviders.Recording.NAME);
  }

  private RewriteDataFilesSparkAction rewrite(Table table) {
    return SparkActions.get(spark)
        .rewriteDataFiles(table)
        .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1");
  }

  private Table createTable(int files) {
    Map<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, tableLocation);
    List<ThreeColumnRecord> records =
        IntStream.range(0, 400)
            .mapToObj(i -> new ThreeColumnRecord(i, "c2-" + (i % 7), "c3-" + i))
            .collect(Collectors.toList());
    spark
        .createDataFrame(records, ThreeColumnRecord.class)
        .repartition(files)
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
    table.refresh();
    assertThat(table.currentSnapshot().allManifests(table.io()).get(0).addedFilesCount())
        .isEqualTo(files);
    return table;
  }

  private List<Row> currentData() {
    Dataset<Row> rows = spark.read().format("iceberg").load(tableLocation);
    return rows.sort("c1", "c2", "c3").collectAsList();
  }
}
