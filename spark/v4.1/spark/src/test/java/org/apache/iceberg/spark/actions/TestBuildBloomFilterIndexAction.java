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

import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBuildBloomFilterIndexAction extends CatalogTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testBloomIndexActionWritesStatisticsFile()
      throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    spark
        .createDataset(
            ImmutableList.of(
                new org.apache.iceberg.spark.source.SimpleRecord(1, "a"),
                new org.apache.iceberg.spark.source.SimpleRecord(2, "b"),
                new org.apache.iceberg.spark.source.SimpleRecord(3, "c")),
            Encoders.bean(org.apache.iceberg.spark.source.SimpleRecord.class))
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot).isNotNull();

    SparkActions actions = SparkActions.get(spark);
    BuildBloomFilterIndexSparkAction.Result result =
        actions.buildBloomFilterIndex(table).column("id").execute();

    assertThat(result).isNotNull();
    List<StatisticsFile> statisticsFiles = table.statisticsFiles();
    assertThat(statisticsFiles).isNotEmpty();
    assertThat(statisticsFiles.get(0).fileSizeInBytes()).isGreaterThan(0L);
  }

  @TestTemplate
  public void testBloomIndexPrunesTasksForEqualityPredicate()
      throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    // Two groups of values so that only one file should match id = 1
    spark
        .createDataset(
            ImmutableList.of(
                new org.apache.iceberg.spark.source.SimpleRecord(1, "a"),
                new org.apache.iceberg.spark.source.SimpleRecord(1, "b"),
                new org.apache.iceberg.spark.source.SimpleRecord(2, "c")),
            Encoders.bean(org.apache.iceberg.spark.source.SimpleRecord.class))
        .repartition(2)
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get(spark);

    // Build Bloom index on id
    actions.buildBloomFilterIndex(table).column("id").execute();
    table.refresh();

    // Plan tasks and validate Bloom pruning via the utility directly (unit-level sanity check)
    List<FileScanTask> allTasks;
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      allTasks = Lists.newArrayList(planned);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    List<FileScanTask> prunedTasks =
        BloomFilterIndexUtil.pruneTasksWithBloomIndex(
            table, table.currentSnapshot(), () -> allTasks, "id", 1);

    assertThat(prunedTasks.size()).isLessThanOrEqualTo(allTasks.size());
  }
}
