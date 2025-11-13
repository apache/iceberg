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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPartitionFieldOrdering extends TestBaseWithCatalog {

  private static final java.util.concurrent.ConcurrentHashMap<String, List<Object>> EXECUTION_LOG =
      new java.util.concurrent.ConcurrentHashMap<>();

  @BeforeEach
  public void setupTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithStringField() throws Exception {
    sql(
        "CREATE TABLE %s (category STRING, region STRING) "
            + "USING iceberg PARTITIONED BY (category, region)",
        tableName);

    sql(
        "INSERT INTO %s VALUES "
            + "('Z', 'West'), "
            + "('A', 'East'), "
            + "('M', 'North'), "
            + "('B', 'South'), "
            + "('A', 'West'), "
            + "('Z', 'East')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    verifyPartitionOrderingPerExecutor(
        "category", (Function<Row, String> & Serializable) row -> row.getAs("category"), 6);

    verifyPartitionOrderingPerExecutor(
        "region", (Function<Row, String> & Serializable) row -> row.getAs("region"), 6);
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithMixedTypes() throws Exception {
    sql(
        "CREATE TABLE %s (id INT, category STRING) "
            + "USING iceberg PARTITIONED BY (id, category)",
        tableName);

    sql(
        "INSERT INTO %s VALUES "
            + "(30, 'Z'), "
            + "(10, 'B'), "
            + "(50, 'A'), "
            + "(20, 'M'), "
            + "(10, 'Z'), "
            + "(30, 'A')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    verifyPartitionOrderingPerExecutor(
        "id", (Function<Row, Integer> & Serializable) row -> row.getAs("id"), 6);

    verifyPartitionOrderingPerExecutor(
        "category", (Function<Row, String> & Serializable) row -> row.getAs("category"), 6);
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithIntegerField() throws Exception {
    sql(
        "CREATE TABLE %s (id INT, priority INT) " + "USING iceberg PARTITIONED BY (id, priority)",
        tableName);

    sql(
        "INSERT INTO %s VALUES "
            + "(30, 1), "
            + "(10, 3), "
            + "(50, 2), "
            + "(20, 5), "
            + "(10, 1), "
            + "(30, 4)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    verifyPartitionOrderingPerExecutor(
        "id", (Function<Row, Integer> & Serializable) row -> row.getAs("id"), 6);

    verifyPartitionOrderingPerExecutor(
        "priority", (Function<Row, Integer> & Serializable) row -> row.getAs("priority"), 6);
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithLongField() throws Exception {
    sql(
        "CREATE TABLE %s (ts BIGINT, version BIGINT) "
            + "USING iceberg PARTITIONED BY (ts, version)",
        tableName);

    sql(
        "INSERT INTO %s VALUES "
            + "(3000L, 2L), "
            + "(1000L, 1L), "
            + "(5000L, 4L), "
            + "(2000L, 3L), "
            + "(1000L, 5L), "
            + "(3000L, 1L)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    verifyPartitionOrderingPerExecutor(
        "ts", (Function<Row, Long> & Serializable) row -> row.getAs("ts"), 6);

    verifyPartitionOrderingPerExecutor(
        "version", (Function<Row, Long> & Serializable) row -> row.getAs("version"), 6);
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithDoubleField() throws Exception {
    sql(
        "CREATE TABLE %s (score DOUBLE, rating DOUBLE) "
            + "USING iceberg PARTITIONED BY (score, rating)",
        tableName);

    sql(
        "INSERT INTO %s VALUES "
            + "(95.5, 4.2), "
            + "(87.2, 3.8), "
            + "(92.1, 4.5), "
            + "(99.9, 3.9), "
            + "(87.2, 4.1), "
            + "(95.5, 3.7)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    verifyPartitionOrderingPerExecutor(
        "score", (Function<Row, Double> & Serializable) row -> row.getAs("score"), 6);

    verifyPartitionOrderingPerExecutor(
        "rating", (Function<Row, Double> & Serializable) row -> row.getAs("rating"), 6);
  }

  @TestTemplate
  public void testPartitionFieldOrderingOnSecondaryField() throws Exception {
    sql(
        "CREATE TABLE %s (category STRING, priority INT) "
            + "USING iceberg PARTITIONED BY (category, priority)",
        tableName);

    sql(
        "INSERT INTO %s VALUES "
            + "('A', 5), "
            + "('A', 1), "
            + "('A', 3), "
            + "('B', 4), "
            + "('B', 2), "
            + "('C', 6)",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    verifyPartitionOrderingPerExecutor(
        "priority", (Function<Row, Integer> & Serializable) row -> row.getAs("priority"), 6);

    verifyPartitionOrderingPerExecutor(
        "category", (Function<Row, String> & Serializable) row -> row.getAs("category"), 6);
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithSessionConfig() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (category STRING) " + "USING iceberg PARTITIONED BY (category)",
        tableName);

    sql("INSERT INTO %s VALUES ('Z'), ('A'), ('M')", tableName);

    spark.conf().set(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD, "category");

    try {
      Dataset<Row> df = spark.read().table(tableName);
      List<Row> rows = df.collectAsList();
      assertThat(rows).hasSize(3);

    } finally {
      spark.conf().unset(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD);
    }
  }

  @TestTemplate
  public void testPartitionFieldOrderingMutuallyExclusiveWithSPJ() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (category STRING) " + "USING iceberg PARTITIONED BY (category)",
        tableName);

    sql("INSERT INTO %s VALUES ('A'), ('B')", tableName);

    spark.conf().set(SparkSQLProperties.PRESERVE_DATA_GROUPING, "true");
    spark.conf().set(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD, "category");

    try {
      Dataset<Row> df = spark.read().table(tableName);
      List<Row> rows = df.collectAsList();
      assertThat(rows).hasSize(2);

    } finally {
      spark.conf().unset(SparkSQLProperties.PRESERVE_DATA_GROUPING);
      spark.conf().unset(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD);
    }
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithNonExistentField() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (category STRING) " + "USING iceberg PARTITIONED BY (category)",
        tableName);

    sql("INSERT INTO %s VALUES ('A'), ('B')", tableName);

    Dataset<Row> df =
        spark
            .read()
            .option(SparkReadOptions.SPLIT_ORDERING_BY_PARTITIONED_FIELD, "nonexistent")
            .table(tableName);

    List<Row> rows = df.collectAsList();
    assertThat(rows).hasSize(2);
  }

  @TestTemplate
  public void testPartitionFieldOrderingReadOptionOverridesSessionConfig()
      throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (category STRING, id INT) "
            + "USING iceberg PARTITIONED BY (category, id)",
        tableName);

    sql("INSERT INTO %s VALUES ('Z', 30), ('A', 10), ('M', 20)", tableName);

    spark.conf().set(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD, "category");

    try {
      Dataset<Row> df =
          spark
              .read()
              .option(SparkReadOptions.SPLIT_ORDERING_BY_PARTITIONED_FIELD, "id")
              .table(tableName);

      List<Row> rows = df.collectAsList();
      assertThat(rows).hasSize(3);
    } finally {
      spark.conf().unset(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD);
    }
  }

  @TestTemplate
  public void testOrderingIsDisabledByDefaultWithTransforms() throws Exception {
    sql(
        "CREATE TABLE %s (id INT, category STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (id, truncate(category, 1))",
        tableName);

    sql(
        "INSERT INTO %s VALUES "
            + "(3, 'Whale'), "
            + "(1, 'Zebra'), "
            + "(2, 'Ape'), "
            + "(3, 'Shark'), "
            + "(1, 'Yak'), "
            + "(2, 'Bear'), "
            + "(1, 'Cat')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    String partitionFieldName = table.spec().fields().get(1).name();
    assertThat(partitionFieldName).isEqualTo("category_trunc");

    verifyPartitionOrderingPerExecutor(
        partitionFieldName,
        (Function<Row, String> & Serializable) row -> row.getString(1).substring(0, 1),
        7);

    verifyPartitionOrderingIsNotApplied(
        (Function<Row, String> & Serializable) row -> row.getString(1).substring(0, 1), 7);
  }

  /** Verifies tasks are processed in partition order within each executor. */
  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> void verifyPartitionOrderingPerExecutor(
      String partitionFieldName, Function<Row, T> partitionValueExtractor, int expectedRowCount) {

    // Read with partition field ordering enabled
    Dataset<Row> df =
        spark
            .read()
            .option(SparkReadOptions.SPLIT_ORDERING_BY_PARTITIONED_FIELD, partitionFieldName)
            .table(tableName);

    Map<String, List<T>> executionLog =
        captureExecutorPartitionValues(df, partitionValueExtractor, expectedRowCount);

    // Verify ALL executors have sorted values
    for (Map.Entry<String, List<T>> entry : executionLog.entrySet()) {
      String executorId = entry.getKey();
      List<T> executorPartitionValues = entry.getValue();

      assertThat(executorPartitionValues).isNotEmpty();

      // Verify this executor's tasks are in sorted partition order
      // Use nullsFirst comparator to handle null partition values (from partition evolution)
      List<T> sortedExecutorValues = new ArrayList<>(executorPartitionValues);
      sortedExecutorValues.sort(
          java.util.Comparator.nullsFirst(java.util.Comparator.naturalOrder()));

      assertThat(executorPartitionValues)
          .as(
              "Executor '%s' should process %d tasks in partition order",
              executorId, executorPartitionValues.size())
          .isEqualTo(sortedExecutorValues);
    }
  }

  /** Verifies tasks are NOT processed in partition order when ordering option is disabled. */
  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> void verifyPartitionOrderingIsNotApplied(
      Function<Row, T> partitionValueExtractor, int expectedRowCount) {

    // Read WITHOUT partition field ordering option
    Dataset<Row> df = spark.read().table(tableName);

    Map<String, List<T>> executionLog =
        captureExecutorPartitionValues(df, partitionValueExtractor, expectedRowCount);

    // At least ONE executor should have tasks in NON-sorted order (proves ordering is disabled)
    boolean foundUnsortedExecutor = false;
    for (Map.Entry<String, List<T>> entry : executionLog.entrySet()) {
      List<T> executorPartitionValues = entry.getValue();

      // Only check executors with multiple tasks
      if (executorPartitionValues.size() > 1) {
        List<T> sortedExecutorValues = new ArrayList<>(executorPartitionValues);
        sortedExecutorValues.sort(
            java.util.Comparator.nullsFirst(java.util.Comparator.naturalOrder()));

        if (!executorPartitionValues.equals(sortedExecutorValues)) {
          foundUnsortedExecutor = true;
          break;
        }
      }
    }

    assertThat(foundUnsortedExecutor).isTrue();
  }

  /** Captures partition values for each row processed by executors. */
  @SuppressWarnings("unchecked")
  private <T extends Comparable<T>> Map<String, List<T>> captureExecutorPartitionValues(
      Dataset<Row> df, Function<Row, T> partitionValueExtractor, int expectedRowCount) {

    // Clear previous execution data
    EXECUTION_LOG.clear();

    // Capture execution metadata while passing through the data unchanged
    Dataset<Row> processedDf =
        df.mapPartitions(
            (MapPartitionsFunction<Row, Row>)
                partition -> {
                  // Get executor ID from SparkEnv
                  String executorId =
                      org.apache.spark.SparkEnv.get().executorId()
                          + "-"
                          + Thread.currentThread().getId();

                  List<Row> rows = new ArrayList<>();

                  while (partition.hasNext()) {
                    Row row = partition.next();

                    // Record partition value for each row
                    T partitionValue = partitionValueExtractor.apply(row);
                    EXECUTION_LOG
                        .computeIfAbsent(executorId, k -> new ArrayList<>())
                        .add(partitionValue);

                    rows.add(row);
                  }

                  return rows.iterator();
                },
            org.apache.spark.sql.Encoders.row(df.schema()));

    // Trigger execution by collecting the data
    List<Row> allRows = processedDf.collectAsList();

    assertThat(allRows).hasSize(expectedRowCount);

    // Cast static map to typed version for verification
    Map<String, List<T>> executionLog = (Map<String, List<T>>) (Object) EXECUTION_LOG;

    assertThat(executionLog).isNotEmpty();

    return executionLog;
  }

  @TestTemplate
  public void testPartitionEvolutionWithSameFieldName() throws Exception {
    sql(
        "CREATE TABLE %s (id INT, event_time TIMESTAMP, data STRING) " + "USING iceberg",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateSpec()
        .addField("time_partition", org.apache.iceberg.expressions.Expressions.day("event_time"))
        .commit();
    table.refresh();

    sql(
        "INSERT INTO %s VALUES "
            + "(1, TIMESTAMP '2025-12-16 10:30:00', 'data1'), "
            + "(2, TIMESTAMP '2025-01-15 14:20:00', 'data2'), "
            + "(3, TIMESTAMP '2025-09-16 09:45:00', 'data3')",
        tableName);

    table.refresh();

    table
        .updateSpec()
        .removeField("time_partition")
        .addField("time_partition", org.apache.iceberg.expressions.Expressions.hour("event_time"))
        .commit();
    table.refresh();

    sql(
        "INSERT INTO %s VALUES "
            + "(4, TIMESTAMP '2025-01-14 08:00:00', 'data4'), "
            + "(5, TIMESTAMP '2025-01-17 11:00:00', 'data5'), "
            + "(6, TIMESTAMP '2025-01-14 16:00:00', 'data6')",
        tableName);

    Dataset<Row> df =
        spark
            .read()
            .option(SparkReadOptions.SPLIT_ORDERING_BY_PARTITIONED_FIELD, "time_partition")
            .table(tableName);

    List<Row> rows = df.collectAsList();
    assertThat(rows).hasSize(6);

    List<Integer> ids = rows.stream().map(row -> row.getInt(0)).collect(Collectors.toList());
    assertThat(ids).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
  }

  @TestTemplate
  public void testPartitionEvolutionWithDifferentFieldNames() throws Exception {
    sql(
        "CREATE TABLE %s (id INT, event_time TIMESTAMP, data STRING) " + "USING iceberg",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    table
        .updateSpec()
        .addField("day_partition", org.apache.iceberg.expressions.Expressions.day("event_time"))
        .commit();
    table.refresh();

    sql(
        "INSERT INTO %s VALUES "
            + "(1, TIMESTAMP '2025-12-16 10:30:00', 'data1'), "
            + "(2, TIMESTAMP '2025-01-15 14:20:00', 'data2'), "
            + "(3, TIMESTAMP '2025-09-16 09:45:00', 'data3')",
        tableName);

    table.refresh();

    table
        .updateSpec()
        .removeField("day_partition")
        .addField("hour_partition", org.apache.iceberg.expressions.Expressions.hour("event_time"))
        .commit();
    table.refresh();

    sql(
        "INSERT INTO %s VALUES "
            + "(4, TIMESTAMP '2025-01-14 08:00:00', 'data4'), "
            + "(5, TIMESTAMP '2025-01-17 11:00:00', 'data5'), "
            + "(6, TIMESTAMP '2025-01-14 16:00:00', 'data6')",
        tableName);

    verifyPartitionOrderingPerExecutor(
        "day_partition",
        (Function<Row, Integer> & Serializable)
            row -> {
              int id = row.getInt(0);
              if (id <= 3) {
                if (id == 1) return 20708;
                if (id == 2) return 20099;
                return 20346;
              } else {
                return null;
              }
            },
        6);
  }
}
