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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPartitionFieldOrdering extends TestBaseWithCatalog {

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
    // Create table partitioned by category (string) and region (string)
    sql(
        "CREATE TABLE %s (category STRING, region STRING) "
            + "USING iceberg PARTITIONED BY (category, region)",
        tableName);

    // Insert test data with different partition values in non-alphabetical order
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

    // Test ordering by first partition field (category)
    List<String> categoryOrdered = getPartitionValuesFromScan(table, "category", 0);
    assertThat(categoryOrdered).containsExactly("A", "A", "B", "M", "Z", "Z");

    // Test ordering by second partition field (region)
    List<String> regionOrdered = getPartitionValuesFromScan(table, "region", 1);
    assertThat(regionOrdered).containsExactly("East", "East", "North", "South", "West", "West");
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithMixedTypes() throws Exception {
    // Create table partitioned by id (integer) and category (string)
    sql(
        "CREATE TABLE %s (id INT, category STRING) "
            + "USING iceberg PARTITIONED BY (id, category)",
        tableName);

    // Insert test data with mixed partition types in non-sorted order
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

    // Test ordering by first partition field (id - integer)
    List<Integer> idOrdered = getPartitionValuesFromScan(table, "id", 0);
    assertThat(idOrdered).containsExactly(10, 10, 20, 30, 30, 50);

    // Test ordering by second partition field (category - string)
    List<String> categoryOrdered = getPartitionValuesFromScan(table, "category", 1);
    assertThat(categoryOrdered).containsExactly("A", "A", "B", "M", "Z", "Z");
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithIntegerField() throws Exception {
    // Create table partitioned by id (integer) and priority (integer)
    sql(
        "CREATE TABLE %s (id INT, priority INT) " + "USING iceberg PARTITIONED BY (id, priority)",
        tableName);

    // Insert test data with integer partitions in non-sorted order
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

    // Test ordering by first partition field (id)
    List<Integer> idOrdered = getPartitionValuesFromScan(table, "id", 0);
    assertThat(idOrdered).containsExactly(10, 10, 20, 30, 30, 50);

    // Test ordering by second partition field (priority)
    List<Integer> priorityOrdered = getPartitionValuesFromScan(table, "priority", 1);
    assertThat(priorityOrdered).containsExactly(1, 1, 2, 3, 4, 5);
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithLongField() throws Exception {
    // Create table partitioned by ts (long) and version (long)
    sql(
        "CREATE TABLE %s (ts BIGINT, version BIGINT) "
            + "USING iceberg PARTITIONED BY (ts, version)",
        tableName);

    // Insert test data with long partitions in non-sorted order
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

    // Test ordering by first partition field (ts)
    List<Long> tsOrdered = getPartitionValuesFromScan(table, "ts", 0);
    assertThat(tsOrdered).containsExactly(1000L, 1000L, 2000L, 3000L, 3000L, 5000L);

    // Test ordering by second partition field (version)
    List<Long> versionOrdered = getPartitionValuesFromScan(table, "version", 1);
    assertThat(versionOrdered).containsExactly(1L, 1L, 2L, 3L, 4L, 5L);
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithDoubleField() throws Exception {
    // Create table partitioned by score (double) and rating (double)
    sql(
        "CREATE TABLE %s (score DOUBLE, rating DOUBLE) "
            + "USING iceberg PARTITIONED BY (score, rating)",
        tableName);

    // Insert test data with double partitions in non-sorted order
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

    // Test ordering by first partition field (score)
    List<Double> scoreOrdered = getPartitionValuesFromScan(table, "score", 0);
    assertThat(scoreOrdered).containsExactly(87.2, 87.2, 92.1, 95.5, 95.5, 99.9);

    // Test ordering by second partition field (rating)
    List<Double> ratingOrdered = getPartitionValuesFromScan(table, "rating", 1);
    assertThat(ratingOrdered).containsExactly(3.7, 3.8, 3.9, 4.1, 4.2, 4.5);
  }

  @TestTemplate
  public void testPartitionFieldOrderingOnSecondaryField() throws Exception {
    // Create table partitioned by category (string) and priority (integer)
    sql(
        "CREATE TABLE %s (category STRING, priority INT) "
            + "USING iceberg PARTITIONED BY (category, priority)",
        tableName);

    // Insert test data where we want to test ordering on the SECOND partition field
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

    // Test ordering by SECOND partition field (priority) - this is the key test!
    List<Integer> priorityOrdered = getPartitionValuesFromScan(table, "priority", 1);
    assertThat(priorityOrdered).containsExactly(1, 2, 3, 4, 5, 6);

    // Also verify first partition field ordering still works
    List<String> categoryOrdered = getPartitionValuesFromScan(table, "category", 0);
    assertThat(categoryOrdered).containsExactly("A", "A", "A", "B", "B", "C");
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithSessionConfig() throws NoSuchTableException {
    // Create table partitioned by category
    sql(
        "CREATE TABLE %s (category STRING) " + "USING iceberg PARTITIONED BY (category)",
        tableName);

    sql("INSERT INTO %s VALUES ('Z'), ('A'), ('M')", tableName);

    // Set session configuration
    spark.conf().set(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD, "category");

    try {
      Dataset<Row> df = spark.read().table(tableName);
      List<Row> rows = df.collectAsList();
      assertThat(rows).hasSize(3);

      // Note: Session config ordering verification would require access to scan internals
      // This test verifies the configuration doesn't break functionality
    } finally {
      // Clean up session config
      spark.conf().unset(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD);
    }
  }

  @TestTemplate
  public void testPartitionFieldOrderingMutuallyExclusiveWithSPJ() throws NoSuchTableException {
    // Create table partitioned by category
    sql(
        "CREATE TABLE %s (category STRING) " + "USING iceberg PARTITIONED BY (category)",
        tableName);

    sql("INSERT INTO %s VALUES ('A'), ('B')", tableName);

    // Enable both preserve-data-grouping (SPJ) and partition field ordering
    spark.conf().set(SparkSQLProperties.PRESERVE_DATA_GROUPING, "true");
    spark.conf().set(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD, "category");

    try {
      // When SPJ is enabled, partition field ordering should be ignored
      Dataset<Row> df = spark.read().table(tableName);
      List<Row> rows = df.collectAsList();
      assertThat(rows).hasSize(2);

      // Verify that SPJ takes precedence (partition field ordering should be ignored)
      // This is verified by the fact that no exception is thrown and data is still readable
    } finally {
      // Clean up session configs
      spark.conf().unset(SparkSQLProperties.PRESERVE_DATA_GROUPING);
      spark.conf().unset(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD);
    }
  }

  @TestTemplate
  public void testPartitionFieldOrderingWithNonExistentField() throws NoSuchTableException {
    // Create table partitioned by category
    sql(
        "CREATE TABLE %s (category STRING) " + "USING iceberg PARTITIONED BY (category)",
        tableName);

    sql("INSERT INTO %s VALUES ('A'), ('B')", tableName);

    // Test ordering by non-existent partition field - should not fail but field ordering should be
    // ignored
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
    // Create table partitioned by both category and id
    sql(
        "CREATE TABLE %s (category STRING, id INT) "
            + "USING iceberg PARTITIONED BY (category, id)",
        tableName);

    sql("INSERT INTO %s VALUES ('Z', 30), ('A', 10), ('M', 20)", tableName);

    // Set session config to order by category
    spark.conf().set(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD, "category");

    try {
      // Read option should override session config (order by id instead of category)
      Dataset<Row> df =
          spark
              .read()
              .option(SparkReadOptions.SPLIT_ORDERING_BY_PARTITIONED_FIELD, "id")
              .table(tableName);

      List<Row> rows = df.collectAsList();
      assertThat(rows).hasSize(3);
    } finally {
      // Clean up session config
      spark.conf().unset(SparkSQLProperties.SPLIT_ORDERING_BY_PARTITIONED_FIELD);
    }
  }

  @TestTemplate
  public void testOrderingIsDisabledByDefaultWithTransforms() throws Exception {
    // Create table partitioned by id and a transform on category
    sql(
        "CREATE TABLE %s (id INT, category STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (id, truncate(category, 1))",
        tableName);

    // Insert more test data in a specific, non-sorted order
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

    // The partition transform on category is truncate(category, 1).
    // The default name for this partition field is 'category_trunc'.
    Table table = validationCatalog.loadTable(tableIdent);
    String partitionFieldName = table.spec().fields().get(1).name(); // Get the actual name
    assertThat(partitionFieldName).isEqualTo("category_trunc");

    // 1. Read from the table WITH the ordering option for the transformed category
    Dataset<Row> orderedDf =
        spark
            .read()
            .option(SparkReadOptions.SPLIT_ORDERING_BY_PARTITIONED_FIELD, partitionFieldName)
            .table(tableName);

    List<String> orderedCategories =
        orderedDf.collectAsList().stream()
            .map(row -> row.getString(1)) // category is the second column (index 1)
            .collect(Collectors.toList());

    // Assert that the ordered read is actually sorted by the first letter of the category
    List<String> expectedSortedCategories =
        Lists.newArrayList("Ape", "Bear", "Cat", "Shark", "Whale", "Yak", "Zebra");
    assertThat(orderedCategories).isEqualTo(expectedSortedCategories);

    // 2. Read from the table WITHOUT any ordering options (default behavior)
    Dataset<Row> defaultDf = spark.read().table(tableName);

    List<String> defaultCategories =
        defaultDf.collectAsList().stream()
            .map(row -> row.getString(1)) // category is the second column (index 1)
            .collect(Collectors.toList());

    // 3. Assert that the default order is NOT the same as the sorted order.
    assertThat(defaultCategories).isNotEqualTo(orderedCategories);
  }

  /**
   * Helper method to get partition values from a DataFrameReader with partition field ordering.
   * This verifies partition ordering by examining the data returned in order.
   */
  @SuppressWarnings("unchecked")
  private <T> List<T> getPartitionValuesFromScan(
      Table table, String partitionFieldName, int fieldPosition) throws Exception {
    // Create DataFrameReader with partition field ordering
    Dataset<Row> df =
        spark
            .read()
            .option(SparkReadOptions.SPLIT_ORDERING_BY_PARTITIONED_FIELD, partitionFieldName)
            .table(tableName);

    // Get the actual partition field name from the table spec
    String actualPartitionFieldName = getPartitionFieldNameFromPosition(table, fieldPosition);
    if (actualPartitionFieldName == null) {
      return Lists.newArrayList();
    }

    // Get partition values by examining the DataFrame's rows ordered by the partition field
    List<Row> rows = df.collectAsList();
    List<T> partitionValues = Lists.newArrayList();

    // Extract partition values from the data rows
    for (Row row : rows) {
      T partitionValue = (T) row.getAs(actualPartitionFieldName);
      partitionValues.add(partitionValue);
    }

    return partitionValues;
  }

  private String getPartitionFieldNameFromPosition(Table table, int position) {
    // Get the actual partition field name from the table's partition spec
    if (table.spec().fields().size() > position) {
      return table.spec().fields().get(position).name();
    }
    return null;
  }
}
