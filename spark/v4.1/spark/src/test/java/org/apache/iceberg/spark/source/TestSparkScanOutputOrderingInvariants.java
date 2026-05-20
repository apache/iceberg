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

import java.util.Collections;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Unit-style tests for the invariants of {@code SparkPartitioningAwareScan.outputOrdering()}. Each
 * test creates the minimum table state needed to exercise exactly one invariant and asserts on the
 * returned {@link SortOrder}[] directly.
 *
 * <p>End-to-end physical plan validation lives in {@link TestSparkScan}.
 *
 * <p>Tracks https://github.com/apache/iceberg/issues/16430.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkScanOutputOrderingInvariants extends TestBaseWithCatalog {

  @Parameter(index = 3)
  private String format;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, format = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        "parquet"
      }
    };
  }

  @BeforeEach
  public void useCatalog() {
    sql("USE %s", catalogName);
  }

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void uniformSortOrderIdProducesNonEmptyOrdering() {
    sql(
        "CREATE TABLE %s (user_id BIGINT, event_time TIMESTAMP) USING iceberg"
            + " TBLPROPERTIES ('write.format.default'='%s')",
        tableName, format);
    Table table = validationCatalog.loadTable(tableIdent);
    table.replaceSortOrder().asc("event_time").commit();

    sql("INSERT INTO %s VALUES (1, TIMESTAMP '2024-01-01 00:00:00')", tableName);
    sql("INSERT INTO %s VALUES (2, TIMESTAMP '2024-01-02 00:00:00')", tableName);

    assertThat(outputOrderingOf(table)).as("uniform sort_order_id").hasSize(1);
  }

  @TestTemplate
  public void mixedSortOrderIdsProduceEmptyOrdering() {
    sql(
        "CREATE TABLE %s (user_id BIGINT, event_time TIMESTAMP) USING iceberg"
            + " TBLPROPERTIES ('write.format.default'='%s')",
        tableName, format);

    Table table = validationCatalog.loadTable(tableIdent);
    table.replaceSortOrder().asc("event_time").commit();
    sql("INSERT INTO %s VALUES (1, TIMESTAMP '2024-01-01 00:00:00')", tableName);

    table.refresh();
    table.replaceSortOrder().asc("user_id").commit();
    sql("INSERT INTO %s VALUES (2, TIMESTAMP '2024-01-02 00:00:00')", tableName);
    table.refresh();

    assertThat(outputOrderingOf(table)).as("mixed sort_order_id -> []").isEmpty();
  }

  @TestTemplate
  public void unsortedTableProducesEmptyOrdering() {
    sql(
        "CREATE TABLE %s (user_id BIGINT, event_time TIMESTAMP) USING iceberg"
            + " TBLPROPERTIES ('write.format.default'='%s')",
        tableName, format);
    sql("INSERT INTO %s VALUES (1, TIMESTAMP '2024-01-01 00:00:00')", tableName);

    assertThat(outputOrderingOf(validationCatalog.loadTable(tableIdent)))
        .as("sort_order_id = 0 -> []")
        .isEmpty();
  }

  @TestTemplate
  public void bucketTransformSortKeyProducesEmptyOrdering() {
    sql(
        "CREATE TABLE %s (user_id BIGINT, event_time TIMESTAMP) USING iceberg"
            + " TBLPROPERTIES ('write.format.default'='%s')",
        tableName, format);
    Table table = validationCatalog.loadTable(tableIdent);
    table
        .replaceSortOrder()
        .asc(org.apache.iceberg.expressions.Expressions.bucket("user_id", 8))
        .commit();
    sql("INSERT INTO %s VALUES (1, TIMESTAMP '2024-01-01 00:00:00')", tableName);

    assertThat(outputOrderingOf(table)).as("bucket transform -> []").isEmpty();
  }

  @TestTemplate
  public void multiFileTaskGroupProducesEmptyOrdering() {
    sql(
        "CREATE TABLE %s (user_id BIGINT, event_time TIMESTAMP) USING iceberg"
            + " TBLPROPERTIES ('write.format.default'='%s')",
        tableName, format);
    Table table = validationCatalog.loadTable(tableIdent);
    table.replaceSortOrder().asc("event_time").commit();

    sql("INSERT INTO %s VALUES (1, TIMESTAMP '2024-01-01 00:00:00')", tableName);
    sql("INSERT INTO %s VALUES (2, TIMESTAMP '2024-01-02 00:00:00')", tableName);

    // Default split size = 128 MB packs tiny test files into one task group
    SortOrder[] ordering =
        ((org.apache.spark.sql.connector.read.SupportsReportOrdering)
                new SparkScanBuilder(spark, table, CaseInsensitiveStringMap.empty()).build())
            .outputOrdering();
    assertThat(ordering).as("multi-file task group -> []").isEmpty();
  }

  @TestTemplate
  public void compositeIdentitySortKeyProducesAllFields() {
    sql(
        "CREATE TABLE %s (region STRING, user_id BIGINT, event_time TIMESTAMP) USING iceberg"
            + " TBLPROPERTIES ('write.format.default'='%s')",
        tableName, format);
    Table table = validationCatalog.loadTable(tableIdent);
    table.replaceSortOrder().asc("region").asc("user_id").commit();

    sql(
        "INSERT INTO %s VALUES ('us', 1, TIMESTAMP '2024-01-01 00:00:00'),"
            + " ('us', 2, TIMESTAMP '2024-01-02 00:00:00')",
        tableName);

    assertThat(outputOrderingOf(table)).as("composite identity sort").hasSize(2);
  }

  /**
   * Builds a scan with split size = 1 byte so each file becomes its own task group, then returns
   * the scan's reported output ordering.
   */
  private SortOrder[] outputOrderingOf(Table table) {
    CaseInsensitiveStringMap opts =
        new CaseInsensitiveStringMap(Collections.singletonMap(SparkReadOptions.SPLIT_SIZE, "1"));
    org.apache.spark.sql.connector.read.Scan scan =
        new SparkScanBuilder(spark, table, opts).build();
    return ((org.apache.spark.sql.connector.read.SupportsReportOrdering) scan).outputOrdering();
  }
}
