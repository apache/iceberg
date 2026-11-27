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

import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;
import static org.assertj.core.api.Assertions.assertThat;
import static scala.collection.JavaConverters.seqAsJavaListConverter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.optimizer.NestedDynamicPruning;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRuntimeFiltering extends TestBaseWithCatalog {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, planningMode = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        LOCAL
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        DISTRIBUTED
      }
    };
  }

  @Parameter(index = 3)
  private PlanningMode planningMode;

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS dim");
  }

  @TestTemplate
  public void testIdentityPartitionedTable() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);
    configurePlanningMode(planningMode);

    Dataset<Row> df =
        spark
            .range(1, 100)
            .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 4 AS INT)")))
            .withColumn("ts", expr("TO_TIMESTAMP(date)"))
            .withColumn("data", expr("CAST(date AS STRING)"))
            .select("id", "data", "date", "ts");

    df.coalesce(1).writeTo(tableName).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    Dataset<Row> dimDF =
        spark.range(1, 10).withColumn("date", expr("DATE '1970-01-02'")).select("id", "date");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    String query =
        String.format(
            "SELECT f.* FROM %s f JOIN dim d ON f.date = d.date AND d.id = 1 ORDER BY id",
            tableName);

    assertQueryContainsRuntimeFilter(query);

    deleteNotMatchingFiles(Expressions.equal("date", 1), 3);

    assertEquals(
        "Should have expected rows",
        sql("SELECT * FROM %s WHERE date = DATE '1970-01-02' ORDER BY id", tableName),
        sql(query));
  }

  @TestTemplate
  public void testBucketedTable() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(8, id))",
        tableName);
    configurePlanningMode(planningMode);

    Dataset<Row> df =
        spark
            .range(1, 100)
            .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 4 AS INT)")))
            .withColumn("ts", expr("TO_TIMESTAMP(date)"))
            .withColumn("data", expr("CAST(date AS STRING)"))
            .select("id", "data", "date", "ts");

    df.coalesce(1).writeTo(tableName).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    Dataset<Row> dimDF =
        spark.range(1, 2).withColumn("date", expr("DATE '1970-01-02'")).select("id", "date");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    String query =
        String.format(
            "SELECT f.* FROM %s f JOIN dim d ON f.id = d.id AND d.date = DATE '1970-01-02' ORDER BY date",
            tableName);

    assertQueryContainsRuntimeFilter(query);

    deleteNotMatchingFiles(Expressions.equal("id", 1), 7);

    assertEquals(
        "Should have expected rows",
        sql("SELECT * FROM %s WHERE id = 1 ORDER BY date", tableName),
        sql(query));
  }

  @TestTemplate
  public void testRenamedSourceColumnTable() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(8, id))",
        tableName);
    configurePlanningMode(planningMode);

    Dataset<Row> df =
        spark
            .range(1, 100)
            .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 4 AS INT)")))
            .withColumn("ts", expr("TO_TIMESTAMP(date)"))
            .withColumn("data", expr("CAST(date AS STRING)"))
            .select("id", "data", "date", "ts");

    df.coalesce(1).writeTo(tableName).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    Dataset<Row> dimDF =
        spark.range(1, 2).withColumn("date", expr("DATE '1970-01-02'")).select("id", "date");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    sql("ALTER TABLE %s RENAME COLUMN id TO row_id", tableName);

    String query =
        String.format(
            "SELECT f.* FROM %s f JOIN dim d ON f.row_id = d.id AND d.date = DATE '1970-01-02' ORDER BY date",
            tableName);

    assertQueryContainsRuntimeFilter(query);

    deleteNotMatchingFiles(Expressions.equal("row_id", 1), 7);

    assertEquals(
        "Should have expected rows",
        sql("SELECT * FROM %s WHERE row_id = 1 ORDER BY date", tableName),
        sql(query));
  }

  @TestTemplate
  public void testMultipleRuntimeFilters() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (data, bucket(8, id))",
        tableName);
    configurePlanningMode(planningMode);

    Dataset<Row> df =
        spark
            .range(1, 100)
            .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 4 AS INT)")))
            .withColumn("ts", expr("TO_TIMESTAMP(date)"))
            .withColumn("data", expr("CAST(date AS STRING)"))
            .select("id", "data", "date", "ts");

    df.coalesce(1).writeTo(tableName).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();

    sql("CREATE TABLE dim (id BIGINT, date DATE, data STRING) USING parquet");
    Dataset<Row> dimDF =
        spark
            .range(1, 2)
            .withColumn("date", expr("DATE '1970-01-02'"))
            .withColumn("data", expr("'1970-01-02'"))
            .select("id", "date", "data");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    String query =
        String.format(
            "SELECT f.* FROM %s f JOIN dim d ON f.id = d.id AND f.data = d.data AND d.date = DATE '1970-01-02'",
            tableName);

    assertQueryContainsRuntimeFilters(query, 2, "Query should have 2 runtime filters");

    deleteNotMatchingFiles(Expressions.equal("id", 1), 31);

    assertEquals(
        "Should have expected rows",
        sql("SELECT * FROM %s WHERE id = 1 AND data = '1970-01-02'", tableName),
        sql(query));
  }

  @TestTemplate
  public void testCaseSensitivityOfRuntimeFilters() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (data, bucket(8, id))",
        tableName);
    configurePlanningMode(planningMode);

    Dataset<Row> df =
        spark
            .range(1, 100)
            .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 4 AS INT)")))
            .withColumn("ts", expr("TO_TIMESTAMP(date)"))
            .withColumn("data", expr("CAST(date AS STRING)"))
            .select("id", "data", "date", "ts");

    df.coalesce(1).writeTo(tableName).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();

    sql("CREATE TABLE dim (id BIGINT, date DATE, data STRING) USING parquet");
    Dataset<Row> dimDF =
        spark
            .range(1, 2)
            .withColumn("date", expr("DATE '1970-01-02'"))
            .withColumn("data", expr("'1970-01-02'"))
            .select("id", "date", "data");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    String caseInsensitiveQuery =
        String.format(
            "select f.* from %s F join dim d ON f.Id = d.iD and f.DaTa = d.dAtA and d.dAtE = date '1970-01-02'",
            tableName);

    assertQueryContainsRuntimeFilters(
        caseInsensitiveQuery, 2, "Query should have 2 runtime filters");

    deleteNotMatchingFiles(Expressions.equal("id", 1), 31);

    assertEquals(
        "Should have expected rows",
        sql("SELECT * FROM %s WHERE id = 1 AND data = '1970-01-02'", tableName),
        sql(caseInsensitiveQuery));
  }

  @TestTemplate
  public void testBucketedTableWithMultipleSpecs() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) USING iceberg",
        tableName);
    configurePlanningMode(planningMode);

    Dataset<Row> df1 =
        spark
            .range(1, 100)
            .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 2 AS INT)")))
            .withColumn("ts", expr("TO_TIMESTAMP(date)"))
            .withColumn("data", expr("CAST(date AS STRING)"))
            .select("id", "data", "date", "ts");

    df1.coalesce(1).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSpec().addField(Expressions.bucket("id", 8)).commit();

    sql("REFRESH TABLE %s", tableName);

    Dataset<Row> df2 =
        spark
            .range(1, 100)
            .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 4 AS INT)")))
            .withColumn("ts", expr("TO_TIMESTAMP(date)"))
            .withColumn("data", expr("CAST(date AS STRING)"))
            .select("id", "data", "date", "ts");

    df2.coalesce(1).writeTo(tableName).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    Dataset<Row> dimDF =
        spark.range(1, 2).withColumn("date", expr("DATE '1970-01-02'")).select("id", "date");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    String query =
        String.format(
            "SELECT f.* FROM %s f JOIN dim d ON f.id = d.id AND d.date = DATE '1970-01-02' ORDER BY date",
            tableName);

    assertQueryContainsRuntimeFilter(query);

    deleteNotMatchingFiles(Expressions.equal("id", 1), 7);

    assertEquals(
        "Should have expected rows",
        sql("SELECT * FROM %s WHERE id = 1 ORDER BY date", tableName),
        sql(query));
  }

  @TestTemplate
  public void testSourceColumnWithDots() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (`i.d` BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(8, `i.d`))",
        tableName);
    configurePlanningMode(planningMode);

    Dataset<Row> df =
        spark
            .range(1, 100)
            .withColumnRenamed("id", "i.d")
            .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(`i.d` % 4 AS INT)")))
            .withColumn("ts", expr("TO_TIMESTAMP(date)"))
            .withColumn("data", expr("CAST(date AS STRING)"))
            .select("`i.d`", "data", "date", "ts");

    df.coalesce(1).writeTo(tableName).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();

    sql("SELECT * FROM %s WHERE `i.d` = 1", tableName);

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    Dataset<Row> dimDF =
        spark.range(1, 2).withColumn("date", expr("DATE '1970-01-02'")).select("id", "date");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    String query =
        String.format(
            "SELECT f.* FROM %s f JOIN dim d ON f.`i.d` = d.id AND d.date = DATE '1970-01-02' ORDER BY date",
            tableName);

    assertQueryContainsRuntimeFilter(query);

    deleteNotMatchingFiles(Expressions.equal("i.d", 1), 7);

    sql(query);

    assertEquals(
        "Should have expected rows",
        sql("SELECT * FROM %s WHERE `i.d` = 1 ORDER BY date", tableName),
        sql(query));
  }

  @TestTemplate
  public void testSourceColumnWithBackticks() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (`i``d` BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(8, `i``d`))",
        tableName);
    configurePlanningMode(planningMode);

    Dataset<Row> df =
        spark
            .range(1, 100)
            .withColumnRenamed("id", "i`d")
            .withColumn(
                "date", date_add(expr("DATE '1970-01-01'"), expr("CAST(`i``d` % 4 AS INT)")))
            .withColumn("ts", expr("TO_TIMESTAMP(date)"))
            .withColumn("data", expr("CAST(date AS STRING)"))
            .select("`i``d`", "data", "date", "ts");

    df.coalesce(1).writeTo(tableName).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    Dataset<Row> dimDF =
        spark.range(1, 2).withColumn("date", expr("DATE '1970-01-02'")).select("id", "date");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    String query =
        String.format(
            "SELECT f.* FROM %s f JOIN dim d ON f.`i``d` = d.id AND d.date = DATE '1970-01-02' ORDER BY date",
            tableName);

    assertQueryContainsRuntimeFilter(query);

    deleteNotMatchingFiles(Expressions.equal("i`d", 1), 7);

    assertEquals(
        "Should have expected rows",
        sql("SELECT * FROM %s WHERE `i``d` = 1 ORDER BY date", tableName),
        sql(query));
  }

  @TestTemplate
  public void testUnpartitionedTable() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) USING iceberg",
        tableName);
    configurePlanningMode(planningMode);

    Dataset<Row> df =
        spark
            .range(1, 100)
            .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 4 AS INT)")))
            .withColumn("ts", expr("TO_TIMESTAMP(date)"))
            .withColumn("data", expr("CAST(date AS STRING)"))
            .select("id", "data", "date", "ts");

    df.coalesce(1).writeTo(tableName).append();

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    Dataset<Row> dimDF =
        spark.range(1, 2).withColumn("date", expr("DATE '1970-01-02'")).select("id", "date");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    String query =
        String.format(
            "SELECT f.* FROM %s f JOIN dim d ON f.id = d.id AND d.date = DATE '1970-01-02' ORDER BY date",
            tableName);

    assertQueryContainsNoRuntimeFilter(query);

    assertEquals(
        "Should have expected rows",
        sql("SELECT * FROM %s WHERE id = 1 ORDER BY date", tableName),
        sql(query));
  }

  @TestTemplate
  public void testNoDataColumnOfUnpartitionedParquetTable() throws NoSuchTableException {
    // the Parquet reader keeps the rows that a data column filter rules out
    assertDataColumnRuntimeFilter("parquet", false);
  }

  @TestTemplate
  public void testDataColumnOfUnpartitionedVortexTable() throws NoSuchTableException {
    assertDataColumnRuntimeFilter("vortex", true);
  }

  @TestTemplate
  public void testDataColumnOfListedParquetFormat() throws NoSuchTableException {
    String formats = SparkSQLProperties.DATA_COLUMN_RUNTIME_FILTERING_FORMATS;
    spark.conf().set(formats, "vortex, parquet");
    try {
      assertDataColumnRuntimeFilter("parquet", true);
    } finally {
      spark.conf().unset(formats);
    }
  }

  private void assertDataColumnRuntimeFilter(String format, boolean offered)
      throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('write.format.default' = '%s')",
        tableName, format);
    configurePlanningMode(planningMode);

    // four files, each with a separate range of ids
    spark
        .range(1, 100)
        .withColumn("data", expr("CAST(id AS STRING)"))
        .repartitionByRange(4, col("id"))
        .sortWithinPartitions("id")
        .writeTo(tableName)
        .append();

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    Dataset<Row> dimDF =
        spark.range(1, 2).withColumn("date", expr("DATE '1970-01-02'")).select("id", "date");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    String query =
        String.format(
            "SELECT f.* FROM %s f JOIN dim d ON f.id = d.id AND d.date = DATE '1970-01-02'",
            tableName);

    var optimizations = spark.experimental().extraOptimizations();
    NestedDynamicPruning.install(spark);
    try {
      if (!offered) {
        assertQueryContainsNoRuntimeFilter(query);
        assertEquals(
            "Should have expected rows",
            sql("SELECT * FROM %s WHERE id = 1", tableName),
            sql(query));
        return;
      }

      assertQueryContainsRuntimeFilter(query);

      withSQLConf(
          ImmutableMap.of(SparkSQLProperties.DATA_COLUMN_RUNTIME_FILTERING_ENABLED, "false"),
          () -> assertQueryContainsNoRuntimeFilter(query));

      // the metrics of the other files rule out the join key, so the scan must not read them
      deleteNotMatchingFiles(Expressions.equal("id", 1), 3);

      assertEquals(
          "Should have expected rows", sql("SELECT * FROM %s WHERE id = 1", tableName), sql(query));

      // a set larger than the limit reaches the readers as its range
      withSQLConf(
          ImmutableMap.of(SparkSQLProperties.RUNTIME_FILTER_MAX_VALUES, "0"),
          () ->
              assertEquals(
                  "Should have expected rows",
                  sql("SELECT * FROM %s WHERE id = 1", tableName),
                  sql(query)));
    } finally {
      spark.experimental().extraOptimizations_$eq(optimizations);
    }
  }

  @TestTemplate
  public void testScanShortensLargeRuntimeInFilter() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('write.format.default' = 'vortex')",
        tableName);
    configurePlanningMode(planningMode);

    spark
        .range(1, 100)
        .withColumn("data", expr("CAST(id AS STRING)"))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    Dataset<Row> dimDF =
        spark.range(1, 21).withColumn("date", expr("DATE '1970-01-02'")).select("id", "date");
    dimDF.coalesce(1).write().mode("append").insertInto("dim");

    String query =
        String.format(
            "SELECT f.* FROM %s f JOIN dim d ON f.id = d.id AND d.date = DATE '1970-01-02' "
                + "ORDER BY id",
            tableName);

    var optimizations = spark.experimental().extraOptimizations();
    NestedDynamicPruning.install(spark);
    try {
      // without adaptive execution, the leaves of the executed plan are the scans
      withSQLConf(
          ImmutableMap.of(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false"),
          () -> {
            Dataset<Row> df = spark.sql(query);

            assertEquals(
                "Should have expected rows",
                sql("SELECT * FROM %s WHERE id <= 20 ORDER BY id", tableName),
                rowsToJava(df.collectAsList()));

            // the scan lists 10 of the 20 join keys
            List<SparkPlan> leaves =
                seqAsJavaListConverter(df.queryExecution().executedPlan().collectLeaves()).asJava();
            assertThat(leaves)
                .filteredOn(BatchScanExec.class::isInstance)
                .map(leaf -> ((BatchScanExec) leaf).scan().toString())
                .anySatisfy(
                    scan ->
                        assertThat(scan)
                            .contains("runtimeFilters=id IN (")
                            .contains(", ... 10 more)"));
          });
    } finally {
      spark.experimental().extraOptimizations_$eq(optimizations);
    }
  }

  private void assertQueryContainsRuntimeFilter(String query) {
    assertQueryContainsRuntimeFilters(query, 1, "Query should have 1 runtime filter");
  }

  private void assertQueryContainsNoRuntimeFilter(String query) {
    assertQueryContainsRuntimeFilters(query, 0, "Query should have no runtime filters");
  }

  private void assertQueryContainsRuntimeFilters(
      String query, int expectedFilterCount, String errorMessage) {
    List<Row> output = spark.sql("EXPLAIN EXTENDED " + query).collectAsList();
    String plan = output.get(0).getString(0);
    int actualFilterCount = StringUtils.countMatches(plan, "dynamicpruningexpression");
    assertThat(actualFilterCount).as(errorMessage).isEqualTo(expectedFilterCount);
  }

  // delete files that don't match the filter to ensure dynamic filtering works and only required
  // files are read
  private void deleteNotMatchingFiles(Expression filter, int expectedDeletedFileCount) {
    Table table = validationCatalog.loadTable(tableIdent);
    FileIO io = table.io();

    Set<String> matchingFileLocations = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> files = table.newScan().filter(filter).planFiles()) {
      for (FileScanTask file : files) {
        String path = file.file().location();
        matchingFileLocations.add(path);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    Set<String> deletedFileLocations = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> files = table.newScan().planFiles()) {
      for (FileScanTask file : files) {
        String path = file.file().location();
        if (!matchingFileLocations.contains(path)) {
          io.deleteFile(path);
          deletedFileLocations.add(path);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    assertThat(deletedFileLocations)
        .as("Deleted unexpected number of files")
        .hasSize(expectedDeletedFileCount);
  }
}
