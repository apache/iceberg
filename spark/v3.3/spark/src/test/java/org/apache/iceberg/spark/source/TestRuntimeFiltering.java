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

import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestRuntimeFiltering extends SparkTestBaseWithCatalog {

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS dim");
  }

  @Test
  public void testIdentityPartitionedTable() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (date)",
        tableName);

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

  @Test
  public void testBucketedTable() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(8, id))",
        tableName);

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

  @Test
  public void testRenamedSourceColumnTable() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(8, id))",
        tableName);

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

  @Test
  public void testMultipleRuntimeFilters() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (data, bucket(8, id))",
        tableName);

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

  @Test
  public void testCaseSensitivityOfRuntimeFilters() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (data, bucket(8, id))",
        tableName);

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

  @Test
  public void testBucketedTableWithMultipleSpecs() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) USING iceberg",
        tableName);

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

  @Test
  public void testSourceColumnWithDots() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (`i.d` BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(8, `i.d`))",
        tableName);

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

  @Test
  public void testSourceColumnWithBackticks() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (`i``d` BIGINT, data STRING, date DATE, ts TIMESTAMP) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(8, `i``d`))",
        tableName);

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

  @Test
  public void testUnpartitionedTable() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING, date DATE, ts TIMESTAMP) USING iceberg",
        tableName);

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
    Assert.assertEquals(errorMessage, expectedFilterCount, actualFilterCount);
  }

  // delete files that don't match the filter to ensure dynamic filtering works and only required
  // files are read
  private void deleteNotMatchingFiles(Expression filter, int expectedDeletedFileCount) {
    Table table = validationCatalog.loadTable(tableIdent);
    FileIO io = table.io();

    Set<String> matchingFileLocations = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> files = table.newScan().filter(filter).planFiles()) {
      for (FileScanTask file : files) {
        String path = file.file().path().toString();
        matchingFileLocations.add(path);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    Set<String> deletedFileLocations = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> files = table.newScan().planFiles()) {
      for (FileScanTask file : files) {
        String path = file.file().path().toString();
        if (!matchingFileLocations.contains(path)) {
          io.deleteFile(path);
          deletedFileLocations.add(path);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    Assert.assertEquals(
        "Deleted unexpected number of files",
        expectedDeletedFileCount,
        deletedFileLocations.size());
  }
}
