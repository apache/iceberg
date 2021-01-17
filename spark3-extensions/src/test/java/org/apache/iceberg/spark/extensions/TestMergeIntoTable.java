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

package org.apache.iceberg.spark.extensions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.PARQUET_VECTORIZATION_ENABLED;

public class TestMergeIntoTable extends SparkRowLevelOperationsTestBase {
  private final String sourceName;
  private final String targetName;

  public TestMergeIntoTable(String catalogName, String implementation, Map<String, String> config,
                            String fileFormat, Boolean vectorized) {
    super(catalogName, implementation, config, fileFormat, vectorized);
    this.sourceName = tableName("source");
    this.targetName = tableName("target");
  }

  @BeforeClass
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(TableProperties.DELETE_MODE, "copy-on-write");
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", targetName);
    sql("DROP TABLE IF EXISTS %s", sourceName);
  }

  @Test
  public void testEmptyTargetInsertAllNonMatchingRows() throws NoSuchTableException {
    createAndInitUnPartitionedTargetTable(targetName);
    createAndInitSourceTable(sourceName);
    append(sourceName, new Employee(1, "emp-id-1"), new Employee(2, "emp-id-2"), new Employee(3, "emp-id-3"));
    String sqlText = "MERGE INTO " + targetName + " AS target " +
                     "USING " + sourceName + " AS source " +
                     "ON target.id = source.id " +
                     "WHEN NOT MATCHED THEN INSERT * ";

    sql(sqlText);
    sql("SELECT * FROM %s ORDER BY id, dep", targetName);
    assertEquals("Should have expected rows",
            ImmutableList.of(row(1, "emp-id-1"), row(2, "emp-id-2"), row(3, "emp-id-3")),
            sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", targetName));
  }

  @Test
  public void testEmptyTargetInsertOnlyMatchingRows() throws NoSuchTableException {
    createAndInitUnPartitionedTargetTable(targetName);
    createAndInitSourceTable(sourceName);
    append(sourceName, new Employee(1, "emp-id-1"), new Employee(2, "emp-id-2"), new Employee(3, "emp-id-3"));
    String sqlText = "MERGE INTO " + targetName + " AS target " +
                     "USING " + sourceName + " AS source " +
                     "ON target.id = source.id " +
                     "WHEN NOT MATCHED AND (source.id >= 2) THEN INSERT * ";

    sql(sqlText);
    List<Object[]> res = sql("SELECT * FROM %s ORDER BY id, dep", targetName);
    assertEquals("Should have expected rows",
            ImmutableList.of(row(2, "emp-id-2"), row(3, "emp-id-3")),
            sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", targetName));
  }

  @Test
  public void testOnlyUpdate() throws NoSuchTableException {
    createAndInitUnPartitionedTargetTable(targetName);
    createAndInitSourceTable(sourceName);
    append(targetName, new Employee(1, "emp-id-one"), new Employee(6, "emp-id-6"));
    append(sourceName, new Employee(2, "emp-id-2"), new Employee(1, "emp-id-1"), new Employee(6, "emp-id-6"));
    String sqlText = "MERGE INTO " + targetName + " AS target " +
            "USING " + sourceName + " AS source " +
            "ON target.id = source.id " +
            "WHEN MATCHED AND target.id = 1 THEN UPDATE SET * ";

    sql(sqlText);
    List<Object[]> res = sql("SELECT * FROM %s ORDER BY id, dep", targetName);
    assertEquals("Should have expected rows",
            ImmutableList.of(row(1, "emp-id-1"), row(6, "emp-id-6")),
            sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", targetName));
  }

  @Test
  public void testOnlyDelete() throws NoSuchTableException {
    createAndInitUnPartitionedTargetTable(targetName);
    createAndInitSourceTable(sourceName);
    append(targetName, new Employee(1, "emp-id-one"), new Employee(6, "emp-id-6"));
    append(sourceName, new Employee(2, "emp-id-2"), new Employee(1, "emp-id-1"), new Employee(6, "emp-id-6"));
    String sqlText = "MERGE INTO " + targetName + " AS target " +
            "USING " + sourceName + " AS source " +
            "ON target.id = source.id " +
            "WHEN MATCHED AND target.id = 6 THEN DELETE";

    sql(sqlText);
    List<Object[]> res = sql("SELECT * FROM %s ORDER BY id, dep", targetName);
    assertEquals("Should have expected rows",
            ImmutableList.of(row(1, "emp-id-one")),
            sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", targetName));
  }

  @Test
  public void testAllCauses() throws NoSuchTableException {
    createAndInitUnPartitionedTargetTable(targetName);
    createAndInitSourceTable(sourceName);
    append(targetName, new Employee(1, "emp-id-one"), new Employee(6, "emp-id-6"));
    append(sourceName, new Employee(2, "emp-id-2"), new Employee(1, "emp-id-1"), new Employee(6, "emp-id-6"));
    String sqlText = "MERGE INTO " + targetName + " AS target " +
                     "USING " + sourceName + " AS source " +
                     "ON target.id = source.id " +
                     "WHEN MATCHED AND target.id = 1 THEN UPDATE SET * " +
                     "WHEN MATCHED AND target.id = 6 THEN DELETE " +
                     "WHEN NOT MATCHED AND source.id = 2 THEN INSERT * ";

    sql(sqlText);
    sql("SELECT * FROM %s ORDER BY id, dep", targetName);
    assertEquals("Should have expected rows",
            ImmutableList.of(row(1, "emp-id-1"), row(2, "emp-id-2")),
            sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", targetName));
  }

  @Test
  public void testAllCausesWithExplicitColumnSpecification() throws NoSuchTableException {
    createAndInitUnPartitionedTargetTable(targetName);
    createAndInitSourceTable(sourceName);
    append(targetName, new Employee(1, "emp-id-one"), new Employee(6, "emp-id-6"));
    append(sourceName, new Employee(2, "emp-id-2"), new Employee(1, "emp-id-1"), new Employee(6, "emp-id-6"));
    String sqlText = "MERGE INTO " + targetName + " AS target " +
            "USING " + sourceName + " AS source " +
            "ON target.id = source.id " +
            "WHEN MATCHED AND target.id = 1 THEN UPDATE SET target.id = source.id, target.dep = source.dep " +
            "WHEN MATCHED AND target.id = 6 THEN DELETE " +
            "WHEN NOT MATCHED AND source.id = 2 THEN INSERT (target.id, target.dep) VALUES (source.id, source.dep) ";

    sql(sqlText);
    sql("SELECT * FROM %s ORDER BY id, dep", targetName);
    assertEquals("Should have expected rows",
            ImmutableList.of(row(1, "emp-id-1"), row(2, "emp-id-2")),
            sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", targetName));
  }

  @Test
  public void testSourceCTE() throws NoSuchTableException {
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhive"));

    createAndInitUnPartitionedTargetTable(targetName);
    createAndInitSourceTable(sourceName);
    append(targetName, new Employee(2, "emp-id-two"), new Employee(6, "emp-id-6"));
    append(sourceName, new Employee(2, "emp-id-3"), new Employee(1, "emp-id-2"), new Employee(5, "emp-id-6"));
    String sourceCTE = "WITH cte1 AS (SELECT id + 1 AS id, dep FROM source)";
    String sqlText = sourceCTE + " " + "MERGE INTO " + targetName + " AS target " +
            "USING cte1"  + " AS source " +
            "ON target.id = source.id " +
            "WHEN MATCHED AND target.id = 2 THEN UPDATE SET * " +
            "WHEN MATCHED AND target.id = 6 THEN DELETE " +
            "WHEN NOT MATCHED AND source.id = 3 THEN INSERT * ";

    sql(sqlText);
    sql("SELECT * FROM %s ORDER BY id, dep", targetName);
    assertEquals("Should have expected rows",
            ImmutableList.of(row(2, "emp-id-2"), row(3, "emp-id-3")),
            sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", targetName));
  }

  @Test
  public void testSourceFromSetOps() throws NoSuchTableException {
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhive"));

    createAndInitUnPartitionedTargetTable(targetName);
    createAndInitSourceTable(sourceName);
    append(targetName, new Employee(1, "emp-id-one"), new Employee(6, "emp-id-6"));
    append(sourceName, new Employee(2, "emp-id-2"), new Employee(1, "emp-id-1"), new Employee(6, "emp-id-6"));
    String derivedSource = " ( SELECT * FROM source WHERE id = 2 " +
                           "   UNION ALL " +
                           "   SELECT * FROM source WHERE id = 1 OR id = 6)";
    String sqlText = "MERGE INTO " + targetName + " AS target " +
            "USING " + derivedSource + " AS source " +
            "ON target.id = source.id " +
            "WHEN MATCHED AND target.id = 1 THEN UPDATE SET * " +
            "WHEN MATCHED AND target.id = 6 THEN DELETE " +
            "WHEN NOT MATCHED AND source.id = 2 THEN INSERT * ";

    sql(sqlText);
    sql("SELECT * FROM %s ORDER BY id, dep", targetName);
    assertEquals("Should have expected rows",
            ImmutableList.of(row(1, "emp-id-1"), row(2, "emp-id-2")),
            sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", targetName));
  }

  protected void createAndInitPartitionedTargetTable(String tabName) {
    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg PARTITIONED BY (dep)", tabName);
    initTable(tabName);
  }

  protected void createAndInitUnPartitionedTargetTable(String tabName) {
    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg", tabName);
    initTable(tabName);
  }

  protected void createAndInitSourceTable(String tabName) {
    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg PARTITIONED BY (dep)", tabName);
    initTable(tabName);
  }

  private void initTable(String tabName) {
    sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')", tabName, DEFAULT_FILE_FORMAT, fileFormat);

    switch (fileFormat) {
      case "parquet":
        sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%b')", tabName, PARQUET_VECTORIZATION_ENABLED, vectorized);
        break;
      case "orc":
        Assert.assertTrue(vectorized);
        break;
      case "avro":
        Assert.assertFalse(vectorized);
        break;
    }

    Map<String, String> props = extraTableProperties();
    props.forEach((prop, value) -> {
      sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')", tabName, prop, value);
    });
  }

  protected void append(String tabName, Employee... employees) throws NoSuchTableException {
    List<Employee> input = Arrays.asList(employees);
    Dataset<Row> inputDF = spark.createDataFrame(input, Employee.class);
    inputDF.coalesce(1).writeTo(tabName).append();
  }
}
