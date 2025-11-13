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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.atIndex;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRegisterTableProcedure extends ExtensionsTestBase {

  private String targetTableName;

  @BeforeEach
  public void setTargetName() {
    targetTableName = tableName("register_table");
  }

  @AfterEach
  public void dropTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", targetTableName);
  }

  @TestTemplate
  public void testRegisterTable() throws NoSuchTableException, ParseException {
    long numRows = 1000;

    sql("CREATE TABLE %s (id int, data string) using ICEBERG", tableName);
    spark
        .range(0, numRows)
        .withColumn("data", functions.col("id").cast(DataTypes.StringType))
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    long originalFileCount = (long) scalarSql("SELECT COUNT(*) from %s.files", tableName);
    long currentSnapshotId = table.currentSnapshot().snapshotId();
    String metadataJson = TableUtil.metadataFileLocation(table);

    List<Object[]> result =
        sql(
            "CALL %s.system.register_table('%s', '%s')",
            catalogName, targetTableName, metadataJson);
    assertThat(result.get(0))
        .as("Current Snapshot is not correct")
        .contains(currentSnapshotId, atIndex(0));

    List<Object[]> original = sql("SELECT * FROM %s", tableName);
    List<Object[]> registered = sql("SELECT * FROM %s", targetTableName);
    assertEquals("Registered table rows should match original table rows", original, registered);
    assertThat(result.get(0))
        .as("Should have the right row count in the procedure result")
        .contains(numRows, atIndex(1))
        .as("Should have the right datafile count in the procedure result")
        .contains(originalFileCount, atIndex(2));
  }

  @TestTemplate
  public void testRegisterTableOverwrite() throws NoSuchTableException, ParseException {
    long numRows = 100;

    sql("CREATE TABLE %s (id int, data string) using ICEBERG", tableName);
    spark
        .range(0, numRows)
        .withColumn("data", functions.col("id").cast(DataTypes.StringType))
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    long originalFileCount = (long) scalarSql("SELECT COUNT(*) from %s.files", tableName);
    long currentSnapshotId = table.currentSnapshot().snapshotId();
    String metadataJson = TableUtil.metadataFileLocation(table);

    List<Object[]> result =
        sql(
            "CALL %s.system.register_table('%s', '%s', '%b')",
            catalogName, targetTableName, metadataJson, true);
    assertThat(result.get(0))
        .as("Current Snapshot is not correct")
        .contains(currentSnapshotId, atIndex(0));

    List<Object[]> original = sql("SELECT * FROM %s", tableName);
    List<Object[]> registered = sql("SELECT * FROM %s", targetTableName);
    assertEquals("Registered table rows should match original table rows", original, registered);
    assertThat(result.get(0))
        .as("Should have the right row count in the procedure result")
        .contains(numRows, atIndex(1))
        .as("Should have the right datafile count in the procedure result")
        .contains(originalFileCount, atIndex(2));
  }

  @TestTemplate
  public void testReRegisterTableOverwrite() throws NoSuchTableException, ParseException {
    testRegisterTable();

    long originalRowsCount = (long) scalarSql("SELECT COUNT(*) from %s", tableName);
    long additionalNumRows = 10;

    spark
        .range(originalRowsCount, originalRowsCount + additionalNumRows)
        .withColumn("data", functions.col("id").cast(DataTypes.StringType))
        .writeTo(tableName)
        .append();
    originalRowsCount = (long) scalarSql("SELECT COUNT(*) from %s", tableName);

    // Test few writes before re-register table
    spark
        .range(originalRowsCount, originalRowsCount + additionalNumRows)
        .withColumn("data", functions.col("id").cast(DataTypes.StringType))
        .writeTo(tableName)
        .append();
    originalRowsCount = (long) scalarSql("SELECT COUNT(*) from %s", tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    long originalFileCount = (long) scalarSql("SELECT COUNT(*) from %s.files", tableName);
    long currentSnapshotId = table.currentSnapshot().snapshotId();
    String metadataJson = TableUtil.metadataFileLocation(table);

    List<Object[]> result =
        sql(
            "CALL %s.system.register_table('%s', '%s', '%b')",
            catalogName, targetTableName, metadataJson, true);
    assertThat(result.get(0))
        .as("Current Snapshot is not correct")
        .contains(currentSnapshotId, atIndex(0));

    Set<Object[]> original = Set.copyOf(sql("SELECT * FROM %s", tableName));
    Set<Object[]> registered = Set.copyOf(sql("SELECT * FROM %s", targetTableName));

    assertThat(original)
        .usingElementComparator((arr1, arr2) -> Arrays.deepEquals(arr1, arr2) ? 0 : 1)
        .containsExactlyInAnyOrderElementsOf(registered);
    assertThat(result.get(0))
        .as("Should have the right row count in the procedure result")
        .contains(originalRowsCount, atIndex(1))
        .as("Should have the right datafile count in the procedure result")
        .contains(originalFileCount, atIndex(2));
  }

  @TestTemplate
  public void testReRegisterTableNotOverwrite() throws NoSuchTableException, ParseException {
    testRegisterTable();

    long numRows = 100;

    spark
        .range(0, numRows)
        .withColumn("data", functions.col("id").cast(DataTypes.StringType))
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    String metadataJson = TableUtil.metadataFileLocation(table);

    assertThatExceptionOfType(AlreadyExistsException.class)
        .isThrownBy(
            () ->
                sql(
                    "CALL %s.system.register_table('%s', '%s', '%b')",
                    catalogName, targetTableName, metadataJson, false))
        .withMessageContaining("Table already exists:");
  }
}
