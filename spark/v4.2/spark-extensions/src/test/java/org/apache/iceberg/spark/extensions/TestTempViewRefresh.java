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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTempViewRefresh extends ExtensionsTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        ImmutableMap.<String, String>builder()
            .putAll(SparkCatalogConfig.HIVE.properties())
            .put("cache-enabled", "false")
            .build()
      },
      {
        SparkCatalogConfig.SPARK_SESSION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION.implementation(),
        ImmutableMap.<String, String>builder()
            .putAll(SparkCatalogConfig.SPARK_SESSION.properties())
            .put("cache-enabled", "true")
            .put("cache.expiration-interval-ms", "-1") // indefinite cache
            .buildKeepingLast()
      }
    };
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP VIEW IF EXISTS tmp");
  }

  @TestTemplate
  public void testSessionWrite() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100), (10, 1000)", tableName);

    // create temp view from Dataset with filter
    spark.table(tableName).filter("salary < 999").createOrReplaceTempView("tmp");

    // query view
    List<Object[]> result = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result).hasSize(1).containsExactly(row(1, 100));

    // add more data via session write
    sql("INSERT INTO %s VALUES (2, 200)", tableName);

    // query view again
    List<Object[]> result2 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result2)
        .as("Temp view should reflect session writes")
        .hasSize(2)
        .containsExactly(row(1, 100), row(2, 200));
  }

  @TestTemplate
  public void testExternalWrite() throws IOException {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100), (10, 1000)", tableName);

    // create temp view from Dataset with filter
    spark.table(tableName).filter("salary < 999").createOrReplaceTempView("tmp");

    List<Object[]> result1 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer adds data
    Table table = validationCatalog.loadTable(tableIdent);

    GenericRecord record = GenericRecord.create(table.schema());
    record.set(0, 2); // id
    record.set(1, 200); // salary

    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newFastAppend().appendFile(dataFile).commit();

    // query view again
    List<Object[]> result2 = sql("SELECT * FROM tmp ORDER BY id");
    if (cachingCatalogEnabled()) {
      assertThat(result2)
          .as("Temp view should not reflect external writes when cache is enabled")
          .hasSize(1)
          .containsExactly(row(1, 100));
    } else {
      assertThat(result2)
          .as("Temp view should reflect external writes when cache is disabled")
          .hasSize(2)
          .containsExactly(row(1, 100), row(2, 200));
    }
  }

  @TestTemplate
  public void testSessionSchemaEvolutionAddColumn() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100), (10, 1000)", tableName);

    // create temp view from Dataset with filter
    spark.table(tableName).filter("salary < 999").createOrReplaceTempView("tmp");

    List<Object[]> result = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result).hasSize(1).containsExactly(row(1, 100));

    // add new column via session DDL
    sql("ALTER TABLE %s ADD COLUMN new_column INT", tableName);

    // add data with new column
    sql("INSERT INTO %s VALUES (2, 200, -1)", tableName);

    // query view again - should preserve original schema but pick up new data
    List<Object[]> result2 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result2)
        .as("Temp view should pick up new data but preserve original schema")
        .hasSize(2)
        .containsExactly(row(1, 100), row(2, 200));
  }

  @TestTemplate
  public void testExternalSchemaEvolutionAddColumn() throws IOException {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100), (10, 1000)", tableName);

    // create temp view from Dataset
    spark.table(tableName).filter("salary < 999").createOrReplaceTempView("tmp");

    List<Object[]> result1 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer adds column and data
    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSchema().addColumn("new_column", Types.IntegerType.get()).commit();

    GenericRecord record = GenericRecord.create(table.schema());
    record.set(0, 2); // id
    record.set(1, 200); // salary
    record.set(2, -1); // new_column

    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newFastAppend().appendFile(dataFile).commit();

    // query view again
    List<Object[]> result2 = sql("SELECT * FROM tmp ORDER BY id");
    if (cachingCatalogEnabled()) {
      assertThat(result2)
          .as("Temp view should not reflect external changes when cache is enabled")
          .hasSize(1)
          .containsExactly(row(1, 100));
    } else {
      assertThat(result2)
          .as("Temp view should preserve original schema but pick up new snapshot")
          .hasSize(2)
          .containsExactly(row(1, 100), row(2, 200));
    }
  }

  @TestTemplate
  public void testSessionSchemaEvolutionDropColumn() {
    // create table with extra column
    sql("CREATE TABLE %s (id INT, salary INT, new_column INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100, 10)", tableName);

    // create temp view from Dataset
    spark.table(tableName).createOrReplaceTempView("tmp");

    List<Object[]> result = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result).hasSize(1).containsExactly(row(1, 100, 10));

    // drop column that view references
    sql("ALTER TABLE %s DROP COLUMN new_column", tableName);

    assertThatThrownBy(() -> sql("SELECT * FROM tmp"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("new_column");
  }

  @TestTemplate
  public void testExternalSchemaEvolutionDropColumn() {
    // create table with extra column
    sql("CREATE TABLE %s (id INT, salary INT, new_column INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100, 10)", tableName);

    // create temp view from Dataset
    spark.table(tableName).createOrReplaceTempView("tmp");

    List<Object[]> result1 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result1).hasSize(1).containsExactly(row(1, 100, 10));

    // external writer drops column
    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSchema().deleteColumn("new_column").commit();

    if (cachingCatalogEnabled()) {
      List<Object[]> result2 = sql("SELECT * FROM tmp ORDER BY id");
      assertThat(result2)
          .as("Temp view should return cached data when cache is enabled")
          .hasSize(1)
          .containsExactly(row(1, 100, 10));
    } else {
      assertThatThrownBy(() -> sql("SELECT * FROM tmp"))
          .isInstanceOf(AnalysisException.class)
          .hasMessageContaining("new_column");
    }
  }

  @TestTemplate
  public void testSessionDropAndRecreateTable() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // create temp view from Dataset
    spark.table(tableName).createOrReplaceTempView("tmp");

    List<Object[]> result = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result).hasSize(1).containsExactly(row(1, 100));

    // drop and recreate table with same schema
    sql("DROP TABLE %s", tableName);
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (2, 200)", tableName);

    // query view again - should resolve to new table
    List<Object[]> result2 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result2)
        .as("Temp view should resolve to newly created table")
        .hasSize(1)
        .containsExactly(row(2, 200));
  }

  @TestTemplate
  public void testExternalDropAndRecreateTable() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // create temp view from Dataset
    spark.table(tableName).createOrReplaceTempView("tmp");

    List<Object[]> result1 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer drops and recreates table
    validationCatalog.dropTable(tableIdent, false /* keep files */);
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "salary", Types.IntegerType.get()));
    validationCatalog.createTable(tableIdent, schema);

    // query view again
    List<Object[]> result2 = sql("SELECT * FROM tmp ORDER BY id");
    if (cachingCatalogEnabled()) {
      assertThat(result2)
          .as("Temp view should return stale data from old table when cache is enabled")
          .hasSize(1)
          .containsExactly(row(1, 100));
    } else {
      assertThat(result2).as("Temp view should resolve to newly created table").isEmpty();
    }
  }

  @TestTemplate
  public void testSessionDropAndAddColumnSameNameSameType() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // create temp view from Dataset
    spark.table(tableName).createOrReplaceTempView("tmp");

    List<Object[]> result = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result).hasSize(1).containsExactly(row(1, 100));

    // drop and add column with same name and type
    sql("ALTER TABLE %s DROP COLUMN salary", tableName);
    sql("ALTER TABLE %s ADD COLUMN salary INT", tableName);
    sql("INSERT INTO %s VALUES (2, 200)", tableName);

    // query view again - should resolve to new column
    List<Object[]> result2 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result2)
        .as("Temp view should resolve columns by name")
        .hasSize(2)
        .containsExactly(row(1, null), row(2, 200));
  }

  @TestTemplate
  public void testExternalDropAndAddColumnSameNameSameType() throws IOException {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // create temp view from Dataset
    spark.table(tableName).createOrReplaceTempView("tmp");

    List<Object[]> result1 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer drops and adds column with same name and type
    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSchema().deleteColumn("salary").commit();
    table.updateSchema().addColumn("salary", Types.IntegerType.get()).commit();

    GenericRecord record = GenericRecord.create(table.schema());
    record.set(0, 2); // id
    record.setField("salary", 200); // new salary column

    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newFastAppend().appendFile(dataFile).commit();

    // query view again
    List<Object[]> result2 = sql("SELECT * FROM tmp ORDER BY id");
    if (cachingCatalogEnabled()) {
      assertThat(result2)
          .as("Temp view should return stale data when cache is enabled")
          .hasSize(1)
          .containsExactly(row(1, 100));
    } else {
      assertThat(result2)
          .as("Temp view should resolve columns by name")
          .hasSize(2)
          .containsExactly(row(1, null), row(2, 200));
    }
  }

  @TestTemplate
  public void testSessionDropAndAddColumnSameNameDifferentType() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // create temp view from Dataset
    spark.table(tableName).createOrReplaceTempView("tmp");

    List<Object[]> result = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result).hasSize(1).containsExactly(row(1, 100));

    // drop and add column with same name but different type
    sql("ALTER TABLE %s DROP COLUMN salary", tableName);
    sql("ALTER TABLE %s ADD COLUMN salary STRING", tableName);

    assertThatThrownBy(() -> sql("SELECT * FROM tmp"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("salary");
  }

  @TestTemplate
  public void testExternalDropAndAddColumnSameNameDifferentType() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // create temp view from Dataset
    spark.table(tableName).createOrReplaceTempView("tmp");

    List<Object[]> result1 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer drops and recreates table with different column type
    validationCatalog.dropTable(tableIdent, false /* keep files */);
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(3, "salary", Types.StringType.get()));
    validationCatalog.createTable(tableIdent, schema);

    if (cachingCatalogEnabled()) {
      List<Object[]> result2 = sql("SELECT * FROM tmp ORDER BY id");
      assertThat(result2)
          .as("Temp view should return cached data when cache is enabled")
          .hasSize(1)
          .containsExactly(row(1, 100));
    } else {
      assertThatThrownBy(() -> sql("SELECT * FROM tmp"))
          .isInstanceOf(AnalysisException.class)
          .hasMessageContaining("salary");
    }
  }

  @TestTemplate
  public void testTypeWidening() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // create temp view from Dataset
    spark.table(tableName).createOrReplaceTempView("tmp");

    List<Object[]> result = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result).hasSize(1).containsExactly(row(1, 100));

    // widen column type from INT to BIGINT
    sql("ALTER TABLE %s ALTER COLUMN salary TYPE BIGINT", tableName);

    // query view again - should fail with analysis exception
    // (type widening is not supported for temp views in current implementation)
    assertThatThrownBy(() -> sql("SELECT * FROM tmp"))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("type has changed from INT to BIGINT");
  }

  @TestTemplate
  public void testMultipleQueriesReflectLatestData() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // create temp view from Dataset with filter
    spark.table(tableName).filter("salary < 999").createOrReplaceTempView("tmp");

    // first query
    List<Object[]> result = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result).hasSize(1).containsExactly(row(1, 100));

    // add more data
    sql("INSERT INTO %s VALUES (2, 200)", tableName);

    // second query - should see new data
    List<Object[]> result2 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result2).hasSize(2).containsExactly(row(1, 100), row(2, 200));

    // add more data
    sql("INSERT INTO %s VALUES (3, 300)", tableName);

    // third query - should see all data
    List<Object[]> result3 = sql("SELECT * FROM tmp ORDER BY id");
    assertThat(result3).hasSize(3).containsExactly(row(1, 100), row(2, 200), row(3, 300));
  }
}
