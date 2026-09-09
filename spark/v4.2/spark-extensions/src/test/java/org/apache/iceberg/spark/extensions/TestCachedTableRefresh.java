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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCachedTableRefresh extends ExtensionsTestBase {

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
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("UNCACHE TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testCachedTableWithExternalWrite() throws IOException {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // cache table
    sql("CACHE TABLE %s", tableName);

    // query table to populate cache
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer adds (2, 200)
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // query table again - should return cached data, not new data
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result2)
        .as("Cached table should not reflect external writes")
        .hasSize(1)
        .containsExactly(row(1, 100));
  }

  @TestTemplate
  public void testCachedTableWithSessionWrite() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // cache table
    sql("CACHE TABLE %s", tableName);

    // query table to populate cache
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // session write adds (2, 200) - should invalidate cache
    sql("INSERT INTO %s VALUES (2, 200)", tableName);

    // query table again - should reflect session writes
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result2)
        .as("Cached table should reflect session writes")
        .hasSize(2)
        .containsExactly(row(1, 100), row(2, 200));
  }

  @TestTemplate
  public void testCachedTableWithSessionWriteAndExternalWrite() throws IOException {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // cache table
    sql("CACHE TABLE %s", tableName);

    // query table to populate cache
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // session write invalidates cache
    sql("INSERT INTO %s VALUES (2, 200)", tableName);

    // external writer adds (3, 300)
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 3);
    record.setField("salary", 300);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // query table again - should see session write but not external write
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result2)
        .as("Cached table should reflect session writes but not external writes after refresh")
        .hasSize(2)
        .containsExactly(row(1, 100), row(2, 200));
  }

  @TestTemplate
  public void testCachedTableWithExternalSchemaChangeAddColumn() throws IOException {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // cache table
    sql("CACHE TABLE %s", tableName);

    // query table to populate cache
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer adds column and data
    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSchema().addColumn("new_column", Types.IntegerType.get()).commit();

    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    record.setField("new_column", -1);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // query table again - should return cached data with original schema
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result2)
        .as("Cached table should pin metadata and not reflect external schema changes")
        .hasSize(1)
        .containsExactly(row(1, 100));
  }

  @TestTemplate
  public void testCachedTableWithSessionSchemaChangeAddColumn() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // cache table
    sql("CACHE TABLE %s", tableName);

    // query table to populate cache
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // session DDL adds column - should invalidate cache
    sql("ALTER TABLE %s ADD COLUMN new_column INT", tableName);
    sql("INSERT INTO %s VALUES (2, 200, -1)", tableName);

    // query table again - should reflect session schema changes
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result2)
        .as("Cached table should reflect session schema changes")
        .hasSize(2)
        .containsExactly(row(1, 100, null), row(2, 200, -1));
  }

  @TestTemplate
  @Disabled("https://issues.apache.org/jira/browse/SPARK-55631")
  public void testCachedTableWithSessionSchemaChangeAndExternalWrite() throws IOException {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // cache table
    sql("CACHE TABLE %s", tableName);

    // query table to populate cache
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // session DDL invalidates cache
    sql("ALTER TABLE %s ADD COLUMN new_column INT", tableName);

    // external writer adds data with new column
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    record.setField("new_column", -1);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // query table again - should see new schema but not external data
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result2)
        .as(
            "Cached table should reflect session schema change but not external write after refresh")
        .hasSize(1)
        .containsExactly(row(1, 100, null));
  }

  @TestTemplate
  public void testCachedTableWithExternalDropAndRecreateTable() {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // cache table
    sql("CACHE TABLE %s", tableName);

    // query table to populate cache
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer drops and recreates table
    validationCatalog.dropTable(tableIdent, false /* keep files */);
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "salary", Types.IntegerType.get()));
    validationCatalog.createTable(tableIdent, schema);

    // query table again
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    if (cachingCatalogEnabled()) {
      assertThat(result2)
          .as("Table cache should prevent from seeing new table ID")
          .hasSize(1)
          .containsExactly(row(1, 100));
    } else {
      assertThat(result2).as("Table ID change must prevent cache match").isEmpty();
    }
  }

  @TestTemplate
  public void testCachedTableUncacheReflectsExternalChanges() throws IOException {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // cache table
    sql("CACHE TABLE %s", tableName);

    // query table to populate cache
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer adds (2, 200)
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // uncache table
    sql("UNCACHE TABLE %s", tableName);

    // query table again
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    if (cachingCatalogEnabled()) {
      assertThat(result2)
          .as("Table cache should prevent from seeing external changes")
          .hasSize(1)
          .containsExactly(row(1, 100));
    } else {
      assertThat(result2)
          .as("Table should reflect external writes if table cache is disabled")
          .hasSize(2)
          .containsExactly(row(1, 100), row(2, 200));
    }
  }

  @TestTemplate
  public void testCachedTableRefreshReflectsExternalChanges() throws IOException {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // cache table
    sql("CACHE TABLE %s", tableName);

    // query table to populate cache
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer adds (2, 200)
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // refresh table cache
    sql("REFRESH TABLE %s", tableName);

    // query table again - should reflect external changes after explicit refresh
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result2)
        .as("Refreshed cached table should reflect external writes")
        .hasSize(2)
        .containsExactly(row(1, 100), row(2, 200));
  }

  @TestTemplate
  public void testCachedTableMultipleQueriesReturnCachedData() throws IOException {
    // create table and insert initial data
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // cache table
    sql("CACHE TABLE %s", tableName);

    // first query to populate cache
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer adds (2, 200)
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // second query - should return cached data
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result2)
        .as("Second query should return cached data")
        .hasSize(1)
        .containsExactly(row(1, 100));

    // external writer adds (3, 300)
    Record record2 = GenericRecord.create(table.schema());
    record2.setField("id", 3);
    record2.setField("salary", 300);
    DataFile dataFile2 = writeData(table, ImmutableList.of(record2));
    table.newAppend().appendFile(dataFile2).commit();

    // third query - should still return cached data
    List<Object[]> result3 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result3)
        .as("Third query should return cached data")
        .hasSize(1)
        .containsExactly(row(1, 100));
  }
}
