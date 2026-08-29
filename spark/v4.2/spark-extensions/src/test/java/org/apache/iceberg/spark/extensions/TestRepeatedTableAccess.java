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
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRepeatedTableAccess extends ExtensionsTestBase {

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
  }

  @TestTemplate
  public void testRepeatedAccessWithExternalWrite() throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // query table
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer adds (2, 200)
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // query table again
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    if (cachingCatalogEnabled()) {
      assertThat(result2)
          .as("Repeated access should not reflect external writes when cache is enabled")
          .hasSize(1)
          .containsExactly(row(1, 100));
    } else {
      assertThat(result2)
          .as("Repeated access should reflect external writes when cache is disabled")
          .hasSize(2)
          .containsExactly(row(1, 100), row(2, 200));
    }
  }

  @TestTemplate
  public void testRepeatedAccessWithExternalSchemaChangeAddColumn() throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // query table
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100));

    // external writer adds new column
    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSchema().addColumn("new_column", Types.IntegerType.get()).commit();

    // external writer adds (2, 200, -1)
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    record.setField("new_column", -1);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // query table again
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    if (cachingCatalogEnabled()) {
      assertThat(result2)
          .as("Repeated access should not reflect external schema changes when cache is enabled")
          .hasSize(1)
          .containsExactly(row(1, 100));
    } else {
      assertThat(result2)
          .as("Repeated access should reflect external schema changes when cache is disabled")
          .hasSize(2)
          .containsExactly(row(1, 100, null), row(2, 200, -1));
    }
  }

  @TestTemplate
  public void testRepeatedAccessWithExternalSchemaChangeDropColumn() throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT, extra INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100, 10)", tableName);

    // query table
    List<Object[]> result1 = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(result1).hasSize(1).containsExactly(row(1, 100, 10));

    // external writer drops column
    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSchema().deleteColumn("extra").commit();

    // external writer adds (2, 200)
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // query table again
    List<Object[]> result2 = sql("SELECT * FROM %s ORDER BY id", tableName);
    if (cachingCatalogEnabled()) {
      assertThat(result2)
          .as("Repeated access should not reflect external schema changes when cache is enabled")
          .hasSize(1)
          .containsExactly(row(1, 100, 10));
    } else {
      assertThat(result2)
          .as("Repeated access should reflect external schema changes when cache is disabled")
          .hasSize(2)
          .containsExactly(row(1, 100), row(2, 200));
    }
  }

  @TestTemplate
  public void testRepeatedAccessWithExternalDropAndRecreateTable() {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // query table
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
          .as("Repeated access should return stale data from old table when cache is enabled")
          .hasSize(1)
          .containsExactly(row(1, 100));
    } else {
      assertThat(result2)
          .as("Repeated access should resolve to newly created table when cache is disabled")
          .isEmpty();
    }
  }
}
