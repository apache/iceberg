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
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIncrementallyConstructedQueries extends ExtensionsTestBase {

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
        SparkCatalogConfig.SPARK_SESSION.properties()
      }
    };
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testIncrementalQueryWithExternalWrite() throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // resolves to table version X (QueryExecution #1)
    Dataset<Row> df1 = spark.table(tableName);

    // external writer adds (2, 200)
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // resolves to table version X + 1 (QueryExecution #2)
    Dataset<Row> df2 = spark.table(tableName);

    // join two DataFrames (QueryExecution #3)
    Dataset<Row> joined = df1.join(df2, df1.col("id").equalTo(df2.col("id")));

    // query should refresh versions and use consistent snapshot
    List<Object[]> result = rowsToJava(joined.collectAsList());
    assertThat(result)
        .as("Join should use consistent table versions after refresh")
        .hasSize(2)
        .containsExactlyInAnyOrder(row(1, 100, 1, 100), row(2, 200, 2, 200));
  }

  @TestTemplate
  public void testIncrementalQueryWithExternalSchemaChangeAddColumn() throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // resolves to table version X (QueryExecution #1)
    Dataset<Row> df1 = spark.table(tableName);

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

    // resolves to table version X + 1 (QueryExecution #2)
    Dataset<Row> df2 = spark.table(tableName);

    // join two DataFrames (QueryExecution #3)
    Dataset<Row> joined = df1.join(df2, df1.col("id").equalTo(df2.col("id")));

    // query should refresh versions, preserve original schema for df1, use new schema for df2
    List<Object[]> result = rowsToJava(joined.collectAsList());
    assertThat(result)
        .as("Join should use consistent versions with compatible schemas")
        .hasSize(2)
        .containsExactlyInAnyOrder(row(1, 100, 1, 100, null), row(2, 200, 2, 200, -1));
  }

  @TestTemplate
  public void testIncrementalQueryWithExternalSchemaChangeDropColumn() throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT, extra INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100, 10)", tableName);

    // resolves to table version X (QueryExecution #1)
    Dataset<Row> df1 = spark.table(tableName);

    // external writer drops column
    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSchema().deleteColumn("extra").commit();

    // external writer adds (2, 200)
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // resolves to table version X + 1 (QueryExecution #2)
    Dataset<Row> df2 = spark.table(tableName);

    // join two DataFrames (QueryExecution #3)
    Dataset<Row> joined = df1.join(df2, df1.col("id").equalTo(df2.col("id")));

    // query should fail due to incompatible schema change
    assertThatThrownBy(joined::collect)
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("extra");
  }

  @TestTemplate
  public void testIncrementalQueryWithExternalDropAndRecreateTable() {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // resolves to table version X (QueryExecution #1)
    Dataset<Row> df1 = spark.table(tableName);

    // external writer drops and recreates table with same schema
    validationCatalog.dropTable(tableIdent, false /* keep files */);
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "salary", Types.IntegerType.get()));
    validationCatalog.createTable(tableIdent, schema);

    // resolves to table version X + 1 (QueryExecution #2)
    Dataset<Row> df2 = spark.table(tableName);

    // join two DataFrames (QueryExecution #3)
    Dataset<Row> joined = df1.join(df2, df1.col("id").equalTo(df2.col("id")));

    if (cachingCatalogEnabled()) {
      assertThat(rowsToJava(joined.collectAsList()))
          .as("Table cache should prevent from seeing external changes")
          .hasSize(2)
          .containsExactlyInAnyOrder(row(1, 100), row(1, 100));
    } else {
      assertThatThrownBy(joined::collect)
          .isInstanceOf(AnalysisException.class)
          .hasMessageContaining("Table ID has changed");
    }
  }

  @TestTemplate
  public void testIncrementalQueryWithExternalDropAndAddColumnSameNameSameType()
      throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // resolves to table version X (QueryExecution #1)
    Dataset<Row> df1 = spark.table(tableName);

    // external writer drops and adds column with same name and type
    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSchema().deleteColumn("salary").commit();
    table.updateSchema().addColumn("salary", Types.IntegerType.get()).commit();

    // external writer adds (2, 200)
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // resolves to table version X + 1 (QueryExecution #2)
    Dataset<Row> df2 = spark.table(tableName);

    // join two DataFrames (QueryExecution #3)
    Dataset<Row> joined = df1.join(df2, df1.col("id").equalTo(df2.col("id")));

    // query should resolve columns by name and show null for old data
    List<Object[]> result = rowsToJava(joined.collectAsList());
    assertThat(result)
        .as("Join should resolve columns by name")
        .hasSize(2)
        .containsExactlyInAnyOrder(row(1, null, 1, null), row(2, 200, 2, 200));
  }

  @TestTemplate
  public void testIncrementalQueryWithExternalDropAndAddColumnSameNameDifferentType()
      throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100)", tableName);

    // resolves to table version X (QueryExecution #1)
    Dataset<Row> df1 = spark.table(tableName);

    // external writer drops and adds column with same name but different type
    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSchema().deleteColumn("salary").commit();
    table.updateSchema().addColumn("salary", Types.StringType.get()).commit();

    // external writer adds (2, "BBB")
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", "BBB");
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // resolves to table version X + 1 (QueryExecution #2)
    Dataset<Row> df2 = spark.table(tableName);

    // join two DataFrames (QueryExecution #3)
    Dataset<Row> joined = df1.join(df2, df1.col("id").equalTo(df2.col("id")));

    // query should fail due to incompatible type change
    assertThatThrownBy(joined::collect)
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("salary");
  }

  @TestTemplate
  public void testIncrementalQueryMultipleReferencesConsistentVersions() throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100), (2, 200)", tableName);

    // resolves to table version X (QueryExecution #1)
    Dataset<Row> df1 = spark.table(tableName);

    // external writer adds (3, 300)
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 3);
    record.setField("salary", 300);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // resolves to table version X + 1 (QueryExecution #2)
    Dataset<Row> df2 = spark.table(tableName);

    // external writer adds (4, 400)
    Record record2 = GenericRecord.create(table.schema());
    record2.setField("id", 4);
    record2.setField("salary", 400);
    DataFile dataFile2 = writeData(table, ImmutableList.of(record2));
    table.newAppend().appendFile(dataFile2).commit();

    // resolves to table version X + 2 (QueryExecution #3)
    Dataset<Row> df3 = spark.table(tableName);

    // create complex query with multiple references (QueryExecution #4)
    Dataset<Row> joined1 = df1.join(df2, df1.col("id").equalTo(df2.col("id")));
    Dataset<Row> joined2 = joined1.join(df3, df1.col("id").equalTo(df3.col("id")));

    // all references should be aligned to consistent version
    List<Object[]> result = rowsToJava(joined2.collectAsList());
    assertThat(result)
        .as("Multiple table references should use consistent versions")
        .hasSize(4)
        .containsExactlyInAnyOrder(
            row(1, 100, 1, 100, 1, 100),
            row(2, 200, 2, 200, 2, 200),
            row(3, 300, 3, 300, 3, 300),
            row(4, 400, 4, 400, 4, 400));
  }

  @TestTemplate
  public void testIncrementalQueryWithFilterAndExternalWrite() throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100), (10, 1000)", tableName);

    // resolves to table version X with filter (QueryExecution #1)
    Dataset<Row> df1 = spark.table(tableName).filter("salary < 999");

    // external writer adds (2, 200)
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 2);
    record.setField("salary", 200);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // resolves to table version X + 1 with different filter (QueryExecution #2)
    Dataset<Row> df2 = spark.table(tableName).filter("salary > 150");

    // join two DataFrames (QueryExecution #3)
    Dataset<Row> joined = df1.join(df2, df1.col("id").equalTo(df2.col("id")));

    // both DataFrames should use consistent version and apply their respective filters
    List<Object[]> result = rowsToJava(joined.collectAsList());
    assertThat(result)
        .as("Join should apply filters on consistent table version")
        .hasSize(1)
        .containsExactly(row(2, 200, 2, 200));
  }

  @TestTemplate
  public void testIncrementalQueryWithAggregationAndExternalWrite() throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100), (2, 200)", tableName);

    // resolves to table version X (QueryExecution #1)
    Dataset<Row> df1 = spark.table(tableName);

    // external writer adds (3, 300)
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 3);
    record.setField("salary", 300);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // resolves to table version X + 1 with aggregation (QueryExecution #2)
    Dataset<Row> df2 = spark.table(tableName).groupBy("id").count();

    // join aggregated result with original (QueryExecution #3)
    Dataset<Row> joined = df1.join(df2, df1.col("id").equalTo(df2.col("id")));

    // both should use consistent version
    List<Object[]> result = rowsToJava(joined.collectAsList());
    assertThat(result)
        .as("Join with aggregation should use consistent table version")
        .hasSize(3)
        .containsExactlyInAnyOrder(row(1, 100, 1, 1L), row(2, 200, 2, 1L), row(3, 300, 3, 1L));
  }

  @TestTemplate
  public void testIncrementalQuerySelfJoinWithExternalWrite() throws IOException {
    // create table
    sql("CREATE TABLE %s (id INT, salary INT) USING iceberg", tableName);

    // insert initial data
    sql("INSERT INTO %s VALUES (1, 100), (2, 200)", tableName);

    // resolves to table version X (QueryExecution #1)
    Dataset<Row> df1 = spark.table(tableName);

    // external writer adds (3, 300)
    Table table = validationCatalog.loadTable(tableIdent);
    Record record = GenericRecord.create(table.schema());
    record.setField("id", 3);
    record.setField("salary", 300);
    DataFile dataFile = writeData(table, ImmutableList.of(record));
    table.newAppend().appendFile(dataFile).commit();

    // self-join using same DataFrame reference (QueryExecution #2)
    Dataset<Row> joined = df1.as("t1").join(df1.as("t2"), df1.col("id").equalTo(df1.col("id")));

    // self-join should use consistent version for both sides
    List<Object[]> result = rowsToJava(joined.collectAsList());
    assertThat(result)
        .as("Self-join should use consistent table version")
        .hasSize(3)
        .containsExactlyInAnyOrder(row(1, 100, 1, 100), row(2, 200, 2, 200), row(3, 300, 3, 300));
  }
}
