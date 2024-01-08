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
package org.apache.iceberg.spark.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestTimestampWithoutZone extends CatalogTestBase {

  private static final String newTableName = "created_table";
  private static final Schema schema =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.required(3, "tsz", Types.TimestampType.withZone()));

  private final List<Object[]> values =
      ImmutableList.of(
          row(1L, toLocalDateTime("2021-01-01T00:00:00.0"), toTimestamp("2021-02-01T00:00:00.0")),
          row(2L, toLocalDateTime("2021-01-01T00:00:00.0"), toTimestamp("2021-02-01T00:00:00.0")),
          row(3L, toLocalDateTime("2021-01-01T00:00:00.0"), toTimestamp("2021-02-01T00:00:00.0")));

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "parquet-enabled", "true",
            "cache-enabled", "false")
      }
    };
  }

  @BeforeEach
  public void createTables() {
    validationCatalog.createTable(tableIdent, schema);
  }

  @AfterEach
  public void removeTables() {
    validationCatalog.dropTable(tableIdent, true);
    sql("DROP TABLE IF EXISTS %s", newTableName);
  }

  /*
  Spark does not really care about the timezone, it will just convert it

  spark-sql (default)> CREATE TABLE t1 (tz TIMESTAMP, ntz TIMESTAMP_NTZ);
  Time taken: 1.925 seconds

  spark-sql (default)> INSERT INTO t1 VALUES(timestamp '2020-01-01T00:00:00+02:00', timestamp_ntz '2020-01-01T00:00:00');
  Time taken: 1.355 seconds
  spark-sql (default)> INSERT INTO t1 VALUES(timestamp_ntz '2020-01-01T00:00:00+02:00', timestamp_ntz '2020-01-01T00:00:00');
  Time taken: 0.129 seconds
  spark-sql (default)> INSERT INTO t1 VALUES(timestamp_ntz '2020-01-01T00:00:00+02:00', timestamp '2020-01-01T00:00:00');
  Time taken: 0.125 seconds
  spark-sql (default)> INSERT INTO t1 VALUES(timestamp '2020-01-01T00:00:00+02:00', timestamp '2020-01-01T00:00:00');
  Time taken: 0.122 seconds

  spark-sql (default)> select * from t1;
  2020-01-01 00:00:00    2020-01-01 00:00:00
  2020-01-01 00:00:00    2020-01-01 00:00:00
  2019-12-31 23:00:00    2020-01-01 00:00:00
  2019-12-31 23:00:00    2020-01-01 00:00:00
  Time taken: 0.32 seconds, Fetched 4 row(s)

  spark-sql (default)> SELECT count(1) FROM t1 JOIN t1 as t2 ON t1.tz = t2.ntz;
  8
   */

  @TestTemplate
  public void testAppendTimestampWithoutZone() {
    // Both NTZ
    sql(
        "INSERT INTO %s VALUES %s",
        tableName,
        rowToSqlValues(
            ImmutableList.of(
                row(
                    1L,
                    toLocalDateTime("2021-01-01T00:00:00.0"),
                    toLocalDateTime("2021-02-01T00:00:00.0")))));
  }

  @TestTemplate
  public void testAppendTimestampWithZone() {
    // Both TZ
    sql(
        "INSERT INTO %s VALUES %s",
        tableName,
        rowToSqlValues(
            ImmutableList.of(
                row(
                    1L,
                    toTimestamp("2021-01-01T00:00:00.0"),
                    toTimestamp("2021-02-01T00:00:00.0")))));
  }

  @TestTemplate
  public void testCreateAsSelectWithTimestampWithoutZone() {
    sql("INSERT INTO %s VALUES %s", tableName, rowToSqlValues(values));

    sql("CREATE TABLE %s USING iceberg AS SELECT * FROM %s", newTableName, tableName);

    assertThat(scalarSql("SELECT count(*) FROM %s", newTableName))
        .as("Should have " + values.size() + " row")
        .isEqualTo((long) values.size());

    assertEquals(
        "Row data should match expected",
        sql("SELECT * FROM %s ORDER BY id", tableName),
        sql("SELECT * FROM %s ORDER BY id", newTableName));
  }

  @TestTemplate
  public void testCreateNewTableShouldHaveTimestampWithZoneIcebergType() {
    sql("INSERT INTO %s VALUES %s", tableName, rowToSqlValues(values));

    sql("CREATE TABLE %s USING iceberg AS SELECT * FROM %s", newTableName, tableName);

    assertThat(scalarSql("SELECT count(*) FROM %s", newTableName))
        .as("Should have " + values.size() + " row")
        .isEqualTo((long) values.size());

    assertEquals(
        "Data from created table should match data from base table",
        sql("SELECT * FROM %s ORDER BY id", tableName),
        sql("SELECT * FROM %s ORDER BY id", newTableName));

    Table createdTable = validationCatalog.loadTable(TableIdentifier.of("default", newTableName));
    assertFieldsType(createdTable.schema(), Types.TimestampType.withoutZone(), "ts");
    assertFieldsType(createdTable.schema(), Types.TimestampType.withZone(), "tsz");
  }

  @TestTemplate
  public void testCreateNewTableShouldHaveTimestampWithoutZoneIcebergType() {
    spark
        .sessionState()
        .catalogManager()
        .currentCatalog()
        .initialize(catalog.name(), new CaseInsensitiveStringMap(catalogConfig));
    sql("INSERT INTO %s VALUES %s", tableName, rowToSqlValues(values));

    sql("CREATE TABLE %s USING iceberg AS SELECT * FROM %s", newTableName, tableName);

    assertThat(scalarSql("SELECT count(*) FROM %s", newTableName))
        .as("Should have " + values.size() + " row")
        .isEqualTo((long) values.size());

    assertEquals(
        "Row data should match expected",
        sql("SELECT * FROM %s ORDER BY id", tableName),
        sql("SELECT * FROM %s ORDER BY id", newTableName));
    Table createdTable = validationCatalog.loadTable(TableIdentifier.of("default", newTableName));
    assertFieldsType(createdTable.schema(), Types.TimestampType.withoutZone(), "ts");
    assertFieldsType(createdTable.schema(), Types.TimestampType.withZone(), "tsz");
  }

  private Timestamp toTimestamp(String value) {
    return new Timestamp(DateTime.parse(value).getMillis());
  }

  private LocalDateTime toLocalDateTime(String value) {
    return LocalDateTime.parse(value);
  }

  private String rowToSqlValues(List<Object[]> rows) {
    List<String> rowValues =
        rows.stream()
            .map(
                row -> {
                  List<String> columns =
                      Arrays.stream(row)
                          .map(
                              value -> {
                                if (value instanceof Long) {
                                  return value.toString();
                                } else if (value instanceof Timestamp) {
                                  return String.format("timestamp '%s'", value);
                                } else if (value instanceof LocalDateTime) {
                                  return String.format("timestamp_ntz '%s'", value);
                                }
                                throw new RuntimeException("Type is not supported");
                              })
                          .collect(Collectors.toList());
                  return "(" + Joiner.on(",").join(columns) + ")";
                })
            .collect(Collectors.toList());
    return Joiner.on(",").join(rowValues);
  }

  private void assertFieldsType(Schema actual, Type.PrimitiveType expected, String... fields) {
    actual
        .select(fields)
        .asStruct()
        .fields()
        .forEach(field -> assertThat(field.type()).isEqualTo(expected));
  }
}
