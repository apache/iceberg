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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestTimestampWithoutZone extends SparkCatalogTestBase {

  private static final String NEW_TABLE_NAME = "created_table";
  private final Map<String, String> config;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.required(3, "tsz", Types.TimestampType.withZone()));

  private final List<Object[]> values =
      ImmutableList.of(
          row(1L, toTimestamp("2021-01-01T00:00:00.0"), toTimestamp("2021-02-01T00:00:00.0")),
          row(2L, toTimestamp("2021-01-01T00:00:00.0"), toTimestamp("2021-02-01T00:00:00.0")),
          row(3L, toTimestamp("2021-01-01T00:00:00.0"), toTimestamp("2021-02-01T00:00:00.0")));

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
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

  public TestTimestampWithoutZone(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    this.config = config;
  }

  @Before
  public void createTables() {
    validationCatalog.createTable(tableIdent, SCHEMA);
  }

  @After
  public void removeTables() {
    validationCatalog.dropTable(tableIdent, true);
    sql("DROP TABLE IF EXISTS %s", NEW_TABLE_NAME);
  }

  @Test
  public void testWriteTimestampWithoutZoneError() {
    assertThatThrownBy(() -> sql("INSERT INTO %s VALUES %s", tableName, rowToSqlValues(values)))
        .as(
            String.format(
                "Write operation performed on a timestamp without timezone field while "
                    + "'%s' set to false should throw exception",
                SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SparkUtil.TIMESTAMP_WITHOUT_TIMEZONE_ERROR);
  }

  @Test
  public void testAppendTimestampWithoutZone() {
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE, "true"),
        () -> {
          sql("INSERT INTO %s VALUES %s", tableName, rowToSqlValues(values));

          Assert.assertEquals(
              "Should have " + values.size() + " row",
              (long) values.size(),
              scalarSql("SELECT count(*) FROM %s", tableName));

          assertEquals(
              "Row data should match expected",
              values,
              sql("SELECT * FROM %s ORDER BY id", tableName));
        });
  }

  @Test
  public void testCreateAsSelectWithTimestampWithoutZone() {
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE, "true"),
        () -> {
          sql("INSERT INTO %s VALUES %s", tableName, rowToSqlValues(values));

          sql("CREATE TABLE %s USING iceberg AS SELECT * FROM %s", NEW_TABLE_NAME, tableName);

          Assert.assertEquals(
              "Should have " + values.size() + " row",
              (long) values.size(),
              scalarSql("SELECT count(*) FROM %s", NEW_TABLE_NAME));

          assertEquals(
              "Row data should match expected",
              sql("SELECT * FROM %s ORDER BY id", tableName),
              sql("SELECT * FROM %s ORDER BY id", NEW_TABLE_NAME));
        });
  }

  @Test
  public void testCreateNewTableShouldHaveTimestampWithZoneIcebergType() {
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE, "true"),
        () -> {
          sql("INSERT INTO %s VALUES %s", tableName, rowToSqlValues(values));

          sql("CREATE TABLE %s USING iceberg AS SELECT * FROM %s", NEW_TABLE_NAME, tableName);

          Assert.assertEquals(
              "Should have " + values.size() + " row",
              (long) values.size(),
              scalarSql("SELECT count(*) FROM %s", NEW_TABLE_NAME));

          assertEquals(
              "Data from created table should match data from base table",
              sql("SELECT * FROM %s ORDER BY id", tableName),
              sql("SELECT * FROM %s ORDER BY id", NEW_TABLE_NAME));

          Table createdTable =
              validationCatalog.loadTable(TableIdentifier.of("default", NEW_TABLE_NAME));
          assertFieldsType(createdTable.schema(), Types.TimestampType.withZone(), "ts", "tsz");
        });
  }

  @Test
  public void testCreateNewTableShouldHaveTimestampWithoutZoneIcebergType() {
    withSQLConf(
        ImmutableMap.of(
            SparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE, "true",
            SparkSQLProperties.USE_TIMESTAMP_WITHOUT_TIME_ZONE_IN_NEW_TABLES, "true"),
        () -> {
          spark
              .sessionState()
              .catalogManager()
              .currentCatalog()
              .initialize(catalog.name(), new CaseInsensitiveStringMap(config));
          sql("INSERT INTO %s VALUES %s", tableName, rowToSqlValues(values));

          sql("CREATE TABLE %s USING iceberg AS SELECT * FROM %s", NEW_TABLE_NAME, tableName);

          Assert.assertEquals(
              "Should have " + values.size() + " row",
              (long) values.size(),
              scalarSql("SELECT count(*) FROM %s", NEW_TABLE_NAME));

          assertEquals(
              "Row data should match expected",
              sql("SELECT * FROM %s ORDER BY id", tableName),
              sql("SELECT * FROM %s ORDER BY id", NEW_TABLE_NAME));
          Table createdTable =
              validationCatalog.loadTable(TableIdentifier.of("default", NEW_TABLE_NAME));
          assertFieldsType(createdTable.schema(), Types.TimestampType.withoutZone(), "ts", "tsz");
        });
  }

  private Timestamp toTimestamp(String value) {
    return new Timestamp(DateTime.parse(value).getMillis());
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
        .forEach(field -> Assert.assertEquals(expected, field.type()));
  }
}
