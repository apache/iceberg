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
package org.apache.iceberg.flink;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestBatchModification extends FlinkCatalogTestBase {
  private static final String TABLE_NAME = "test_batch_mod";
  private final Map<String, String> tableProps = Maps.newHashMap();
  private final boolean withPk;
  private final RowLevelOperationMode mode;

  public TestBatchModification(
      String catalogName, FileFormat format, boolean withPk, RowLevelOperationMode mode) {
    super(catalogName, Namespace.empty());
    tableProps.put(TableProperties.FORMAT_VERSION, "2");
    tableProps.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    this.withPk = withPk;
    this.mode = mode;
  }

  @Parameterized.Parameters(name = "catalogName={0}, format={1}, withPk={2}, modificationMode={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (RowLevelOperationMode mode :
        new RowLevelOperationMode[] {RowLevelOperationMode.COPY_ON_WRITE}) {
      for (boolean withPk : new boolean[] {true, false}) {
        for (FileFormat format :
            new FileFormat[] {FileFormat.PARQUET, FileFormat.AVRO, FileFormat.ORC}) {
          for (String catalogName : new String[] {"testhadoop", "testhive"}) {
            parameters.add(new Object[] {catalogName, format, withPk, mode});
          }
        }
      }
    }

    return parameters;
  }

  @Override
  @Before
  public void before() {
    super.before();
    sql("USE CATALOG %s", catalogName);
    sql("CREATE DATABASE IF NOT EXISTS %s", DATABASE);
    sql("USE %s", DATABASE);

    String pkSpec = withPk ? ", PRIMARY KEY(id,dt) NOT ENFORCED" : "";
    sql(
        "CREATE TABLE %s(id INT NOT NULL, name STRING NOT NULL, dt DATE %s) "
            + "PARTITIONED BY (dt) WITH %s",
        TABLE_NAME, pkSpec, toWithClause(tableProps));

    getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_MODIFICATION_MODE, mode.modeName());
  }

  @Override
  @After
  public void clean() {
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testBatchUpdate() {
    LocalDate dt20220301 = LocalDate.of(2022, 3, 1);
    LocalDate dt20220302 = LocalDate.of(2022, 3, 2);
    LocalDate dt20220303 = LocalDate.of(2022, 3, 3);

    try {
      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Jane', DATE '2022-03-01'),"
              + "(2, 'Bill', DATE '2022-03-01')",
          TABLE_NAME);

      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Jane', DATE '2022-03-02'),"
              + "(2, 'Jane', DATE '2022-03-02')",
          TABLE_NAME);

      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Bill', DATE '2022-03-03'),"
              + "(2, 'Bill', DATE '2022-03-03')",
          TABLE_NAME);

      sql("UPDATE %s SET name='Jack' WHERE name='Jane'", TABLE_NAME);

      List<Row> rowsOn20220301 =
          Lists.newArrayList(Row.of(1, "Jack", dt20220301), Row.of(2, "Bill", dt20220301));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-01'", TABLE_NAME), rowsOn20220301);

      List<Row> rowsOn20220302 =
          Lists.newArrayList(Row.of(1, "Jack", dt20220302), Row.of(2, "Jack", dt20220302));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-02'", TABLE_NAME), rowsOn20220302);

      List<Row> rowsOn20220303 =
          Lists.newArrayList(Row.of(1, "Bill", dt20220303), Row.of(2, "Bill", dt20220303));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-03'", TABLE_NAME), rowsOn20220303);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", TABLE_NAME),
          Lists.newArrayList(Iterables.concat(rowsOn20220301, rowsOn20220302, rowsOn20220303)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    }
  }

  @Test
  public void testBatchUpdateWithPartitionFilter() {
    LocalDate dt20220301 = LocalDate.of(2022, 3, 1);
    LocalDate dt20220302 = LocalDate.of(2022, 3, 2);
    LocalDate dt20220303 = LocalDate.of(2022, 3, 3);

    try {
      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Jane', DATE '2022-03-01'),"
              + "(2, 'Bill', DATE '2022-03-01')",
          TABLE_NAME);

      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Jane', DATE '2022-03-02'),"
              + "(2, 'Jane', DATE '2022-03-02')",
          TABLE_NAME);

      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Bill', DATE '2022-03-03'),"
              + "(2, 'Jane', DATE '2022-03-03')",
          TABLE_NAME);

      sql("UPDATE %s SET name='Jack' WHERE name='Jane' AND dt<='2022-03-02'", TABLE_NAME);

      List<Row> rowsOn20220301 =
          Lists.newArrayList(Row.of(1, "Jack", dt20220301), Row.of(2, "Bill", dt20220301));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-01'", TABLE_NAME), rowsOn20220301);

      List<Row> rowsOn20220302 =
          Lists.newArrayList(Row.of(1, "Jack", dt20220302), Row.of(2, "Jack", dt20220302));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-02'", TABLE_NAME), rowsOn20220302);

      List<Row> rowsOn20220303 =
          Lists.newArrayList(Row.of(1, "Bill", dt20220303), Row.of(2, "Jane", dt20220303));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-03'", TABLE_NAME), rowsOn20220303);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", TABLE_NAME),
          Lists.newArrayList(Iterables.concat(rowsOn20220301, rowsOn20220302, rowsOn20220303)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    }
  }

  @Test
  public void testBatchDelete() {
    LocalDate dt20220301 = LocalDate.of(2022, 3, 1);
    LocalDate dt20220303 = LocalDate.of(2022, 3, 3);

    try {
      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Jane', DATE '2022-03-01'),"
              + "(2, 'Bill', DATE '2022-03-01')",
          TABLE_NAME);

      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Jane', DATE '2022-03-02'),"
              + "(2, 'Jane', DATE '2022-03-02')",
          TABLE_NAME);

      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Bill', DATE '2022-03-03'),"
              + "(2, 'Bill', DATE '2022-03-03')",
          TABLE_NAME);

      sql("DELETE FROM %s WHERE name='Jane'", TABLE_NAME);

      List<Row> rowsOn20220301 = Lists.newArrayList(Row.of(2, "Bill", dt20220301));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-01'", TABLE_NAME), rowsOn20220301);

      List<Row> rowsOn20220302 = Lists.newArrayList();
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-02'", TABLE_NAME), rowsOn20220302);

      List<Row> rowsOn20220303 =
          Lists.newArrayList(Row.of(1, "Bill", dt20220303), Row.of(2, "Bill", dt20220303));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-03'", TABLE_NAME), rowsOn20220303);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", TABLE_NAME),
          Lists.newArrayList(Iterables.concat(rowsOn20220301, rowsOn20220302, rowsOn20220303)));
    } finally {
      sql("DROP TABLE IF EXISTS %s", TABLE_NAME);
    }
  }

  @Test
  public void testBatchDeleteWithPartitionFilter() {
    LocalDate dt20220301 = LocalDate.of(2022, 3, 1);
    LocalDate dt20220303 = LocalDate.of(2022, 3, 3);

    try {
      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Jane', DATE '2022-03-01'),"
              + "(2, 'Bill', DATE '2022-03-01')",
          TABLE_NAME);

      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Jane', DATE '2022-03-02'),"
              + "(2, 'Jane', DATE '2022-03-02')",
          TABLE_NAME);

      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Bill', DATE '2022-03-03'),"
              + "(2, 'Jane', DATE '2022-03-03')",
          TABLE_NAME);

      sql("DELETE FROM %s WHERE name='Jane' AND dt <= '2022-03-02'", TABLE_NAME);

      List<Row> rowsOn20220301 = Lists.newArrayList(Row.of(2, "Bill", dt20220301));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-01'", TABLE_NAME), rowsOn20220301);

      List<Row> rowsOn20220302 = Lists.newArrayList();
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-02'", TABLE_NAME), rowsOn20220302);

      List<Row> rowsOn20220303 =
          Lists.newArrayList(Row.of(1, "Bill", dt20220303), Row.of(2, "Jane", dt20220303));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-03'", TABLE_NAME), rowsOn20220303);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", TABLE_NAME),
          Lists.newArrayList(Iterables.concat(rowsOn20220301, rowsOn20220302, rowsOn20220303)));
    } finally {
      sql("DROP TABLE IF EXISTS %s", TABLE_NAME);
    }
  }
}
