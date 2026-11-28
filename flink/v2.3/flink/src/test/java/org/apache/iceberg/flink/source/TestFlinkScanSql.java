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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test Flink SELECT SQLs. */
public class TestFlinkScanSql extends TestFlinkSource {
  private volatile TableEnvironment tEnv;

  @BeforeEach
  public void before() throws IOException {
    SqlHelpers.sql(
        getTableEnv(),
        "create catalog iceberg_catalog with ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        CATALOG_EXTENSION.warehouse());
    SqlHelpers.sql(getTableEnv(), "use catalog iceberg_catalog");
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
  }

  @Test
  public void testDecimalLiteralAgainstDoubleColumn() {
    SqlHelpers.sql(getTableEnv(), "CREATE TABLE t_double_decimal_filter (id INT, score DOUBLE)");

    SqlHelpers.sql(
        getTableEnv(),
        "INSERT INTO t_double_decimal_filter VALUES " + "(1, 10.5), (2, 14.3), (3, 20.0)");

    List<Row> rows =
        SqlHelpers.sql(
            getTableEnv(),
            "SELECT id, score FROM t_double_decimal_filter " + "WHERE score <= 14.3 ORDER BY id");

    assertThat(rows).containsExactly(Row.of(1, 10.5), Row.of(2, 14.3));
  }

  private TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          this.tEnv =
              TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        }
      }
    }
    return tEnv;
  }

  @Override
  protected List<Row> run(
      FlinkSource.Builder formatBuilder,
      Map<String, String> sqlOptions,
      String sqlFilter,
      String... sqlSelectedFields) {
    String select = String.join(",", sqlSelectedFields);
    String optionStr = SqlHelpers.sqlOptionsToString(sqlOptions);
    return SqlHelpers.sql(getTableEnv(), "select %s from t %s %s", select, optionStr, sqlFilter);
  }
}
