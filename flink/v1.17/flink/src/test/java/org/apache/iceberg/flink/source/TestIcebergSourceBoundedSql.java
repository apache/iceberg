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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;

public class TestIcebergSourceBoundedSql extends TestIcebergSourceBounded {
  private volatile TableEnvironment tEnv;

  @BeforeEach
  public void before() throws IOException {
    Configuration tableConf = getTableEnv().getConfig().getConfiguration();
    tableConf.setBoolean(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE.key(), true);
    SqlHelpers.sql(
        getTableEnv(),
        "create catalog iceberg_catalog with ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        catalogExtension.warehouse());
    SqlHelpers.sql(getTableEnv(), "use catalog iceberg_catalog");
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
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
      Schema projectedSchema,
      List<Expression> filters,
      Map<String, String> options,
      String sqlFilter,
      String... sqlSelectedFields)
      throws Exception {
    String select = String.join(",", sqlSelectedFields);
    String optionStr = SqlHelpers.sqlOptionsToString(options);
    TableResult tableResult =
        getTableEnv()
            .executeSql(String.format("select %s from t %s %s", select, optionStr, sqlFilter));
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      return Lists.newArrayList(iter);
    } catch (Exception e) {
      // To retrieve the underlying exception information that actually caused the task failure.
      //      throw (RuntimeException)
      // e.getCause().getCause().getCause().getCause().getCause().getCause();
      throw (RuntimeException) ExceptionUtils.getRootCause(e);
    }
  }
}
