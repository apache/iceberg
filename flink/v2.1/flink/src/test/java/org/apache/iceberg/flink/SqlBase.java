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

import static org.apache.iceberg.flink.FlinkCatalogFactory.DEFAULT_CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public abstract class SqlBase {
  protected abstract TableEnvironment getTableEnv();

  protected static TableResult exec(TableEnvironment env, String query, Object... args) {
    return env.executeSql(String.format(query, args));
  }

  protected TableResult exec(String query, Object... args) {
    return exec(getTableEnv(), query, args);
  }

  protected List<Row> sql(String query, Object... args) {
    TableResult tableResult = exec(query, args);
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      return Lists.newArrayList(iter);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }

  protected void assertSameElements(Iterable<Row> expected, Iterable<Row> actual) {
    assertThat(actual).isNotNull().containsExactlyInAnyOrderElementsOf(expected);
  }

  protected void assertSameElements(String message, Iterable<Row> expected, Iterable<Row> actual) {
    assertThat(actual).isNotNull().as(message).containsExactlyInAnyOrderElementsOf(expected);
  }

  /**
   * We can not drop currently used catalog after FLINK-29677, so we have make sure that we do not
   * use the current catalog before dropping it. This method switches to the 'default_catalog' and
   * drops the one requested.
   *
   * @param catalogName The catalog to drop
   * @param ifExists If we should use the 'IF EXISTS' when dropping the catalog
   */
  protected void dropCatalog(String catalogName, boolean ifExists) {
    sql("USE CATALOG %s", DEFAULT_CATALOG_NAME);
    sql("DROP CATALOG %s %s", ifExists ? "IF EXISTS" : "", catalogName);
  }

  /**
   * We can not drop currently used database after FLINK-33226, so we have make sure that we do not
   * use the current database before dropping it. This method switches to the default database in
   * the default catalog, and then it and drops the one requested.
   *
   * @param database The database to drop
   * @param ifExists If we should use the 'IF EXISTS' when dropping the database
   */
  protected void dropDatabase(String database, boolean ifExists) {
    String currentCatalog = getTableEnv().getCurrentCatalog();
    sql("USE CATALOG %s", DEFAULT_CATALOG_NAME);
    sql("USE %s", getTableEnv().listDatabases()[0]);
    sql("USE CATALOG %s", currentCatalog);
    sql("DROP DATABASE %s %s", ifExists ? "IF EXISTS" : "", database);
  }

  protected static String toWithClause(Map<String, String> props) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    int propCount = 0;
    for (Map.Entry<String, String> entry : props.entrySet()) {
      if (propCount > 0) {
        builder.append(",");
      }
      builder
          .append("'")
          .append(entry.getKey())
          .append("'")
          .append("=")
          .append("'")
          .append(entry.getValue())
          .append("'");
      propCount++;
    }
    builder.append(")");
    return builder.toString();
  }
}
