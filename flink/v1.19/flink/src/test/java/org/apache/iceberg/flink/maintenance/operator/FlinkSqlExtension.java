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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Junit 5 extension for running Flink SQL queries. {@link
 * org.apache.flink.test.junit5.MiniClusterExtension} is used for executing the SQL batch jobs.
 */
public class FlinkSqlExtension implements BeforeEachCallback, AfterEachCallback {
  private final String catalogName;
  private final Map<String, String> catalogProperties;
  private final String databaseName;
  private final Path warehouse;
  private final CatalogLoader catalogLoader;
  private TableEnvironment tableEnvironment;

  public FlinkSqlExtension(
      String catalogName, Map<String, String> catalogProperties, String databaseName) {
    this.catalogName = catalogName;
    this.catalogProperties = Maps.newHashMap(catalogProperties);
    this.databaseName = databaseName;

    // Add temporary dir as a warehouse location
    try {
      this.warehouse = Files.createTempDirectory("warehouse");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.catalogProperties.put(
        CatalogProperties.WAREHOUSE_LOCATION, String.format("file://%s", warehouse));
    this.catalogLoader =
        CatalogLoader.hadoop(catalogName, new Configuration(), this.catalogProperties);
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    // We need to recreate the tableEnvironment for every test as the minicluster is recreated
    this.tableEnvironment =
        TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
    exec("CREATE CATALOG %s WITH %s", catalogName, toWithClause(catalogProperties));
    exec("CREATE DATABASE %s.%s", catalogName, databaseName);
    exec("USE CATALOG %s", catalogName);
    exec("USE %s", databaseName);
  }

  @Override
  public void afterEach(ExtensionContext context) throws IOException {
    List<Row> tables = exec("SHOW TABLES");
    tables.forEach(t -> exec("DROP TABLE IF EXISTS %s", t.getField(0)));
    exec("USE CATALOG default_catalog");
    exec("DROP CATALOG IF EXISTS %s", catalogName);
    Files.walk(warehouse).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
  }

  /**
   * Executes an SQL query with the given parameters. The parameter substitution is done by {@link
   * String#format(String, Object...)}.
   *
   * @param query to run
   * @param parameters to substitute to the query
   * @return The {@link Row}s returned by the query
   */
  public List<Row> exec(String query, Object... parameters) {
    TableResult tableResult = tableEnvironment.executeSql(String.format(query, parameters));
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      return Lists.newArrayList(iter);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }

  /**
   * Returns the {@link TableLoader} which could be used to access the given table.
   *
   * @param tableName of the table
   * @return the {@link TableLoader} for the table
   */
  public TableLoader tableLoader(String tableName) {
    TableLoader tableLoader =
        TableLoader.fromCatalog(catalogLoader, TableIdentifier.of(databaseName, tableName));
    tableLoader.open();
    return tableLoader;
  }

  private static String toWithClause(Map<String, String> props) {
    return String.format(
        "(%s)",
        props.entrySet().stream()
            .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
            .collect(Collectors.joining(",")));
  }
}
