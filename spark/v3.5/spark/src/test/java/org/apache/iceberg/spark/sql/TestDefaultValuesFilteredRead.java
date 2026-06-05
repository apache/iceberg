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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Reading an initial-default column that is absent from older data files must return the default
 * for the backfilled rows even when the query filters on that column. Before the row-group filter
 * became default-aware, a file written before the column existed was treated as all-null and
 * skipped, silently dropping the backfilled rows on {@code WHERE col = <default>} / {@code col IS
 * NOT NULL}.
 */
public class TestDefaultValuesFilteredRead extends CatalogTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      }
    };
  }

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void filteredReadOverInitialDefaultColumnAbsentFromFile() {
    // F_old is written before column c exists, so c is physically absent from that file
    sql(
        "CREATE TABLE %s (id bigint, name string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3','write.format.default'='parquet')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'Alice')", tableName);

    // add optional column c with initial-default 'US' (absent from F_old, backfilled on read)
    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSchema().addColumn("c", Types.StringType.get(), Expressions.lit("US")).commit();
    sql("REFRESH TABLE %s", tableName);

    // F_new is written after c was added, with c = 'US' physically present
    sql("INSERT INTO %s VALUES (2, 'Bob', 'US')", tableName);
    sql("REFRESH TABLE %s", tableName);

    // full scan materializes the default for the backfilled row (id = 1)
    assertThat(ids("SELECT id FROM %s ORDER BY id", tableName)).containsExactly(1L, 2L);

    // filtering on the defaulted column keeps the backfilled row
    assertThat(ids("SELECT id FROM %s WHERE c = 'US' ORDER BY id", tableName))
        .containsExactly(1L, 2L);
    // even an expression Iceberg cannot push (Spark still infers and pushes IsNotNull(c))
    assertThat(ids("SELECT id FROM %s WHERE upper(c) = 'US' ORDER BY id", tableName))
        .containsExactly(1L, 2L);
    assertThat(ids("SELECT id FROM %s WHERE c IS NOT NULL ORDER BY id", tableName))
        .containsExactly(1L, 2L);

    // a non-matching constant still correctly excludes the absent-column file
    assertThat(ids("SELECT id FROM %s WHERE c = 'CA' ORDER BY id", tableName)).isEmpty();
    assertThat(ids("SELECT id FROM %s WHERE c IS NULL ORDER BY id", tableName)).isEmpty();
  }

  private List<Long> ids(String query, Object... args) {
    return sql(query, args).stream().map(row -> (Long) row[0]).collect(Collectors.toList());
  }
}
