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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestViews extends SparkExtensionsTestBase {
  private final String tableName = "table";
  private final String viewName = "view";

  @Before
  public void before() {
    spark.conf().set("spark.sql.defaultCatalog", SparkCatalogConfig.SPARK_WITH_VIEWS.catalogName());
    sql("CREATE NAMESPACE IF NOT EXISTS default");
    sql("CREATE TABLE %s (id INT, data STRING)", tableName);
  }

  @After
  public void removeTable() {
    sql("DROP VIEW IF EXISTS %s", viewName);
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK_WITH_VIEWS.catalogName(),
        SparkCatalogConfig.SPARK_WITH_VIEWS.implementation(),
        SparkCatalogConfig.SPARK_WITH_VIEWS.properties()
      }
    };
  }

  public TestViews(String catalog, String implementation, Map<String, String> properties) {
    super(catalog, implementation, properties);
  }

  @Test
  public void createView() throws NoSuchTableException {
    insertRows(10);

    sql("CREATE VIEW %s AS SELECT id FROM %s", viewName, tableName);

    List<Object[]> objects = sql("SELECT * FROM %s", viewName);
    List<Object[]> expected =
        IntStream.rangeClosed(1, 10).mapToObj(this::row).collect(Collectors.toList());

    assertThat(objects).hasSize(10).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void createMultipleViews() throws NoSuchTableException {
    insertRows(6);
    String secondView = "secondView";

    sql("CREATE VIEW %s AS SELECT id FROM %s WHERE id <= 3", viewName, tableName);
    sql("CREATE VIEW %s AS SELECT id FROM %s WHERE id > 3", secondView, tableName);

    List<Object[]> first = sql("SELECT * FROM %s", viewName);
    assertThat(first).hasSize(3).containsExactlyInAnyOrder(row(1), row(2), row(3));

    List<Object[]> second = sql("SELECT * FROM %s", secondView);
    assertThat(second).hasSize(3).containsExactlyInAnyOrder(row(4), row(5), row(6));
  }

  @Test
  public void createAlreadyExistingView() {
    sql("CREATE VIEW %s AS SELECT id FROM %s", viewName, tableName);

    assertThatThrownBy(() -> sql("CREATE VIEW %s AS SELECT id FROM %s", viewName, tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Cannot create view default.view because it already exists");

    // using IF NOT EXISTS should work
    sql("CREATE VIEW IF NOT EXISTS %s AS SELECT id FROM %s", viewName, tableName);
  }

  @Test
  public void createViewUsingNonExistingTable() throws NoSuchTableException {
    insertRows(10);

    sql("CREATE VIEW %s AS SELECT id FROM %s", viewName, tableName);

    List<Object[]> objects = sql("SELECT * FROM %s", viewName);
    List<Object[]> expected =
        IntStream.rangeClosed(1, 10).mapToObj(this::row).collect(Collectors.toList());

    assertThat(objects).hasSize(10).containsExactlyInAnyOrderElementsOf(expected);
  }

  private void insertRows(int numRows) throws NoSuchTableException {
    List<SimpleRecord> records = Lists.newArrayListWithCapacity(numRows);
    for (int i = 1; i <= numRows; i++) {
      int val = 'a';
      val += i;
      records.add(new SimpleRecord(i, Character.toString((char) val)));
    }

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
  }
}
