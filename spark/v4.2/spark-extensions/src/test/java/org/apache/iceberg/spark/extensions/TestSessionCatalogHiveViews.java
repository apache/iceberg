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

import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSessionCatalogHiveViews extends ExtensionsTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK_SESSION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION.implementation(),
        SparkCatalogConfig.SPARK_SESSION.properties()
      }
    };
  }

  @AfterEach
  public void dropViewsAndTable() {
    sql("DROP VIEW IF EXISTS %s.default.aliased_values", catalogName);
    sql("DROP VIEW IF EXISTS %s.default.evolving_values", catalogName);
    sql("DROP VIEW IF EXISTS %s.default.aliased_table", catalogName);
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void readViewWithColumnAliases() {
    sql("CREATE VIEW %s.default.aliased_values (c1, c2) AS VALUES (1, 2)", catalogName);

    assertThat(sql("SELECT * FROM %s.default.aliased_values", catalogName))
        .containsExactly(row(1, 2));
  }

  @TestTemplate
  public void readViewWithSchemaEvolution() {
    sql(
        "CREATE VIEW %s.default.evolving_values (c1, c2) WITH SCHEMA EVOLUTION AS VALUES (1, 2)",
        catalogName);

    assertThat(sql("SELECT c1, c2 FROM %s.default.evolving_values", catalogName))
        .containsExactly(row(1, 2));
  }

  @TestTemplate
  public void readViewWithColumnAliasesOverTable() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    sql(
        "CREATE VIEW %s.default.aliased_table (new_id COMMENT 'the id', new_data) "
            + "AS SELECT id, data FROM %s",
        catalogName, tableName);

    assertThat(sql("SELECT * FROM %s.default.aliased_table ORDER BY new_id", catalogName))
        .containsExactly(row(1, "a"), row(2, "b"));
    assertThat(spark.table(catalogName + ".default.aliased_table").schema().fieldNames())
        .containsExactly("new_id", "new_data");
    assertThat(
            spark
                .table(catalogName + ".default.aliased_table")
                .schema()
                .apply("new_id")
                .getComment()
                .get())
        .isEqualTo("the id");
  }
}
