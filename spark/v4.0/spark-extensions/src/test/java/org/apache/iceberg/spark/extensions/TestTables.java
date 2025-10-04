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

import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTables extends ExtensionsTestBase {

  @Parameter(index = 3)
  private String sourceName;

  @BeforeEach
  public void createTableIfNotExists() {
    sql(
            "CREATE TABLE IF NOT EXISTS %s (id bigint NOT NULL, data string) "
                    + "USING iceberg PARTITIONED BY (truncate(id, 3))",
            sourceName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", sourceName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }


  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, sourceName = {3}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
        SparkCatalogConfig.HIVE.catalogName() + ".default.source"
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        SparkCatalogConfig.HADOOP.catalogName() + ".default.source"
      },
      {
        SparkCatalogConfig.SPARK_SESSION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION.implementation(),
        SparkCatalogConfig.SPARK_SESSION.properties(),
        "default.source"
      }
    };
  }

  @TestTemplate
  public void testCreateTableLike() {
    sql("CREATE TABLE %s LIKE %s", tableName, sourceName);

    Table targetTable = validationCatalog.loadTable(tableIdent);

    Schema expectedSchema =
            new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "data", Types.StringType.get()));

    assertThat(targetTable.schema().asStruct())
            .as("Should have expected schema")
            .isEqualTo(expectedSchema.asStruct());

    assertThat(sql("SELECT * FROM %s", tableName)).isEmpty();
  }
}