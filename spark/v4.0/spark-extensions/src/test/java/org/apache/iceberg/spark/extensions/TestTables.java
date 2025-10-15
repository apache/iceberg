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
import org.apache.iceberg.PartitionSpec;
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
  private String srcTableName;

  @BeforeEach
  public void beforeEach() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) "
            + "USING iceberg PARTITIONED BY (truncate(id, 3))",
            srcTableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", srcTableName);
  }

  @AfterEach
  public void afterEach() {
    sql("DROP TABLE IF EXISTS %s", srcTableName);
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, srcTableName = {3}")
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
  public void testNotPartitionedTable() {
    sql("CREATE OR REPLACE TABLE %s (id bigint NOT NULL, data string) USING iceberg", srcTableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", srcTableName);
    sql("CREATE TABLE %s LIKE %s", tableName, srcTableName);

    Schema expectedSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.schema().asStruct())
        .as("Should have expected nullable schema")
        .isEqualTo(expectedSchema.asStruct());
    assertThat(table.spec().fields()).as("Should be an unpartitioned table").isEmpty();
  }

  @TestTemplate
  public void testPartitionedTable() {
    sql("CREATE TABLE %s LIKE %s", tableName, srcTableName);

    Schema expectedSchema =
            new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "data", Types.StringType.get()));

    PartitionSpec expectedSpec = PartitionSpec.builderFor(expectedSchema).truncate("id", 3).build();

    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.schema().asStruct())
            .as("Should have expected nullable schema")
            .isEqualTo(expectedSchema.asStruct());
    assertThat(table.spec()).as("Should be partitioned by id").isEqualTo(expectedSpec);
  }

}
