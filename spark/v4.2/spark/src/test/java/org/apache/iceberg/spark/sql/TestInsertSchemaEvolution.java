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
import java.util.Map;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkTableProperties;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
class TestInsertSchemaEvolution extends CatalogTestBase {

  @Parameter(index = 3)
  private boolean byName;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, byName = {3}")
  protected static Object[][] parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (Object[] catalogParams : CatalogTestBase.parameters()) {
      for (boolean byName : new boolean[] {false, true}) {
        parameters.add(new Object[] {catalogParams[0], catalogParams[1], catalogParams[2], byName});
      }
    }

    return parameters.toArray(new Object[0][]);
  }

  @AfterEach
  void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void addColumnWhileCastingSmallintToInt() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg TBLPROPERTIES ('%s' = 'false')",
        tableName, SparkTableProperties.WRITE_ACCEPT_ANY_SCHEMA);
    sql("INSERT INTO %s VALUES (1, 'old')", tableName);

    sql(
        "INSERT WITH SCHEMA EVOLUTION INTO %s %s SELECT %s "
            + "FROM VALUES (CAST(2 AS SMALLINT), 'new', 'added') AS source(id, data, extra)",
        tableName, byName ? "BY NAME" : "", byName ? "extra, data, id" : "id, data, extra");

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "extra", Types.StringType.get()));
    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .isEqualTo(expectedSchema.asStruct());
    assertEquals(
        "Should retain INT values and add a nullable column",
        Lists.newArrayList(row(1, "old", null), row(2, "new", "added")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  void widenIntToBigint() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg TBLPROPERTIES ('%s' = 'false')",
        tableName, SparkTableProperties.WRITE_ACCEPT_ANY_SCHEMA);
    sql("INSERT INTO %s VALUES (1, 'old')", tableName);

    long newId = (long) Integer.MAX_VALUE + 1;
    sql(
        "INSERT WITH SCHEMA EVOLUTION INTO %s %s SELECT %s "
            + "FROM VALUES (CAST(%d AS BIGINT), 'new') AS source(id, data)",
        tableName, byName ? "BY NAME" : "", byName ? "data, id" : "id, data", newId);

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .isEqualTo(expectedSchema.asStruct());
    assertEquals(
        "Should preserve old and new values after widening to BIGINT",
        Lists.newArrayList(row(1L, "old"), row(newId, "new")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  void castBigintMapKeysToInt() {
    sql(
        "CREATE TABLE %s (id INT, data MAP<INT, STRING>) USING iceberg "
            + "TBLPROPERTIES ('%s' = 'false')",
        tableName, SparkTableProperties.WRITE_ACCEPT_ANY_SCHEMA);
    sql("INSERT INTO %s SELECT 1, map(1, 'old')", tableName);

    sql(
        "INSERT WITH SCHEMA EVOLUTION INTO %s %s SELECT %s "
            + "FROM VALUES (2, map(CAST(2 AS BIGINT), 'new', CAST(3 AS BIGINT), 'other')) "
            + "AS source(id, data)",
        tableName, byName ? "BY NAME" : "", byName ? "data, id" : "id, data");

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(
                2,
                "data",
                Types.MapType.ofOptional(3, 4, Types.IntegerType.get(), Types.StringType.get())));
    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .isEqualTo(expectedSchema.asStruct());
    Map<Integer, String> oldMap = Maps.newHashMap();
    oldMap.put(1, "old");
    Map<Integer, String> newMap = Maps.newHashMap();
    newMap.put(2, "new");
    newMap.put(3, "other");
    assertEquals(
        "Should preserve map values and retain INT keys",
        Lists.newArrayList(row(1, oldMap), row(2, newMap)),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  void evolveSchemaWithAcceptAnySchemaAndMergeSchema() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg TBLPROPERTIES ('%s' = 'true')",
        tableName, SparkTableProperties.WRITE_ACCEPT_ANY_SCHEMA);
    sql("INSERT INTO %s SELECT 1 AS id, 'old' AS data", tableName);

    long newId = (long) Integer.MAX_VALUE + 1;
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SparkSQLProperties.MERGE_SCHEMA, "true");
    // accept-any-schema skips Spark's by-name column alignment and Iceberg checks the column order
    // by default, so the source columns stay in table order for both insert modes
    withSQLConf(
        conf,
        () ->
            sql(
                "INSERT WITH SCHEMA EVOLUTION INTO %s %s SELECT id, data, extra "
                    + "FROM VALUES (CAST(%d AS BIGINT), 'new', 'added') AS source(id, data, extra)",
                tableName, byName ? "BY NAME" : "", newId));

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "extra", Types.StringType.get()));
    assertThat(validationCatalog.loadTable(tableIdent).schema().asStruct())
        .isEqualTo(expectedSchema.asStruct());
    assertEquals(
        "Should widen and add columns with native and legacy evolution enabled",
        Lists.newArrayList(row(1L, "old", null), row(newId, "new", "added")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }
}
