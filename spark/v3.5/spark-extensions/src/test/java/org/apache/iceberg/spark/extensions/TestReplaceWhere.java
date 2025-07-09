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

import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.MERGE_MODE_DEFAULT;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;

public abstract class TestReplaceWhere extends SparkRowLevelOperationsTestBase {
  @BeforeAll
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
  }

  @TestTemplate
  public void testInsertFromTable() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-two\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }\n"
            + "{ \"id\": 4, \"dep\": \"emp-id-4\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 5, \"dep\": \"emp-id-5\" }");

    // Only 1 data file
    assertThat(sql("SELECT content FROM %s.files", tableName)).containsExactly(row(0));
    sql("INSERT INTO %s REPLACE WHERE id <= 2 SELECT * FROM source", tableName);

    assertThat(sql("SELECT dep FROM %s ORDER BY id", tableName))
        .containsExactly(
            row("emp-id-1"), row("emp-id-2"), row("emp-id-3"), row("emp-id-4"), row("emp-id-5"));

    Table table = validationCatalog.loadTable(tableIdent);
    if (mode(table) == RowLevelOperationMode.COPY_ON_WRITE) {
      assertThat(sql("SELECT count(*) FROM %s.position_deletes", tableName))
          .containsExactly(row(0L));
    } else {
      assertThat(sql("SELECT count(*) FROM %s.position_deletes", tableName))
          .containsExactly(row(2L));
    }
  }

  @TestTemplate
  public void testInsertFromTableWithDifferentCol() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-two\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }\n"
            + "{ \"id\": 4, \"dep\": \"emp-id-4\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING, location STRING    ",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" , \"location\":\"SH\"}\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" ,\"location\":\"SH\"}\n"
            + "{ \"id\": 5, \"dep\": \"emp-id-5\" ,\"location\":\"SH\"}");

    // Only 1 data file
    assertThat(sql("SELECT content FROM %s.files", tableName)).containsExactly(row(0));
    sql("INSERT INTO %s REPLACE WHERE id <= 2 SELECT id,dep FROM source", tableName);

    assertThat(sql("SELECT dep FROM %s ORDER BY id", tableName))
        .containsExactly(
            row("emp-id-1"), row("emp-id-2"), row("emp-id-3"), row("emp-id-4"), row("emp-id-5"));

    Table table = validationCatalog.loadTable(tableIdent);
    if (mode(table) == RowLevelOperationMode.COPY_ON_WRITE) {
      assertThat(sql("SELECT count(*) FROM %s.position_deletes", tableName))
          .containsExactly(row(0L));
    } else {
      assertThat(sql("SELECT count(*) FROM %s.position_deletes", tableName))
          .containsExactly(row(2L));
    }
  }

  @TestTemplate
  public void testInsertFromValues() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-two\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }\n"
            + "{ \"id\": 4, \"dep\": \"emp-id-4\" }");

    // Only 1 data file
    assertThat(sql("SELECT content FROM %s.files", tableName)).containsExactly(row(0));
    sql(
        "INSERT INTO %s REPLACE WHERE id <= 2 VALUES(1, 'emp-id-1'),(2, 'emp-id-2'),(5, 'emp-id-5')",
        tableName);

    assertThat(sql("SELECT dep FROM %s ORDER BY id", tableName))
        .containsExactly(
            row("emp-id-1"), row("emp-id-2"), row("emp-id-3"), row("emp-id-4"), row("emp-id-5"));

    Table table = validationCatalog.loadTable(tableIdent);
    if (mode(table) == RowLevelOperationMode.COPY_ON_WRITE) {
      assertThat(sql("SELECT count(*) FROM %s.position_deletes", tableName))
          .containsExactly(row(0L));
    } else {
      assertThat(sql("SELECT count(*) FROM %s.position_deletes", tableName))
          .containsExactly(row(2L));
    }
  }

  @TestTemplate
  public void testInsertOverwrite() {
    // Insert overwrite should use OverWriteByExpressionExec, same as before.
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-two\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }\n"
            + "{ \"id\": 4, \"dep\": \"emp-id-4\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING, location STRING    ",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" , \"location\":\"SH\"}\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" ,\"location\":\"SH\"}\n"
            + "{ \"id\": 5, \"dep\": \"emp-id-5\" ,\"location\":\"SH\"}");

    // Only 1 data file
    assertThat(sql("SELECT content FROM %s.files", tableName)).containsExactly(row(0));
    sql("INSERT OVERWRITE %s  SELECT id,dep FROM source", tableName);

    // insert successfullly
    assertThat(sql("SELECT dep FROM %s ORDER BY id", tableName))
        .containsExactly(row("emp-id-1"), row("emp-id-2"), row("emp-id-5"));

    assertThat(sql("SELECT count(*) FROM %s.position_deletes", tableName)).containsExactly(row(0L));
  }

  @TestTemplate
  public void testFallBackToOverWriteByExpression() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-two\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }\n"
            + "{ \"id\": 4, \"dep\": \"emp-id-4\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING, location STRING    ",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" , \"location\":\"SH\"}\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" ,\"location\":\"SH\"}\n"
            + "{ \"id\": 5, \"dep\": \"emp-id-5\" ,\"location\":\"SH\"}");

    // Only 1 data file
    assertThat(sql("SELECT content FROM %s.files", tableName)).containsExactly(row(0));
    sql("INSERT INTO %s REPLACE WHERE id <= 4 SELECT id,dep FROM source", tableName);

    assertThat(sql("SELECT dep FROM %s ORDER BY id", tableName))
        .containsExactly(row("emp-id-1"), row("emp-id-2"), row("emp-id-5"));

    assertThat(sql("SELECT count(*) FROM %s.position_deletes", tableName)).containsExactly(row(0L));
  }

  @TestTemplate
  public void testCaseSentitive() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-two\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }\n"
            + "{ \"id\": 4, \"dep\": \"emp-id-4\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING, location STRING    ",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" , \"location\":\"SH\"}\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" ,\"location\":\"SH\"}\n"
            + "{ \"id\": 5, \"dep\": \"emp-id-5\" ,\"location\":\"SH\"}");

    // Only 1 data file
    assertThat(sql("SELECT content FROM %s.files", tableName)).containsExactly(row(0));
    sql("INSERT INTO %s REPLACE WHERE ID <= 4 SELECT ID,DEP FROM source", tableName);

    assertThat(sql("SELECT dep FROM %s ORDER BY id", tableName))
        .containsExactly(row("emp-id-1"), row("emp-id-2"), row("emp-id-5"));
  }

  private RowLevelOperationMode mode(Table table) {
    String modeName = table.properties().getOrDefault(MERGE_MODE, MERGE_MODE_DEFAULT);
    return RowLevelOperationMode.fromName(modeName);
  }
}
