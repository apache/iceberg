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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestCaseSensitivity extends ExtensionsTestBase {

  @AfterEach
  public void cleanup() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testReplaceWhere() {

    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg PARTITIONED BY (dep)", tableName);
    sql("INSERT INTO %s (id, dep) VALUES (1, 'hr')", tableName);
    sql("INSERT INTO %s (id, dep) VALUES (5, 'support')", tableName);

    sql("SET spark.sql.caseSensitive = false");
    sql("INSERT INTO %s REPLACE WHERE DEP = 'hr' VALUES (3, 'hr')", tableName);
    assertThat(sql("SELECT ID, DEP FROM %s ORDER BY ID", tableName))
        .containsExactly(row(3, "hr"), row(5, "support"));

    sql("SET spark.sql.caseSensitive = true");
    String errorMsg = "A column or function parameter with name `DEP` cannot be resolved";
    assertThatThrownBy(
            () -> sql("INSERT INTO %s REPLACE WHERE DEP = 'hr' VALUES (4, 'hr')", tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(errorMsg);
    assertThatThrownBy(() -> sql("SELECT DEP FROM %s ORDER BY ID", tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(errorMsg);

    sql("INSERT INTO %s REPLACE WHERE dep = 'hr' VALUES (4, 'hr')", tableName);
    assertThat(sql("SELECT id, dep FROM %s ORDER BY id", tableName))
        .containsExactly(row(4, "hr"), row(5, "support"));
  }

  @TestTemplate
  public void testDeleteWhere() {
    // test DeleteFrom
    sql(
        "CREATE TABLE %s (id INT, dep STRING) USING iceberg  TBLPROPERTIES('format-version'='1')",
        tableName);
    sql("INSERT INTO %s (id, dep) VALUES (1, 'hr')", tableName);
    sql("INSERT INTO %s (id, dep) VALUES (5, 'support')", tableName);
    sql("SET spark.sql.caseSensitive = true");
    String errorMsg = "A column or function parameter with name `DEP` cannot be resolved";
    assertThatThrownBy(() -> sql("DELETE FROM %s where DEP = 'support'", tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(errorMsg);

    sql("SET spark.sql.caseSensitive = false");
    sql("DELETE FROM %s where DEP = 'support'", tableName);
    assertThat(sql("SELECT id, dep FROM %s ORDER BY id", tableName)).containsExactly(row(1, "hr"));
  }

  @TestTemplate
  public void testUpdateByCopyOnWrite() {
    // Test CopyOnWrite Commit
    sql(
        "CREATE TABLE %s (id INT, dep STRING) USING iceberg PARTITIONED BY (dep) TBLPROPERTIES('write.update.mode'='copy-on-write')",
        tableName);
    sql("INSERT INTO %s (id, dep) VALUES (1, 'hr')", tableName);
    sql("INSERT INTO %s (id, dep) VALUES (5, 'support')", tableName);
    sql("SET spark.sql.caseSensitive = true");
    String errorMsg = "A column or function parameter with name `DEP` cannot be resolved";
    assertThatThrownBy(() -> sql("UPDATE %s SET DEP = 'it' WHERE id = 1", tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(errorMsg);

    sql("SET spark.sql.caseSensitive = false");
    sql("UPDATE %s SET DEP = 'it' where id = 1", tableName);
    assertThat(sql("SELECT id, dep FROM %s ORDER BY id", tableName))
        .containsExactly(row(1, "it"), row(5, "support"));
  }

  @TestTemplate
  public void testInsertOverwrite() {
    // Test DynamicOverwrite Commit
    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg PARTITIONED BY (dep)", tableName);
    sql("INSERT INTO %s (id, dep) VALUES (1, 'hr')", tableName);
    sql("INSERT INTO %s (id, dep) VALUES (5, 'support')", tableName);
    sql("SET spark.sql.caseSensitive = true");
    String errorMsg = "A column or function parameter with name `DEP` cannot be resolved";
    assertThatThrownBy(() -> sql("INSERT OVERWRITE %s(id, DEP) VALUES (2, 'hr')", tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(errorMsg);

    sql("SET spark.sql.caseSensitive = false");
    sql("INSERT OVERWRITE %s(id, DEP) VALUES (2, 'hr')", tableName);
    assertThat(sql("SELECT id, dep FROM %s ORDER BY id", tableName))
        .containsExactly(row(2, "hr"), row(5, "support"));
  }
}
