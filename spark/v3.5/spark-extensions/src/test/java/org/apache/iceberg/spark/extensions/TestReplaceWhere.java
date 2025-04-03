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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestReplaceWhere extends ExtensionsTestBase {

  @AfterEach
  public void cleanup() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testCaseSensitivity() {

    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg PARTITIONED BY (dep)", tableName);
    sql("INSERT INTO %s (id, dep) VALUES (1, 'hr')", tableName);
    sql("INSERT INTO %s (id, dep) VALUES (2, 'support')", tableName);

    sql("SET spark.sql.caseSensitive = false");
    sql("INSERT INTO %s REPLACE WHERE DEP = 'hr' VALUES (3, 'hr')", tableName);
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(2, "support"), row(3, "hr") // insert
            );
    assertEquals(
        "Should have expected rows", sql("SELECT * FROM %s ORDER BY id", tableName), expectedRows);

    sql("SET spark.sql.caseSensitive = true");
    assertThatThrownBy(
            () -> sql("INSERT INTO %s REPLACE WHERE DEP = 'hr' VALUES (4, 'hr')", tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("A column or function parameter with name `DEP` cannot be resolved");
    sql("INSERT INTO %s REPLACE WHERE dep = 'hr' VALUES (4, 'hr')", tableName);

    expectedRows =
        ImmutableList.of(
            row(2, "support"), row(4, "hr") // insert
            );
    assertEquals(
        "Should have expected rows", sql("SELECT * FROM %s ORDER BY id", tableName), expectedRows);
  }
}
