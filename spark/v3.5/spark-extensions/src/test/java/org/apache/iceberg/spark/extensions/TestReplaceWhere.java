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

import java.util.Map;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestReplaceWhere extends SparkRowLevelOperationsTestBase {

  @Override
  protected Map<String, String> extraTableProperties() {
    return Map.of();
  }

  @AfterEach
  public void cleanup() {
    sql("drop table if exists %s", tableName);
  }

  @TestTemplate
  public void testReplaceWhereCaseSenstive() {
    createAndInitTable(
        "id INT, dep STRING",
        "PARTITIONED BY (dep)",
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\" }\n"
            + "{ \"id\": 3, \"dep\": \"support\" }");

    sql("SET spark.sql.caseSensitive = false");
    sql("INSERT INTO %s REPLACE WHERE  DEP = 'hr' VALUES (2, 'hr')", tableName);

    sql("SET spark.sql.caseSensitive = true");
    String errorMsg = "A column or function parameter with name `DEP` cannot be resolved";
    assertThatThrownBy(
            () -> sql("INSERT INTO %s REPLACE WHERE DEP = 'hr' VALUES (2, 'hr')", tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(errorMsg);
    sql("INSERT INTO %s REPLACE WHERE dep = 'hr' VALUES (2, 'hr')", tableName);
  }
}
