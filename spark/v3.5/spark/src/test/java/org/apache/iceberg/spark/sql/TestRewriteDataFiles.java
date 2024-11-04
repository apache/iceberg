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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.CatalogTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestRewriteDataFiles extends CatalogTestBase {

  @BeforeEach
  public void createTables() {
    sql("CREATE TABLE %s (key int, value int) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1,1)", tableName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testRewriteDataFilesFailsByDefaultSetting() {
    assertThatThrownBy(
            () ->
                sql(
                    "call %s.system.rewrite_data_files(table=>'%s', where=>'VALUE > 0')",
                    catalogName, tableName))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot find field 'VALUE' in struct: struct<1: key, 2: value>");
  }

  @TestTemplate
  public void testRewriteDataFilesSucceedByCaseInsensitive() {
    sql("set spark.sql.caseSensitive=false");
    assertEquals(
        "Should have 0 files to rewrite, 0 files to add and 0 bytes to rewrite, since only 1 file is present",
        ImmutableList.of(row(0, 0, 0)),
        sql(
            "call %s.system.rewrite_data_files(table=>'%s', where=>'VALUE > 0')",
            catalogName, tableName));
  }
}
