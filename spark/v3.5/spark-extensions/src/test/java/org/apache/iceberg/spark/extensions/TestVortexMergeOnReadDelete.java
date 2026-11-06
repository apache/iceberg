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

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

import org.apache.iceberg.Parameters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * End-to-end merge-on-read DELETE coverage for Vortex tables. A DELETE reads the data with the
 * synthetic {@code _pos} column (wired through Vortex's {@code row_idx} expression) to compute the
 * positions to delete, writes a position-delete file (a Vortex delete file for v2, a deletion
 * vector for v3) through the format-model registry, and the subsequent read excludes the deleted
 * rows via native scan pushdown.
 */
public class TestVortexMergeOnReadDelete extends ExtensionsTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      }
    };
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  private void runMergeOnReadDelete(int formatVersion) {
    sql(
        "CREATE TABLE %s (id INT, dep STRING) USING iceberg "
            + "TBLPROPERTIES ('%s'='vortex', '%s'='%d', '%s'='merge-on-read')",
        tableName, DEFAULT_FILE_FORMAT, FORMAT_VERSION, formatVersion, DELETE_MODE);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", tableName);

    sql("DELETE FROM %s WHERE id IN (2, 4)", tableName);

    assertEquals(
        "Merge-on-read DELETE should exclude exactly the deleted rows",
        ImmutableList.of(row(1, "a"), row(3, "c"), row(5, "e")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // A second DELETE produces another set of positions against the same data file.
    sql("DELETE FROM %s WHERE id = 5", tableName);

    assertEquals(
        "Subsequent merge-on-read DELETE should remove additional rows",
        ImmutableList.of(row(1, "a"), row(3, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testMergeOnReadDeleteFormatV2() {
    runMergeOnReadDelete(2);
  }

  @TestTemplate
  public void testMergeOnReadDeleteFormatV3() {
    runMergeOnReadDelete(3);
  }
}
