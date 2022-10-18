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

import java.util.Map;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class TestCopyOnWriteMerge extends TestMerge {

  public TestCopyOnWriteMerge(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      boolean vectorized,
      String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
  }

  @Test
  public void testMergeWithTableWithNonNullableColumn() {
    createAndInitTable(
        "id INT NOT NULL, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT NOT NULL, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET * "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 2 THEN "
            + "  INSERT *",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // updated
            row(2, "emp-id-2") // new
            );
    assertEquals(
        "Should have expected rows", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }
}
