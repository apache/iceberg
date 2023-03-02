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
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestAlterSortOrder extends SparkExtensionsTestBase {
  public TestAlterSortOrder(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testDeleteColumnFromOldSortOrder() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE ORDERED BY ts", tableName);

    table.refresh();

    Assert.assertEquals("Should have two sort orders", 2, table.sortOrders().size());
    Assert.assertEquals(
        "Expected identity sort order on ts",
        "[\n" + "  identity(3) ASC NULLS FIRST\n" + "]",
        table.sortOrders().get(1).toString());

    sql("ALTER TABLE %s WRITE ORDERED BY id", tableName);

    table.refresh();

    Assert.assertEquals("Should have three sort orders", 3, table.sortOrders().size());
    Assert.assertEquals(
        "Expected identity sort order on id",
        "[\n" + "  identity(1) ASC NULLS FIRST\n" + "]",
        table.sortOrders().get(2).toString());

    sql("ALTER TABLE %s DROP COLUMN ts", tableName);

    table.refresh();
  }
}
