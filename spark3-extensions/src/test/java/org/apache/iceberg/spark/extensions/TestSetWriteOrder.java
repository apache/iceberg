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
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.bucket;

public class TestSetWriteOrder extends SparkExtensionsTestBase {
  public TestSetWriteOrder(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSetWriteOrderByColumn() {
    sql("CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE ORDERED BY category, id", tableName);

    table.refresh();

    SortOrder expected = SortOrder.builderFor(table.schema())
        .withOrderId(1)
        .asc("category", NullOrder.NULLS_FIRST)
        .asc("id", NullOrder.NULLS_FIRST)
        .build();

    Assert.assertEquals("Should have expected order", expected, table.sortOrder());
  }

  @Test
  public void testSetWriteOrderByColumnWithDirection() {
    sql("CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE ORDERED BY category ASC, id DESC", tableName);

    table.refresh();

    SortOrder expected = SortOrder.builderFor(table.schema())
        .withOrderId(1)
        .asc("category", NullOrder.NULLS_FIRST)
        .desc("id", NullOrder.NULLS_LAST)
        .build();

    Assert.assertEquals("Should have expected order", expected, table.sortOrder());
  }

  @Test
  public void testSetWriteOrderByColumnWithDirectionAndNullOrder() {
    sql("CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST", tableName);

    table.refresh();

    SortOrder expected = SortOrder.builderFor(table.schema())
        .withOrderId(1)
        .asc("category", NullOrder.NULLS_LAST)
        .desc("id", NullOrder.NULLS_FIRST)
        .build();

    Assert.assertEquals("Should have expected order", expected, table.sortOrder());
  }

  @Test
  public void testSetWriteOrderByTransform() {
    sql("CREATE TABLE %s (id bigint NOT NULL, category string, ts timestamp, data string) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start unsorted", table.sortOrder().isUnsorted());

    sql("ALTER TABLE %s WRITE ORDERED BY category DESC, bucket(16, id), id", tableName);

    table.refresh();

    SortOrder expected = SortOrder.builderFor(table.schema())
        .withOrderId(1)
        .desc("category")
        .asc(bucket("id", 16))
        .asc("id")
        .build();

    Assert.assertEquals("Should have expected order", expected, table.sortOrder());
  }
}
