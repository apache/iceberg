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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestCopyOnWriteDelete extends TestDelete {

  public TestCopyOnWriteDelete(String catalogName, String implementation, Map<String, String> config,
                               String fileFormat, Boolean vectorized, String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(TableProperties.DELETE_MODE, "copy-on-write",
        TableProperties.DELETE_DISTRIBUTION_MODE, "range",
        TableProperties.FILE_AS_SPLIT, "true");
  }

  @Test
  public void testDeleteWithRangePartitioningOptimization() throws NoSuchTableException {
    Assume.assumeTrue(fileFormat.equalsIgnoreCase("parquet"));

    createAndInitPartitionedTable();
    sql("ALTER TABLE %s WRITE ORDERED BY %s", tableName, "id");

    append(
        new Employee(1, "hr"),
        new Employee(3, "hr"),
        new Employee(4, "hr"),
        new Employee(5, "hr"),
        new Employee(6, "hr"),
        new Employee(7, "hr"),
        new Employee(8, "hr"),
        new Employee(9, "hr"));
    append(
        new Employee(1, "hardware"),
        new Employee(2, "hardware"),
        new Employee(3, "hardware"),
        new Employee(4, "hardware"),
        new Employee(5, "hardware"),
        new Employee(6, "hardware"),
        new Employee(7, "hardware"),
        new Employee(8, "hardware"),
        new Employee(9, "hardware"),
        new Employee(10, "hardware"));

    List<Object[]> sortOrders = sql("SELECT sort_order_id from %s.files", tableName);
    Assert.assertTrue(sortOrders.stream().allMatch(x -> (int) x[x.length - 1] == 1));

    sql("DELETE FROM %s WHERE id = 2", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = table.currentSnapshot();
    validateCopyOnWrite(currentSnapshot, "1", "1", "1");

    assertEquals("Should have expected rows",
        ImmutableList.of(
            row(1, "hardware"),
            row(1, "hr"),
            row(3, "hardware"),
            row(3, "hr"),
            row(4, "hardware"),
            row(4, "hr"),
            row(5, "hardware"),
            row(5, "hr"),
            row(6, "hardware"),
            row(6, "hr"),
            row(7, "hardware"),
            row(7, "hr"),
            row(8, "hardware"),
            row(8, "hr"),
            row(9, "hardware"),
            row(9, "hr"),
            row(10, "hardware")),
        sql("SELECT * FROM %s ORDER BY id, dep", tableName));
  }
}
