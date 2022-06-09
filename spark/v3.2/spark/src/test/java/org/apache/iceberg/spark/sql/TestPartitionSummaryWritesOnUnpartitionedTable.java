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

import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestPartitionSummaryWritesOnUnpartitionedTable extends SparkCatalogTestBase {
  public TestPartitionSummaryWritesOnUnpartitionedTable(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTables() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg TBLPROPERTIES('write.summary.partition-limit'=100)",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testUnpartitionedTableDoesNotGeneratePartitionSummaries() {
    assertEquals("Rows in partition table must match", ImmutableList.of(), sql("SELECT * FROM %s", tableName + ".partitions"));
    Assert.assertEquals("Should have no partitions", 0L, scalarSql("SELECT count(*) FROM %s", tableName + ".partitions"));

    Table table = validationCatalog.loadTable(tableIdent);

    table.refresh();

    Snapshot currentSnapshot = table.currentSnapshot();

    Assert.assertEquals("Snapshot summary for unpartitioned table should have changed partition count of zero",
        currentSnapshot.summary().get(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP), "0");
    // TODO - For backwards compatibility we might want to make this false.
    Assert.assertNull("Snapshot summary for unpartitioned tables shouldn't have the partition summary included field",
        currentSnapshot.summary().get(SnapshotSummary.PARTITION_SUMMARY_PROP));
  }
}
