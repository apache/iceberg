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

import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestExpireSnapshotProcedure extends SparkExtensionsTestBase {
  public TestExpireSnapshotProcedure(
      String catalogName,
      String implementation,
      Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testExpireSnapshotUsingPostionArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long firstSnapshotTimestamp = System.currentTimeMillis();
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    table.refresh();
    Assert.assertEquals(2, Lists.newArrayList(table.snapshots()).size());
    Snapshot secondSnapshot = table.currentSnapshot();

    // without the retained last num arg
    List<Object[]> output = sql(
        "CALL %s.system.expire_snapshot('%s', '%s', TIMESTAMP '%s')",
        catalogName, tableIdent.namespace(), tableIdent.name(), new Timestamp(firstSnapshotTimestamp).toString());
    table.refresh();
    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(null, firstSnapshotTimestamp)),
        output);
    Assert.assertEquals(1, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals(secondSnapshot.snapshotId(), Lists.newArrayList(table.snapshots()).get(0).snapshotId());

    //with the retained last num arg
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    table.refresh();
    Snapshot tailSnapshot = table.currentSnapshot();
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    table.refresh();
    Assert.assertEquals(4, Lists.newArrayList(table.snapshots()).size());
    Snapshot headSnapshot = table.currentSnapshot();
    long currentTimeStamp = System.currentTimeMillis();
    int retainedLastNum = 3;
    output = sql(
        "CALL %s.system.expire_snapshot('%s', '%s', TIMESTAMP '%s', %d)",
        catalogName,
        tableIdent.namespace(),
        tableIdent.name(),
        new Timestamp(currentTimeStamp).toString(),
        retainedLastNum);
    table.refresh();
    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(retainedLastNum, currentTimeStamp)),
        output);
    Assert.assertEquals(3, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals(tailSnapshot.snapshotId(), Lists.newArrayList(table.snapshots()).get(0).snapshotId());
    Assert.assertEquals(headSnapshot.snapshotId(), Lists.newArrayList(table.snapshots()).get(2).snapshotId());
  }

  @Test
  public void testExpireSnapshotUsingNameArgs() {

  }

  @Test
  public void testExpireSnapshotWithInvalidRetainLastNum() {

  }
}
