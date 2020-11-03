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

package org.apache.iceberg.hive;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;


public class TestHiveCommits extends HiveTableBaseTest {

  @Test
  public void testSuppressUnlockExceptions() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema()
            .addColumn("n", Types.IntegerType.get())
            .commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    ArgumentCaptor<Long> lockId = ArgumentCaptor.forClass(Long.class);
    doThrow(new RuntimeException()).when(spyOps).doUnlock(lockId.capture());

    try {
      spyOps.commit(metadataV2, metadataV1);
    } finally {
      ops.doUnlock(lockId.getValue());
    }

    ops.refresh();

    // the commit must succeed
    Assert.assertEquals(1, ops.current().schema().columns().size());
  }

  @Test
  public void testIgnoreCommitExceptions() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema()
            .addColumn("n", Types.IntegerType.get())
            .commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    ArgumentCaptor<Long> lockId = ArgumentCaptor.forClass(Long.class);
    doThrow(new RuntimeException()).when(spyOps).doUnlock(lockId.capture());

    ArgumentCaptor<org.apache.hadoop.hive.metastore.api.Table> tableArgumentCaptor =
            ArgumentCaptor.forClass(org.apache.hadoop.hive.metastore.api.Table.class);
    ArgumentCaptor<Boolean> booleanArgumentCaptor = ArgumentCaptor.forClass(Boolean.class);
    // throw  TException when persistTable
    doThrow(new TException("test")).when(spyOps).persistTable(tableArgumentCaptor.capture(),
            booleanArgumentCaptor.capture());

    ArgumentCaptor<String> pendingLocationArgumentCaptor =
            ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> hiveLocationArgumentCaptor = ArgumentCaptor.forClass(String.class);
    // but the location is same with hive
    doReturn(true).when(spyOps).checkLocationSameWithHive(pendingLocationArgumentCaptor.capture(),
            hiveLocationArgumentCaptor.capture());

    try {
      spyOps.commit(metadataV2, metadataV1);
      org.apache.hadoop.hive.metastore.api.Table hiveTable = tableArgumentCaptor.getValue();
      ops.persistTable(hiveTable, true);
    } finally {
      ops.doUnlock(lockId.getValue());
    }
    ops.refresh();
    // the commit must succeed
    Assert.assertEquals(1, ops.current().schema().columns().size());
  }
}
