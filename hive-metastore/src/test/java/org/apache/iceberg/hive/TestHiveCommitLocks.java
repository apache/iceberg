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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestHiveCommitLocks extends HiveTableBaseTest {
  HiveTableOperations ops = null;
  HiveTableOperations spyOps = null;
  HiveClientPool spyClientPool = null;
  HiveMetaStoreClient spyClient = null;
  TableMetadata metadataV1 = null;
  TableMetadata metadataV2 = null;

  long dummyLockId = 500L;
  LockResponse waitLockResponse = new LockResponse(dummyLockId, LockState.WAITING);
  LockResponse acquiredLockResponse = new LockResponse(dummyLockId, LockState.ACQUIRED);
  LockResponse notAcquiredLockResponse = new LockResponse(dummyLockId, LockState.NOT_ACQUIRED);

  @Before
  public void before() throws Exception {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    ops = (HiveTableOperations) ((HasTableOperations) table).operations();
    String dbName = TABLE_IDENTIFIER.namespace().level(0);
    String tableName = TABLE_IDENTIFIER.name();
    Configuration overriddenHiveConf = new Configuration(hiveConf);
    overriddenHiveConf.setLong("iceberg.hive.lock-timeout-ms", 10 * 1000);
    overriddenHiveConf.setLong("iceberg.hive.lock-check-min-wait-ms", 50);
    overriddenHiveConf.setLong("iceberg.hive.lock-check-max-wait-ms", 5 * 1000);
    overriddenHiveConf.setDouble("iceberg.hive.lock-check-backoff-scale-factor", 3.0);

    metadataV1 = ops.current();

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    ops.refresh();

    metadataV2 = ops.current();

    Assert.assertEquals(2, ops.current().schema().columns().size());

    spyClientPool = spy(new HiveClientPool(1, overriddenHiveConf));
    AtomicReference<HiveMetaStoreClient> spyClientRef = new AtomicReference<>();

    when(spyClientPool.newClient()).thenAnswer(invocation -> {
      HiveMetaStoreClient client = (HiveMetaStoreClient) invocation.callRealMethod();
      spyClientRef.set(spy(client));
      return spyClientRef.get();
    });

    spyOps = spy(new HiveTableOperations(overriddenHiveConf, spyClientPool, ops.io(), catalog.name(),
        dbName, tableName));
    spyClientPool.run(client -> client.isLocalMetaStore()); // To ensure new client is created.
    Assert.assertNotNull(spyClientRef.get());

    spyClient = spyClientRef.get();
  }

  @After
  public void cleanup() {
    try {
      spyClientPool.close();
    } catch (Throwable t) {
      // Ignore any exception
    }
  }

  @Test
  public void testLockAcquisitionAtFirstTime() throws TException, InterruptedException {
    doReturn(acquiredLockResponse).when(spyClient).lock(any());
    doNothing().when(spyOps).doUnlock(eq(dummyLockId));

    spyOps.doCommit(metadataV2, metadataV1);

    Assert.assertEquals(1, spyOps.current().schema().columns().size()); // should be 1 again
  }

  @Test
  public void testLockAcquisitionAfterRetries() throws TException, InterruptedException {

    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(acquiredLockResponse)
        .when(spyClient)
        .checkLock(eq(dummyLockId));
    doNothing().when(spyOps).doUnlock(eq(dummyLockId));

    spyOps.doCommit(metadataV2, metadataV1);

    Assert.assertEquals(1, spyOps.current().schema().columns().size()); // should be 1 again
  }

  @Test
  public void testLockFailureAtFirstTime() throws TException {
    doReturn(notAcquiredLockResponse).when(spyClient).lock(any());

    AssertHelpers.assertThrows("Expected an exception",
        CommitFailedException.class,
        "Could not acquire the lock on",
        () -> spyOps.doCommit(metadataV2, metadataV1));
  }

  @Test
  public void testLockFailureAfterRetries() throws TException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(waitLockResponse)
        .doReturn(notAcquiredLockResponse)
        .when(spyClient)
        .checkLock(eq(dummyLockId));

    AssertHelpers.assertThrows("Expected an exception",
        CommitFailedException.class,
        "Could not acquire the lock on",
        () -> spyOps.doCommit(metadataV2, metadataV1));
  }

  @Test
  public void testLockTimeoutAfterRetries() throws TException, InterruptedException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse).when(spyClient).checkLock(eq(dummyLockId));

    AssertHelpers.assertThrows("Expected an exception",
        CommitFailedException.class,
        "Timed out after",
        () -> spyOps.doCommit(metadataV2, metadataV1));
  }

  @Test
  public void testPassThroughThriftExceptions() throws TException, InterruptedException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse).doThrow(new TException("Test Thrift Exception"))
        .when(spyClient).checkLock(eq(dummyLockId));

    AssertHelpers.assertThrows("Expected an exception",
        RuntimeException.class,
        "Metastore operation failed for",
        () -> spyOps.doCommit(metadataV2, metadataV1));
  }
}
