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

import static org.apache.iceberg.PartitionSpec.builderFor;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;

public class TestHiveCommitLocks {
  private static HiveTableOperations spyOps = null;
  private static HiveClientPool spyClientPool = null;
  private static CachedClientPool spyCachedClientPool = null;
  private static Configuration overriddenHiveConf;
  private static final AtomicReference<IMetaStoreClient> SPY_CLIENT_REF = new AtomicReference<>();
  private static IMetaStoreClient spyClient = null;
  HiveTableOperations ops = null;
  TableMetadata metadataV1 = null;
  TableMetadata metadataV2 = null;

  long dummyLockId = 500L;
  LockResponse waitLockResponse = new LockResponse(dummyLockId, LockState.WAITING);
  LockResponse acquiredLockResponse = new LockResponse(dummyLockId, LockState.ACQUIRED);
  LockResponse notAcquiredLockResponse = new LockResponse(dummyLockId, LockState.NOT_ACQUIRED);
  ShowLocksResponse emptyLocks = new ShowLocksResponse(Lists.newArrayList());

  private static final String DB_NAME = "hivedb";
  private static final String TABLE_NAME = "tbl";
  private static final Schema SCHEMA =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
  private static final PartitionSpec PARTITION_SPEC = builderFor(SCHEMA).identity("id").build();
  static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);

  @RegisterExtension
  private static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION =
      HiveMetastoreExtension.builder()
          .withDatabase(DB_NAME)
          .withConfig(ImmutableMap.of(HiveConf.ConfVars.HIVE_TXN_TIMEOUT.varname, "1s"))
          .build();

  private static HiveCatalog catalog;
  private Path tableLocation;

  @BeforeAll
  public static void initCatalog() throws Exception {
    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                ImmutableMap.of(
                    CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                    String.valueOf(TimeUnit.SECONDS.toMillis(10))),
                HIVE_METASTORE_EXTENSION.hiveConf());

    // start spies
    overriddenHiveConf = new Configuration(HIVE_METASTORE_EXTENSION.hiveConf());
    overriddenHiveConf.setLong("iceberg.hive.lock-timeout-ms", 6 * 1000);
    overriddenHiveConf.setLong("iceberg.hive.lock-check-min-wait-ms", 50);
    overriddenHiveConf.setLong("iceberg.hive.lock-check-max-wait-ms", 5 * 1000);
    overriddenHiveConf.setLong("iceberg.hive.lock-heartbeat-interval-ms", 100);

    // Set up the spy clients as static variables instead of before every test.
    // The spy clients are reused between methods and closed at the end of all tests in this class.
    spyClientPool = spy(new HiveClientPool(1, overriddenHiveConf));
    when(spyClientPool.newClient())
        .thenAnswer(
            invocation -> {
              // cannot spy on RetryingHiveMetastoreClient as it is a proxy
              IMetaStoreClient client =
                  spy(new HiveMetaStoreClient(HIVE_METASTORE_EXTENSION.hiveConf()));
              SPY_CLIENT_REF.set(client);
              return SPY_CLIENT_REF.get();
            });

    spyClientPool.run(IMetaStoreClient::isLocalMetaStore); // To ensure new client is created.

    spyCachedClientPool =
        spy(new CachedClientPool(HIVE_METASTORE_EXTENSION.hiveConf(), Collections.emptyMap()));
    when(spyCachedClientPool.clientPool()).thenAnswer(invocation -> spyClientPool);

    assertThat(SPY_CLIENT_REF.get()).isNotNull();

    spyClient = SPY_CLIENT_REF.get();
  }

  @BeforeEach
  public void before() throws Exception {
    this.tableLocation =
        new Path(catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PARTITION_SPEC).location());
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    ops = (HiveTableOperations) ((HasTableOperations) table).operations();
    String dbName = TABLE_IDENTIFIER.namespace().level(0);
    String tableName = TABLE_IDENTIFIER.name();

    metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    metadataV2 = ops.current();

    assertThat(ops.current().schema().columns()).hasSize(2);

    spyOps =
        spy(
            new HiveTableOperations(
                overriddenHiveConf,
                spyCachedClientPool,
                ops.io(),
                catalog.name(),
                dbName,
                tableName));
    reset(spyClient);
  }

  @AfterEach
  public void dropTestTable() throws Exception {
    // drop the table data
    tableLocation.getFileSystem(HIVE_METASTORE_EXTENSION.hiveConf()).delete(tableLocation, true);
    catalog.dropTable(TABLE_IDENTIFIER, false /* metadata only, location was already deleted */);
  }

  @AfterAll
  public static void cleanup() {
    try {
      spyClientPool.close();
    } catch (Throwable t) {
      // Ignore any exception
    }
  }

  @Test
  public void testLockAcquisitionAtFirstTime() throws TException, InterruptedException {
    doReturn(acquiredLockResponse).when(spyClient).lock(any());
    doNothing().when(spyClient).unlock(eq(dummyLockId));
    doNothing().when(spyClient).heartbeat(eq(0L), eq(dummyLockId));

    spyOps.doCommit(metadataV2, metadataV1);

    assertThat(spyOps.current().schema().columns()).hasSize(1); // should be 1 again
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
    doNothing().when(spyClient).unlock(eq(dummyLockId));
    doNothing().when(spyClient).heartbeat(eq(0L), eq(dummyLockId));

    spyOps.doCommit(metadataV2, metadataV1);

    assertThat(spyOps.current().schema().columns()).hasSize(1); // should be 1 again
  }

  @Test
  public void testLockAcquisitionAfterFailedNotFoundLock() throws TException, InterruptedException {
    doReturn(emptyLocks).when(spyClient).showLocks(any());
    doThrow(new TException("Failed to connect to HMS"))
        .doReturn(waitLockResponse)
        .when(spyClient)
        .lock(any());
    doReturn(waitLockResponse)
        .doReturn(acquiredLockResponse)
        .when(spyClient)
        .checkLock(eq(dummyLockId));
    doNothing().when(spyClient).heartbeat(eq(0L), eq(dummyLockId));

    spyOps.doCommit(metadataV2, metadataV1);

    verify(spyClient, times(1)).showLocks(any()); // Make sure HiveLock's findLock method is called
    assertThat(spyOps.current().schema().columns()).hasSize(1); // should be 1 again
  }

  @Test
  public void testLockAcquisitionAfterFailedAndFoundLock() throws TException, InterruptedException {
    ArgumentCaptor<LockRequest> lockRequestCaptor = ArgumentCaptor.forClass(LockRequest.class);
    doReturn(emptyLocks).when(spyClient).showLocks(any());
    doThrow(new TException("Failed to connect to HMS"))
        .doReturn(waitLockResponse)
        .when(spyClient)
        .lock(lockRequestCaptor.capture());

    // Capture the lockRequest, and generate a response simulating that we have a lock
    ShowLocksResponse showLocksResponse = new ShowLocksResponse(Lists.newArrayList());
    ShowLocksResponseElement showLocksElement =
        new ShowLocksResponseElementWrapper(lockRequestCaptor);
    showLocksResponse.getLocks().add(showLocksElement);

    doReturn(showLocksResponse).when(spyClient).showLocks(any());
    doReturn(acquiredLockResponse).when(spyClient).checkLock(eq(dummyLockId));
    doNothing().when(spyClient).heartbeat(eq(0L), eq(dummyLockId));

    spyOps.doCommit(metadataV2, metadataV1);

    verify(spyClient, times(1)).showLocks(any()); // Make sure HiveLock's findLock method is called
    assertThat(spyOps.current().schema().columns()).hasSize(1); // should be 1 again
  }

  @Test
  public void testUnLock() throws TException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(acquiredLockResponse).when(spyClient).checkLock(eq(dummyLockId));
    doNothing().when(spyClient).unlock(eq(dummyLockId));
    doNothing().when(spyClient).heartbeat(eq(0L), eq(dummyLockId));

    spyOps.doCommit(metadataV2, metadataV1);

    verify(spyClient, times(1)).unlock(eq(dummyLockId));
  }

  @Test
  public void testUnLockInterruptedUnLock() throws TException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(acquiredLockResponse).when(spyClient).checkLock(eq(dummyLockId));
    doAnswer(
            invocation -> {
              throw new InterruptedException("Interrupt test");
            })
        .doNothing()
        .when(spyClient)
        .unlock(eq(dummyLockId));
    doNothing().when(spyClient).heartbeat(eq(0L), eq(dummyLockId));

    spyOps.doCommit(metadataV2, metadataV1);

    verify(spyClient, times(2)).unlock(eq(dummyLockId));
  }

  @Test
  public void testUnLockAfterInterruptedLock() throws TException {
    ArgumentCaptor<LockRequest> lockRequestCaptor = ArgumentCaptor.forClass(LockRequest.class);
    doAnswer(
            invocation -> {
              throw new InterruptedException("Interrupt test");
            })
        .when(spyClient)
        .lock(lockRequestCaptor.capture());

    // Capture the lockRequest, and generate a response simulating that we have a lock
    ShowLocksResponse showLocksResponse = new ShowLocksResponse(Lists.newArrayList());
    ShowLocksResponseElement showLocksElement =
        new ShowLocksResponseElementWrapper(lockRequestCaptor);
    showLocksResponse.getLocks().add(showLocksElement);

    doReturn(showLocksResponse).when(spyClient).showLocks(any());
    doReturn(acquiredLockResponse).when(spyClient).checkLock(eq(dummyLockId));
    doNothing().when(spyClient).unlock(eq(dummyLockId));
    doNothing().when(spyClient).heartbeat(eq(0L), eq(dummyLockId));

    assertThatThrownBy(() -> spyOps.doCommit(metadataV2, metadataV1))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "org.apache.iceberg.hive.LockException: "
                + "Interrupted while creating lock on table hivedb.tbl");

    verify(spyClient, times(1)).unlock(eq(dummyLockId));
    // Make sure that we exit the lock loop on InterruptedException
    verify(spyClient, times(1)).lock(any());
  }

  @Test
  public void testUnLockAfterInterruptedLockCheck() throws TException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doAnswer(
            invocation -> {
              throw new InterruptedException("Interrupt test");
            })
        .when(spyClient)
        .checkLock(eq(dummyLockId));

    doNothing().when(spyClient).unlock(eq(dummyLockId));
    doNothing().when(spyClient).heartbeat(eq(0L), eq(dummyLockId));

    assertThatThrownBy(() -> spyOps.doCommit(metadataV2, metadataV1))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "org.apache.iceberg.hive.LockException: "
                + "Could not acquire the lock on hivedb.tbl, lock request ended in state WAITING");

    verify(spyClient, times(1)).unlock(eq(dummyLockId));
    // Make sure that we exit the checkLock loop on InterruptedException
    verify(spyClient, times(1)).checkLock(eq(dummyLockId));
  }

  @Test
  public void testUnLockAfterInterruptedGetTable() throws TException {
    doReturn(acquiredLockResponse).when(spyClient).lock(any());
    doAnswer(
            invocation -> {
              throw new InterruptedException("Interrupt test");
            })
        .when(spyClient)
        .getTable(any(), any());

    doNothing().when(spyClient).unlock(eq(dummyLockId));
    doNothing().when(spyClient).heartbeat(eq(0L), eq(dummyLockId));

    assertThatThrownBy(() -> spyOps.doCommit(metadataV2, metadataV1))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Interrupted during commit");

    verify(spyClient, times(1)).unlock(eq(dummyLockId));
  }

  /** Wraps an ArgumentCaptor to provide data based on the request */
  private class ShowLocksResponseElementWrapper extends ShowLocksResponseElement {
    private final ArgumentCaptor<LockRequest> wrapped;

    private ShowLocksResponseElementWrapper(ArgumentCaptor<LockRequest> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public String getAgentInfo() {
      return wrapped.getValue().getAgentInfo();
    }

    @Override
    public LockState getState() {
      return LockState.WAITING;
    }

    @Override
    public long getLockid() {
      return dummyLockId;
    }
  }

  @Test
  public void testLockFailureAtFirstTime() throws TException {
    doReturn(notAcquiredLockResponse).when(spyClient).lock(any());

    assertThatThrownBy(() -> spyOps.doCommit(metadataV2, metadataV1))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage(
            "org.apache.iceberg.hive.LockException: "
                + "Could not acquire the lock on hivedb.tbl, lock request ended in state NOT_ACQUIRED");
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

    assertThatThrownBy(() -> spyOps.doCommit(metadataV2, metadataV1))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage(
            "org.apache.iceberg.hive.LockException: "
                + "Could not acquire the lock on hivedb.tbl, lock request ended in state NOT_ACQUIRED");
  }

  @Test
  public void testLockTimeoutAfterRetries() throws TException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse).when(spyClient).checkLock(eq(dummyLockId));

    assertThatThrownBy(() -> spyOps.doCommit(metadataV2, metadataV1))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageStartingWith("org.apache.iceberg.hive.LockException")
        .hasMessageContaining("Timed out after")
        .hasMessageEndingWith("waiting for lock on hivedb.tbl");
  }

  @Test
  public void testPassThroughThriftExceptions() throws TException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse)
        .doThrow(new TException("Test Thrift Exception"))
        .when(spyClient)
        .checkLock(eq(dummyLockId));

    assertThatThrownBy(() -> spyOps.doCommit(metadataV2, metadataV1))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "org.apache.iceberg.hive.LockException: Metastore operation failed for hivedb.tbl");
  }

  @Test
  public void testPassThroughThriftExceptionsForHiveVersion_1()
      throws TException, InterruptedException {
    try (MockedStatic<HiveVersion> ignore = mockStatic(HiveVersion.class)) {
      // default order is 0, meets the requirements of this test
      HiveVersion version = mock(HiveVersion.class);
      when(HiveVersion.current()).thenReturn(version);

      doReturn(emptyLocks).when(spyClient).showLocks(any());
      doThrow(new TException("Failed to connect to HMS"))
          .doReturn(waitLockResponse)
          .when(spyClient)
          .lock(any());
      doReturn(waitLockResponse)
          .doReturn(acquiredLockResponse)
          .when(spyClient)
          .checkLock(eq(dummyLockId));
      doNothing().when(spyClient).heartbeat(eq(0L), eq(dummyLockId));

      assertThatThrownBy(() -> spyOps.doCommit(metadataV2, metadataV1))
          .isInstanceOf(CommitFailedException.class)
          .hasMessage(
              "org.apache.iceberg.hive.LockException: Failed to find lock for table hivedb.tbl");
    }
  }

  @Test
  public void testPassThroughInterruptions() throws TException {
    doReturn(waitLockResponse).when(spyClient).lock(any());
    doReturn(waitLockResponse)
        .doAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              Thread.sleep(10);
              return waitLockResponse;
            })
        .when(spyClient)
        .checkLock(eq(dummyLockId));

    assertThatThrownBy(() -> spyOps.doCommit(metadataV2, metadataV1))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage(
            "org.apache.iceberg.hive.LockException: "
                + "Could not acquire the lock on hivedb.tbl, lock request ended in state WAITING");
  }

  @Test
  public void testTableLevelProcessLockBlocksConcurrentHMSRequestsForSameTable() throws Exception {
    int numConcurrentCommits = 10;
    // resetting the spy client to forget about prior call history
    reset(spyClient);

    // simulate several concurrent commit operations on the same table
    ExecutorService executor = Executors.newFixedThreadPool(numConcurrentCommits);
    IntStream.range(0, numConcurrentCommits)
        .forEach(
            i ->
                executor.submit(
                    () -> {
                      try {
                        spyOps.doCommit(metadataV2, metadataV1);
                      } catch (CommitFailedException e) {
                        // failures are expected here when checking the base version
                        // it's no problem, we're not testing the actual commit success here, only
                        // the HMS lock acquisition attempts
                      }
                    }));
    executor.shutdown();
    executor.awaitTermination(30, TimeUnit.SECONDS);

    // intra-process commits to the same table should be serialized now
    // i.e. no thread should receive WAITING state from HMS and have to call checkLock periodically
    verify(spyClient, never()).checkLock(any(Long.class));
    // all threads eventually got their turn
    verify(spyClient, times(numConcurrentCommits)).lock(any(LockRequest.class));
  }

  @Test
  public void testLockHeartbeat() throws TException, InterruptedException {
    doReturn(acquiredLockResponse).when(spyClient).lock(any());
    doAnswer(AdditionalAnswers.answersWithDelay(2000, InvocationOnMock::callRealMethod))
        .when(spyOps)
        .loadHmsTable();
    doNothing().when(spyClient).heartbeat(eq(0L), eq(dummyLockId));

    spyOps.doCommit(metadataV2, metadataV1);

    verify(spyClient, atLeastOnce()).heartbeat(eq(0L), eq(dummyLockId));
  }

  @Test
  public void testLockHeartbeatFailureDuringCommit() throws TException, InterruptedException {
    doReturn(acquiredLockResponse).when(spyClient).lock(any());
    doAnswer(AdditionalAnswers.answersWithDelay(2000, InvocationOnMock::callRealMethod))
        .when(spyOps)
        .loadHmsTable();
    doThrow(new TException("Failed to heart beat."))
        .when(spyClient)
        .heartbeat(eq(0L), eq(dummyLockId));

    assertThatThrownBy(() -> spyOps.doCommit(metadataV2, metadataV1))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage(
            "org.apache.iceberg.hive.LockException: "
                + "Failed to heartbeat for hive lock. Failed to heart beat.");
  }

  @Test
  public void testNoLockCallsWithNoLock() throws TException {
    Configuration confWithLock = new Configuration(overriddenHiveConf);
    confWithLock.setBoolean(ConfigProperties.LOCK_HIVE_ENABLED, false);

    HiveTableOperations noLockSpyOps =
        spy(
            new HiveTableOperations(
                confWithLock,
                spyCachedClientPool,
                ops.io(),
                catalog.name(),
                TABLE_IDENTIFIER.namespace().level(0),
                TABLE_IDENTIFIER.name()));

    ArgumentCaptor<EnvironmentContext> contextCaptor =
        ArgumentCaptor.forClass(EnvironmentContext.class);

    doNothing()
        .when(spyClient)
        .alter_table_with_environmentContext(any(), any(), any(), contextCaptor.capture());

    noLockSpyOps.doCommit(metadataV2, metadataV1);

    // Make sure that the locking is not used
    verify(spyClient, never()).lock(any(LockRequest.class));
    verify(spyClient, never()).checkLock(any(Long.class));
    verify(spyClient, never()).heartbeat(any(Long.class), any(Long.class));
    verify(spyClient, never()).unlock(any(Long.class));

    // Make sure that the expected parameter context values are set
    Map<String, String> context = contextCaptor.getValue().getProperties();
    assertThat(context).hasSize(3);
    assertThat(HiveTableOperations.METADATA_LOCATION_PROP)
        .isEqualTo(context.get("expected_parameter_key"));
    assertThat(metadataV2.metadataFileLocation())
        .isEqualTo(context.get("expected_parameter_value"));
  }
}
