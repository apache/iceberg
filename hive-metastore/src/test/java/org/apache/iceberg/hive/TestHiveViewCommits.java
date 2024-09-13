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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Test Hive locks and Hive errors and retry during commits. */
public class TestHiveViewCommits {

  private static final String VIEW_NAME = "test_iceberg_view";
  private static final String DB_NAME = "hivedb";
  private static final Namespace NS = Namespace.of(DB_NAME);
  private static final Schema SCHEMA =
      new Schema(
          5,
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));
  private static final TableIdentifier VIEW_IDENTIFIER = TableIdentifier.of(NS, VIEW_NAME);

  @RegisterExtension
  protected static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION =
      HiveMetastoreExtension.builder().withDatabase(DB_NAME).build();

  private View view;
  private Path viewLocation;

  private static HiveCatalog catalog;

  @BeforeAll
  public static void initCatalog() {
    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                ImmutableMap.of(
                    CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                    String.valueOf(TimeUnit.SECONDS.toMillis(10))),
                HIVE_METASTORE_EXTENSION.hiveConf());
  }

  @BeforeEach
  public void createTestView() {
    view =
        catalog
            .buildView(VIEW_IDENTIFIER)
            .withSchema(SCHEMA)
            .withDefaultNamespace(NS)
            .withQuery("hive", "select * from ns.tbl")
            .create();
    viewLocation = new Path(view.location());
  }

  @AfterEach
  public void dropTestView() throws IOException {
    viewLocation.getFileSystem(HIVE_METASTORE_EXTENSION.hiveConf()).delete(viewLocation, true);
    catalog.dropView(VIEW_IDENTIFIER);
  }

  @Test
  public void testSuppressUnlockExceptions() {
    HiveViewOperations ops = (HiveViewOperations) ((BaseView) view).operations();
    ViewMetadata metadataV1 = ops.current();
    assertThat(ops.current().properties()).hasSize(0);

    view.updateProperties().set("k1", "v1").commit();
    ops.refresh();
    ViewMetadata metadataV2 = ops.current();
    assertThat(ops.current().properties()).hasSize(1).containsEntry("k1", "v1");

    HiveViewOperations spyOps = spy(ops);

    AtomicReference<HiveLock> lockRef = new AtomicReference<>();

    when(spyOps.lockObject())
        .thenAnswer(
            i -> {
              HiveLock lock = (HiveLock) i.callRealMethod();
              lockRef.set(lock);
              return lock;
            });

    try {
      spyOps.commit(metadataV2, metadataV1);
      HiveLock spyLock = spy(lockRef.get());
      doThrow(new RuntimeException()).when(spyLock).unlock();
    } finally {
      lockRef.get().unlock();
    }

    ops.refresh();

    // the commit must succeed
    assertThat(ops.current().properties()).hasSize(0);
  }

  /**
   * Pretends we throw an error while persisting, and not found with check state, commit state
   * should be treated as unknown, because in reality the persisting may still succeed, just not yet
   * by the time of checking.
   */
  @Test
  public void testThriftExceptionUnknownStateIfNotInHistoryFailureOnCommit()
      throws TException, InterruptedException {
    HiveViewOperations ops = (HiveViewOperations) ((BaseView) view).operations();
    assertThat(ops.current().properties()).hasSize(0);

    view.updateProperties().set("k1", "v1").commit();
    ops.refresh();
    ViewMetadata metadataV2 = ops.current();
    assertThat(ops.current().properties()).hasSize(1).containsEntry("k1", "v1");

    HiveViewOperations spyOps = spy(ops);

    failCommitAndThrowException(spyOps);

    ops.refresh();

    assertThat(ops.current()).as("Current metadata should not have changed").isEqualTo(metadataV2);
    assertThat(metadataFileExists(metadataV2)).as("Current metadata should still exist").isTrue();
    assertThat(metadataFileCount(ops.current()))
        .as(
            "New metadata files should still exist, new location not in history but"
                + " the commit may still succeed")
        .isEqualTo(2);
  }

  /** Pretends we throw an error while persisting that actually does commit serverside. */
  @Test
  public void testThriftExceptionSuccessOnCommit() throws TException, InterruptedException {
    HiveViewOperations ops = (HiveViewOperations) ((BaseView) view).operations();

    ViewMetadata metadataV1 = ops.current();
    assertThat(ops.current().properties()).hasSize(0);

    view.updateProperties().set("k1", "v1").commit();
    ops.refresh();
    assertThat(ops.current().properties()).hasSize(1).containsEntry("k1", "v1");
    ViewMetadata metadataV2 = ops.current();

    HiveViewOperations spyOps = spy(ops);

    // Simulate a communication error after a successful commit
    commitAndThrowException(ops, spyOps);
    spyOps.commit(metadataV2, metadataV1);

    assertThat(ops.current()).as("Current metadata should have not changed").isEqualTo(metadataV2);
    assertThat(metadataFileExists(ops.current()))
        .as("Current metadata file should still exist")
        .isTrue();
  }

  /**
   * Pretends we throw an exception while persisting and don't know what happened, can't check to
   * find out, but in reality the commit failed
   */
  @Test
  public void testThriftExceptionUnknownFailedCommit() throws TException, InterruptedException {
    HiveViewOperations ops = (HiveViewOperations) ((BaseView) view).operations();
    ViewMetadata metadataV1 = ops.current();
    assertThat(ops.current().properties()).hasSize(0);

    view.updateProperties().set("k1", "v1").commit();
    ops.refresh();
    ViewMetadata metadataV2 = ops.current();
    assertThat(ops.current().properties()).hasSize(1).containsEntry("k1", "v1");

    HiveViewOperations spyOps = spy(ops);

    failCommitAndThrowException(spyOps);
    breakFallbackCatalogCommitCheck(spyOps);

    assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("Datacenter on fire");

    ops.refresh();

    assertThat(ops.current()).as("Current metadata should not have changed").isEqualTo(metadataV2);
    assertThat(metadataFileExists(ops.current()))
        .as("Current metadata file should still exist")
        .isTrue();
  }

  /**
   * Pretends we threw an exception while persisting, the commit succeeded, the lock expired, and a
   * second committer placed a commit on top of ours before the first committer was able to check if
   * their commit succeeded or not
   *
   * <p>Timeline:
   *
   * <ul>
   *   <li>Client 1 commits which throws an exception but succeeded
   *   <li>Client 1's lock expires while waiting to do the recheck for commit success
   *   <li>Client 2 acquires a lock, commits successfully on top of client 1's commit and release
   *       lock
   *   <li>Client 1 check's to see if their commit was successful
   * </ul>
   *
   * <p>This tests to make sure a disconnected client 1 doesn't think their commit failed just
   * because it isn't the current one during the recheck phase.
   */
  @Test
  public void testThriftExceptionConcurrentCommit() throws TException, InterruptedException {
    HiveViewOperations ops = (HiveViewOperations) ((BaseView) view).operations();
    ViewMetadata metadataV1 = ops.current();
    assertThat(ops.current().properties()).hasSize(0);

    view.updateProperties().set("k0", "v0").commit();
    ops.refresh();
    ViewMetadata metadataV2 = ops.current();
    assertThat(ops.current().properties()).hasSize(1).containsEntry("k0", "v0");

    HiveViewOperations spyOps = spy(ops);

    AtomicReference<HiveLock> lock = new AtomicReference<>();
    doAnswer(
            l -> {
              lock.set(ops.lockObject());
              return lock.get();
            })
        .when(spyOps)
        .lockObject();

    concurrentCommitAndThrowException(ops, spyOps, (BaseView) view, lock);

    /*
    This commit should fail and concurrent commit should succeed even though this commit throws an exception
    after the persist operation succeeds
     */
    assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageContaining("Datacenter on fire");

    ops.refresh();

    assertThat(ops.current()).as("Current metadata should have changed").isNotEqualTo(metadataV2);
    assertThat(metadataFileExists(ops.current()))
        .as("Current metadata file should still exist")
        .isTrue();
    assertThat(ops.current().properties())
        .as("The new properties from the concurrent commit should have been successful")
        .hasSize(2);
  }

  @Test
  public void testInvalidObjectException() {
    TableIdentifier badTi = TableIdentifier.of(DB_NAME, "`test_iceberg_view`");
    assertThatThrownBy(
            () ->
                catalog
                    .buildView(badTi)
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(NS)
                    .withQuery("hive", "select * from ns.tbl")
                    .create())
        .isInstanceOf(ValidationException.class)
        .hasMessage(String.format("Invalid Hive object for %s.%s", DB_NAME, "`test_iceberg_view`"));
  }

  /** Uses NoLock and pretends we throw an error because of a concurrent commit */
  @Test
  public void testNoLockThriftExceptionConcurrentCommit() throws TException, InterruptedException {
    HiveViewOperations ops = (HiveViewOperations) ((BaseView) view).operations();
    assertThat(ops.current().properties()).hasSize(0);

    view.updateProperties().set("k1", "v1").commit();
    ops.refresh();
    ViewMetadata metadataV2 = ops.current();
    assertThat(ops.current().properties()).hasSize(1).containsEntry("k1", "v1");

    HiveViewOperations spyOps = spy(ops);

    // Sets NoLock
    doReturn(new NoLock()).when(spyOps).lockObject();

    // Simulate a concurrent view modification error
    doThrow(
            new RuntimeException(
                "MetaException(message:The table has been modified. The parameter value for key 'metadata_location' is"))
        .when(spyOps)
        .persistTable(any(), anyBoolean(), any());

    ops.refresh();

    assertThat(ops.current()).as("Current metadata should not have changed").isEqualTo(metadataV2);
    assertThat(metadataFileExists(metadataV2)).as("Current metadata should still exist").isTrue();
    assertThat(metadataFileCount(ops.current()))
        .as("New metadata files should not exist")
        .isEqualTo(2);
  }

  @Test
  public void testLockExceptionUnknownSuccessCommit() throws TException, InterruptedException {
    HiveViewOperations ops = (HiveViewOperations) ((BaseView) view).operations();
    ViewMetadata metadataV1 = ops.current();
    assertThat(ops.current().properties()).hasSize(0);

    view.updateProperties().set("k1", "v1").commit();
    ops.refresh();
    ViewMetadata metadataV2 = ops.current();
    assertThat(ops.current().properties()).hasSize(1).containsEntry("k1", "v1");

    HiveViewOperations spyOps = spy(ops);

    // Simulate a communication error after a successful commit
    doAnswer(
            i -> {
              org.apache.hadoop.hive.metastore.api.Table tbl =
                  i.getArgument(0, org.apache.hadoop.hive.metastore.api.Table.class);
              String location = i.getArgument(2, String.class);
              ops.persistTable(tbl, true, location);
              throw new LockException("Datacenter on fire");
            })
        .when(spyOps)
        .persistTable(any(), anyBoolean(), any());

    assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .hasMessageContaining("Failed to heartbeat for hive lock while")
        .isInstanceOf(CommitStateUnknownException.class);

    ops.refresh();

    assertThat(ops.current().location())
        .as("Current metadata should have changed to metadata V1")
        .isEqualTo(metadataV1.location());
    assertThat(metadataFileExists(ops.current()))
        .as("Current metadata file should still exist")
        .isTrue();
  }

  private void commitAndThrowException(
      HiveViewOperations realOperations, HiveViewOperations spyOperations)
      throws TException, InterruptedException {
    // Simulate a communication error after a successful commit
    doAnswer(
            i -> {
              org.apache.hadoop.hive.metastore.api.Table tbl =
                  i.getArgument(0, org.apache.hadoop.hive.metastore.api.Table.class);
              String location = i.getArgument(2, String.class);
              realOperations.persistTable(tbl, true, location);
              throw new TException("Datacenter on fire");
            })
        .when(spyOperations)
        .persistTable(any(), anyBoolean(), any());
  }

  private void concurrentCommitAndThrowException(
      HiveViewOperations realOperations,
      HiveViewOperations spyOperations,
      BaseView baseView,
      AtomicReference<HiveLock> lock)
      throws TException, InterruptedException {
    // Simulate a communication error after a successful commit
    doAnswer(
            i -> {
              org.apache.hadoop.hive.metastore.api.Table tbl =
                  i.getArgument(0, org.apache.hadoop.hive.metastore.api.Table.class);
              String location = i.getArgument(2, String.class);
              realOperations.persistTable(tbl, true, location);
              // Simulate lock expiration or removal
              lock.get().unlock();
              baseView.operations().refresh();
              baseView.updateProperties().set("k1", "v1").set("k2", "v2").commit();
              throw new TException("Datacenter on fire");
            })
        .when(spyOperations)
        .persistTable(any(), anyBoolean(), any());
  }

  private void failCommitAndThrowException(HiveViewOperations spyOperations)
      throws TException, InterruptedException {
    doThrow(new TException("Datacenter on fire"))
        .when(spyOperations)
        .persistTable(any(), anyBoolean(), any());
  }

  private void breakFallbackCatalogCommitCheck(HiveViewOperations spyOperations) {
    when(spyOperations.refresh())
        .thenThrow(new RuntimeException("Still on fire")); // Failure on commit check
  }

  private boolean metadataFileExists(ViewMetadata metadata) {
    return new File(metadata.metadataFileLocation().replace("file:", "")).exists();
  }

  private int metadataFileCount(ViewMetadata metadata) {
    return new File(metadata.metadataFileLocation().replace("file:", ""))
        .getParentFile()
        .listFiles(file -> file.getName().endsWith("metadata.json"))
        .length;
  }
}
