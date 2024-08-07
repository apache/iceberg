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
package org.apache.iceberg.flink.maintenance.operator;

import static org.apache.iceberg.flink.maintenance.operator.TagBasedLockFactory.RUNNING_TAG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestTagBasedLockFactory extends TestLockFactoryBase {
  @Override
  TriggerLockFactory lockFactory() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    return new TagBasedLockFactory(tableLoader);
  }

  @Test
  void testTryLockWithEmptyTable() throws IOException {
    sql.exec("CREATE TABLE %s (id int, data varchar)", "empty_table");

    TableLoader tableLoader = sql.tableLoader("empty_table");
    try (TriggerLockFactory lockFactory = new TagBasedLockFactory(tableLoader)) {
      lockFactory.open();
      TriggerLockFactory.Lock lock = lockFactory.createLock();
      assertThat(lock.tryLock()).isTrue();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testLockRetries(boolean alreadyHeld) throws IOException {
    // Prepare the mocks
    TableLoader spyLoader = spy(sql.tableLoader(TABLE_NAME));
    Table spyTable = spy(spyLoader.loadTable());
    ManageSnapshots spyManageSnapshots = spy(spyTable.manageSnapshots());
    doReturn(spyTable).when(spyLoader).loadTable();
    doReturn(spyManageSnapshots).when(spyTable).manageSnapshots();
    // First return a failure, succeed later
    AtomicBoolean success = new AtomicBoolean(false);
    doAnswer(
            unused -> {
              if (success.get()) {
                return null;
              } else {
                // succeed next time
                success.set(true);
                throw new CommitFailedException("Test failure");
              }
            })
        .when(spyManageSnapshots)
        .createTag(anyString(), anyLong());
    // If the tag should be created even on failure return the refs accordingly
    SnapshotRef snapshotRef = spyTable.refs().get("main");
    doAnswer(unused -> ImmutableMap.of("main", snapshotRef))
        .doAnswer(
            unused -> {
              if (alreadyHeld) {
                return ImmutableMap.of("main", snapshotRef, RUNNING_TAG_PREFIX, snapshotRef);
              } else {
                return ImmutableMap.of("main", snapshotRef);
              }
            })
        .when(spyTable)
        .refs();

    // Run the test
    try (TriggerLockFactory factoryToTest = new TagBasedLockFactory(spyLoader)) {
      factoryToTest.open();
      TriggerLockFactory.Lock lock = factoryToTest.createLock();

      lock.tryLock();
      if (alreadyHeld) {
        verify(spyManageSnapshots, times(1)).createTag(anyString(), anyLong());
      } else {
        verify(spyManageSnapshots, times(2)).createTag(anyString(), anyLong());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUnlockRetries(boolean alreadyRemoved) throws IOException {
    // Run the unlock test
    TableLoader spyLoader = spy(sql.tableLoader(TABLE_NAME));
    Table spyTable = spy(spyLoader.loadTable());
    ManageSnapshots spyManageSnapshots = spy(spyTable.manageSnapshots());
    doReturn(spyTable).when(spyLoader).loadTable();
    doReturn(spyManageSnapshots).when(spyTable).manageSnapshots();
    // First return a failure, succeed later
    AtomicBoolean success = new AtomicBoolean(false);
    doAnswer(
            unused -> {
              if (success.get()) {
                return null;
              } else {
                // succeed next time
                success.set(true);
                throw new CommitFailedException("Test failure");
              }
            })
        .when(spyManageSnapshots)
        .removeTag(anyString());
    // If the tag should be removed even on failure return the tags accordingly
    SnapshotRef snapshotRef = spyTable.refs().get("main");
    doAnswer(unused -> ImmutableMap.of("main", snapshotRef, RUNNING_TAG_PREFIX, snapshotRef))
        .doAnswer(
            unused -> {
              if (alreadyRemoved) {
                return ImmutableMap.of("main", snapshotRef);
              } else {
                return ImmutableMap.of("main", snapshotRef, RUNNING_TAG_PREFIX, snapshotRef);
              }
            })
        .when(spyTable)
        .refs();

    try (TriggerLockFactory factoryToTest = new TagBasedLockFactory(spyLoader)) {
      factoryToTest.open();
      TriggerLockFactory.Lock lock = factoryToTest.createLock();

      lock.unlock();
      if (alreadyRemoved) {
        verify(spyManageSnapshots, times(1)).removeTag(anyString());
      } else {
        verify(spyManageSnapshots, times(2)).removeTag(anyString());
      }
    }
  }
}
