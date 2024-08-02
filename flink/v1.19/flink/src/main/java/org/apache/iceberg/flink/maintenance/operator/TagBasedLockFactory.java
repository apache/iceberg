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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg table {@link ManageSnapshots#createTag(String, long)}/{@link
 * ManageSnapshots#removeTag(String)} based lock implementation for {@link TriggerLockFactory}.
 */
@Internal
public class TagBasedLockFactory implements TriggerLockFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TagBasedLockFactory.class);
  @VisibleForTesting static final String RUNNING_TAG_PREFIX = "__flink_maintenance_running_";
  @VisibleForTesting static final String RECOVERING_TAG_PREFIX = "__flink_maintenance_recovering_";
  private static final int CHANGE_ATTEMPTS = 3;

  private final TableLoader tableLoader;
  private transient Table table;

  public TagBasedLockFactory(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void open() {
    tableLoader.open();
    this.table = tableLoader.loadTable();
  }

  @Override
  public TriggerLockFactory.Lock createLock() {
    return new Lock(table, RUNNING_TAG_PREFIX);
  }

  @Override
  public TriggerLockFactory.Lock createRecoveryLock() {
    return new Lock(table, RECOVERING_TAG_PREFIX);
  }

  @Override
  public void close() throws IOException {
    tableLoader.close();
  }

  public static class Lock implements TriggerLockFactory.Lock {
    private final Table table;
    private final String lockPrefix;

    public Lock(Table table, String lockPrefix) {
      Preconditions.checkNotNull(table, "Table should not be null");
      Preconditions.checkNotNull(lockPrefix, "Lock key should not be null");
      this.table = table;
      this.lockPrefix = lockPrefix;
    }

    /**
     * The lock will be acquired by jobs with creating a new tag. A new empty commit is added for a
     * table without snapshots.
     *
     * @return <code>true</code> if the lock is acquired by this operator
     */
    @Override
    public boolean tryLock() {
      if (isHeld()) {
        LOG.info("Lock is already held. The relevant tag: {}", findLock());
        return false;
      }

      if (table.currentSnapshot() == null) {
        // Create an empty commit
        table.newFastAppend().commit();
        LOG.info("Empty table, new empty commit added for using tags");
      }

      String lockKey = lockPrefix + UUID.randomUUID();
      try {
        Tasks.foreach(1)
            .retry(CHANGE_ATTEMPTS)
            .stopOnFailure()
            .throwFailureWhenFinished()
            .run(
                unused -> {
                  try {
                    table.refresh();
                    ManageSnapshots manage = table.manageSnapshots();
                    manage.createTag(lockKey, table.currentSnapshot().snapshotId());
                    manage.commit();
                    LOG.debug("Lock created");
                  } catch (Exception e) {
                    if (!isHeld()) {
                      LOG.warn("Retrying lock after exception", e);
                      throw e;
                    } else {
                      LOG.debug("Lock created, hiding exception", e);
                    }
                  }
                });
      } catch (Exception e) {
        LOG.info("Concurrent lock created. Is there a concurrent maintenance job running?", e);
        return false;
      }

      return true;
    }

    @Override
    public void unlock() {
      table.refresh();

      Optional<String> lockKey = findLock();
      if (lockKey.isPresent()) {
        Tasks.foreach(lockKey.get())
            .retry(CHANGE_ATTEMPTS)
            .stopOnFailure()
            .throwFailureWhenFinished()
            .run(
                key -> {
                  try {
                    table.refresh();
                    ManageSnapshots manage = table.manageSnapshots();
                    manage.removeTag(key);
                    manage.commit();
                  } catch (Exception e) {
                    table.refresh();
                    if (table.refs().containsKey(key)) {
                      LOG.warn("Retrying lock removal after exception", e);
                      throw e;
                    } else {
                      LOG.debug("Lock removed, hiding exception", e);
                    }
                  }
                });
        LOG.debug("Lock removed");
      } else {
        LOG.warn(
            "Missing lock, can not remove. Found only the following tags: {}.",
            table.refs().keySet());
      }
    }

    @Override
    public boolean isHeld() {
      table.refresh();
      return findLock().isPresent();
    }

    private Optional<String> findLock() {
      List<String> locks =
          table.refs().keySet().stream()
              .filter(tag -> tag.startsWith(lockPrefix))
              .collect(Collectors.toList());
      Preconditions.checkArgument(locks.size() < 2, "Invalid lock state: %s", locks);
      return locks.stream().findAny();
    }
  }
}
