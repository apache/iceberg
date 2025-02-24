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

import static org.apache.iceberg.hive.MetastoreLock.HIVE_LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

public class TestMetastoreLock extends HiveTableBaseTest {

  @Test
  public void testReleaseExpiredLock() throws TException, InterruptedException {

    ScheduledExecutorService exitingScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-hive-lock-heartbeat-%d")
                .build());
    HiveClientPool clientPool = null;
    MetastoreLock.Heartbeat heartbeat = null;
    try {
      clientPool = new HiveClientPool(1, HIVE_METASTORE_EXTENSION.hiveConf());
      MetastoreLock lock1 =
          new MetastoreLock(
              HIVE_METASTORE_EXTENSION.hiveConf(),
              clientPool,
              CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
              DB_NAME,
              TABLE_NAME);

      long firstLockId = lock1.acquireLock();
      heartbeat =
          new MetastoreLock.Heartbeat(
              clientPool, firstLockId, HIVE_LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT);
      heartbeat.schedule(exitingScheduledExecutorService);

      MetastoreLock lock2 =
          new MetastoreLock(
              HIVE_METASTORE_EXTENSION.hiveConf(),
              clientPool,
              CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
              DB_NAME,
              TABLE_NAME);
      assertThatThrownBy(lock2::acquireLock).isInstanceOf(LockException.class);

      heartbeat.cancel();
      long secondLockId = lock2.acquireLock();
      lock2.doUnlock(secondLockId);
    } finally {
      if (heartbeat != null) {
        heartbeat.cancel();
      }
      exitingScheduledExecutorService.shutdown();
      if (clientPool != null) {
        clientPool.close();
      }
    }
  }
}
