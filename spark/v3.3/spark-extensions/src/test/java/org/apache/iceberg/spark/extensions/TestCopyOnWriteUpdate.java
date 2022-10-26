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

import static org.apache.iceberg.TableProperties.UPDATE_ISOLATION_LEVEL;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestCopyOnWriteUpdate extends TestUpdate {

  public TestCopyOnWriteUpdate(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      boolean vectorized,
      String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
  }

  @Test
  public void testUpdateWithConcurrentTableRefresh() throws InterruptedException {
    // this test can only be run with Hive tables as it requires a reliable lock
    // also, the table cache must be enabled so that the same table instance can be reused
    Assume.assumeTrue(catalogName.equalsIgnoreCase("testhive"));

    createAndInitTable("id INT, dep STRING");

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, UPDATE_ISOLATION_LEVEL, "snapshot");

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);

    // update thread
    Future<?> updateFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < Integer.MAX_VALUE; numOperations++) {
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }
                sql("UPDATE %s SET id = -1 WHERE id = 1", tableName);
                barrier.incrementAndGet();
              }
            });

    // append thread
    Future<?> appendFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < Integer.MAX_VALUE; numOperations++) {
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }
                sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
                barrier.incrementAndGet();
              }
            });

    try {
      Assertions.assertThatThrownBy(updateFuture::get)
          .isInstanceOf(ExecutionException.class)
          .cause()
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("the table has been concurrently refreshed");
    } finally {
      appendFuture.cancel(true);
    }

    executorService.shutdown();
    Assert.assertTrue("Timeout", executorService.awaitTermination(2, TimeUnit.MINUTES));
  }
}
