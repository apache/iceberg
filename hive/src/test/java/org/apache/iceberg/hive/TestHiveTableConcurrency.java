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

import com.google.common.util.concurrent.MoreExecutors;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.Tasks;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;

public class TestHiveTableConcurrency extends HiveTableBaseTest {

  @Test
  public synchronized void testConcurrentFastAppends() {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    Table anotherIcebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    String fileName = UUID.randomUUID().toString();
    DataFile file = DataFiles.builder(icebergTable.spec())
        .withPath(FileFormat.PARQUET.addExtension(fileName))
        .withRecordCount(2)
        .withFileSizeInBytes(0)
        .build();

    ExecutorService executorService = MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);
    Tasks.foreach(icebergTable, anotherIcebergTable)
        .stopOnFailure().throwFailureWhenFinished()
        .executeWith(executorService)
        .run(table -> {
          for (int numCommittedFiles = 0; numCommittedFiles < 10; numCommittedFiles++) {
            while (barrier.get() < numCommittedFiles * 2) {
              try {
                Thread.sleep(10);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }

            table.newFastAppend().appendFile(file).commit();
            barrier.incrementAndGet();
          }
        });

    icebergTable.refresh();
    Assert.assertEquals(20, icebergTable.currentSnapshot().manifests().size());
  }

  @Test
  public synchronized void testConcurrentConnections() throws InterruptedException {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    icebergTable.updateProperties()
        .set(COMMIT_NUM_RETRIES, "20")
        .set(COMMIT_MIN_RETRY_WAIT_MS, "25")
        .set(COMMIT_MAX_RETRY_WAIT_MS, "25")
        .commit();

    String fileName = UUID.randomUUID().toString();
    DataFile file = DataFiles.builder(icebergTable.spec())
        .withPath(FileFormat.PARQUET.addExtension(fileName))
        .withRecordCount(2)
        .withFileSizeInBytes(0)
        .build();

    ExecutorService executorService = MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor) Executors.newFixedThreadPool(10));

    for (int i = 0; i < 10; i++) {
      executorService.submit(() -> icebergTable.newAppend().appendFile(file).commit());
    }

    executorService.shutdown();
    Assert.assertTrue("Timeout", executorService.awaitTermination(1, TimeUnit.MINUTES));
  }
}
