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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestScanTaskIterableLeak {

  @Test
  public void testBackgroundWorkerLeakOnEarlyClose() throws Exception {
    /* * Generate tasks exceeding the default queue capacity (1000) to ensure
     * that background PlanTaskWorker threads fill the buffer and block.
     */
    int totalTasks = 1050;
    List<FileScanTask> mockTasks =
        IntStream.range(0, totalTasks)
            .mapToObj(i -> Mockito.mock(FileScanTask.class))
            .collect(Collectors.toList());

    ThreadPoolExecutor planningExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

    ScanTaskIterable scanTaskIterable =
        new ScanTaskIterable(
            Collections.emptyList(),
            mockTasks,
            Mockito.mock(RESTClient.class),
            null,
            null,
            Collections.emptyMap(),
            planningExecutor,
            null);

    CloseableIterable<FileScanTask> wrappedIterable =
        CloseableIterable.whenComplete(scanTaskIterable, () -> {});

    CloseableIterator<FileScanTask> iterator = wrappedIterable.iterator();
    if (iterator.hasNext()) {
      iterator.next();
    }

    wrappedIterable.close();

    Thread.sleep(2000);

    int activeCount = planningExecutor.getActiveCount();
    planningExecutor.shutdownNow();

    assertThat(activeCount)
        .as("PlanTaskWorker background threads should have terminated when the iterable closed")
        .isEqualTo(0);
  }
}
