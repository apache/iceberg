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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.Test;

public class TestThreadPools {

  @Test
  public void testNewExitingWorkerPool() {
    ExecutorService executor = ThreadPools.newExitingWorkerPool("test-shutdown-hook", 1);
    try {
      assertThat(ManagedThreadPools.getPools()).contains(executor);
      assertThat(executor.isShutdown()).as("Executor should be running").isFalse();
    } finally {
      ManagedThreadPools.remove(executor);
      executor.shutdown();
      assertThat(executor.isShutdown()).as("Executor should be shut down").isTrue();
    }
  }

  @Test
  public void testShutdownAllClosesAndClearsPools() {
    ExecutorService e1 = ThreadPools.newExitingWorkerPool("test-shutdown-all-1", 1);
    ExecutorService e2 = ThreadPools.newExitingWorkerPool("test-shutdown-all-2", 1);
    assertThat(ManagedThreadPools.getPools()).contains(e1, e2);

    ManagedThreadPools.shutdownAll();

    assertThat(e1.isShutdown()).as("First pool should be shut down").isTrue();
    assertThat(e2.isShutdown()).as("Second pool should be shut down").isTrue();
    assertThat(ManagedThreadPools.getPools()).isEmpty();
  }

  @Test
  public void testShutdownAllIsIdempotent() {
    ThreadPools.newExitingWorkerPool("test-idempotent", 1);

    ManagedThreadPools.shutdownAll();
    assertThat(ManagedThreadPools.getPools()).isEmpty();

    // second call on empty registry must not throw
    ManagedThreadPools.shutdownAll();
    assertThat(ManagedThreadPools.getPools()).isEmpty();
  }
}

