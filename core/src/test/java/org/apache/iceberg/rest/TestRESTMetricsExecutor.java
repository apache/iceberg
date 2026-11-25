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

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class TestRESTMetricsExecutor {

  @Test
  void idleThreadExits() {
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) RESTSessionCatalog.newMetricsExecutor(Duration.ofMillis(10));
    try {
      executor.execute(() -> {});
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(executor.getPoolSize()).isZero());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void runTaskAfterIdleThreadExits() throws Exception {
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) RESTSessionCatalog.newMetricsExecutor(Duration.ofMillis(10));
    try {
      executor.submit(() -> {}).get();
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(executor.getPoolSize()).isZero());
      assertThat(executor.submit(() -> "sent").get(10, TimeUnit.SECONDS)).isEqualTo("sent");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void threadDoesNotInheritThreadLocals() throws Exception {
    InheritableThreadLocal<String> threadLocal = new InheritableThreadLocal<>();
    threadLocal.set("value");
    ExecutorService executor = RESTSessionCatalog.newMetricsExecutor(Duration.ofMinutes(1));
    try {
      assertThat(executor.submit(threadLocal::get).get()).isNull();
    } finally {
      executor.shutdownNow();
      threadLocal.remove();
    }
  }
}
