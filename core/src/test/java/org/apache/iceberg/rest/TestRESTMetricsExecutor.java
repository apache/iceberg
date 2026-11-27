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

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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

  @Test
  void threadUsesIcebergClassLoader() throws Exception {
    // Load the catalog through a non-system class loader, like how Apache Spark does for
    // '--jars' and '--packages'. Otherwise, the thread falls back to the system class loader.
    List<URL> classpath = Lists.newArrayList();
    for (String entry :
        Splitter.on(File.pathSeparator).split(System.getProperty("java.class.path"))) {
      classpath.add(new File(entry).toURI().toURL());
    }

    try (URLClassLoader icebergLoader =
        new URLClassLoader(classpath.toArray(new URL[0]), ClassLoader.getPlatformClassLoader())) {
      Method newMetricsExecutor =
          Class.forName(RESTSessionCatalog.class.getName(), true, icebergLoader)
              .getDeclaredMethod("newMetricsExecutor", Duration.class);
      newMetricsExecutor.setAccessible(true);
      ExecutorService executor =
          (ExecutorService) newMetricsExecutor.invoke(null, Duration.ofMinutes(1));
      try {
        assertThat(executor.submit(() -> Thread.currentThread().getContextClassLoader()).get())
            .isSameAs(icebergLoader);
      } finally {
        executor.shutdownNow();
      }
    }
  }
}
