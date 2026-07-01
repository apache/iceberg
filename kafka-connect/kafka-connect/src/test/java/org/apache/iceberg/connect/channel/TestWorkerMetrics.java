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
package org.apache.iceberg.connect.channel;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;

public class TestWorkerMetrics {

  private static final String CONNECTOR = "test-connector";
  private static final String TASK_ID = "test-connector-7";

  @Test
  public void testRegistersAndUnregistersMBeans() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName query =
        new ObjectName(
            "iceberg-kafka-connect-metrics:type=worker-metrics,connector="
                + CONNECTOR
                + ",task="
                + TASK_ID);

    try (WorkerMetrics metrics = new WorkerMetrics(CONNECTOR, TASK_ID)) {
      Set<ObjectName> registered = server.queryNames(query, null);
      assertThat(registered).isNotEmpty();
    }

    Set<ObjectName> afterClose = server.queryNames(query, null);
    assertThat(afterClose).isEmpty();
  }

  @Test
  public void testTimingMetrics() throws Exception {
    try (WorkerMetrics metrics = new WorkerMetrics(CONNECTOR, "task-timing")) {
      metrics.recordSave(100);
      metrics.recordSave(200);

      assertThat(readDouble("worker-metrics", "save-time-avg", "task-timing")).isEqualTo(150.0);
      assertThat(readDouble("worker-metrics", "save-time-max", "task-timing")).isEqualTo(200.0);
      assertThat(readDouble("worker-metrics", "save-time-total", "task-timing")).isEqualTo(300.0);

      metrics.recordConsume(10);
      metrics.recordConsume(30);

      assertThat(readDouble("worker-metrics", "consume-available-time-avg", "task-timing"))
          .isEqualTo(20.0);
      assertThat(readDouble("worker-metrics", "consume-available-time-max", "task-timing"))
          .isEqualTo(30.0);
      assertThat(readDouble("worker-metrics", "consume-available-time-total", "task-timing"))
          .isEqualTo(40.0);
    }
  }

  @Test
  public void testCounters() throws Exception {
    try (WorkerMetrics metrics = new WorkerMetrics(CONNECTOR, "task-counter")) {
      metrics.incDataWritten(3);
      metrics.incDataWritten(2);
      metrics.incDataComplete();
      metrics.incDataComplete();

      assertThat(readDouble("worker-metrics", "data-written-total", "task-counter")).isEqualTo(5.0);
      assertThat(readDouble("worker-metrics", "data-complete-total", "task-counter"))
          .isEqualTo(2.0);
    }
  }

  @Test
  public void testCloseAllowsReregistration() throws Exception {
    String task = "task-reregister";
    try (WorkerMetrics first = new WorkerMetrics(CONNECTOR, task)) {
      assertThat(first).isNotNull();
    }
    try (WorkerMetrics second = new WorkerMetrics(CONNECTOR, task)) {
      assertThat(second).isNotNull();
    }
  }

  private static double readDouble(String group, String name, String taskId) throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName =
        new ObjectName(
            "iceberg-kafka-connect-metrics:type="
                + group
                + ",connector="
                + CONNECTOR
                + ",task="
                + taskId);
    return ((Number) server.getAttribute(objectName, name)).doubleValue();
  }
}
