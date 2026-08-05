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
import java.util.UUID;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;

public class TestWorkerMetrics {

  private final String connector = "test-connector-" + UUID.randomUUID();
  private final String taskId = connector + "-7";

  @Test
  public void testRegistersAndUnregistersMBeans() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName query =
        new ObjectName(
            "iceberg.kafka.connect:type=worker-metrics,connector=" + connector + ",task=" + taskId);

    try (WorkerMetrics metrics = new WorkerMetrics(connector, taskId)) {
      Set<ObjectName> registered = server.queryNames(query, null);
      assertThat(registered).isNotEmpty();
    }

    Set<ObjectName> afterClose = server.queryNames(query, null);
    assertThat(afterClose).isEmpty();
  }

  @Test
  public void testTimingMetrics() throws Exception {
    try (WorkerMetrics metrics = new WorkerMetrics(connector, taskId)) {
      metrics.recordSave(100);
      metrics.recordSave(200);

      assertThat(readDouble("save-time-avg")).isEqualTo(150.0);
      assertThat(readDouble("save-time-max")).isEqualTo(200.0);
      assertThat(readDouble("save-time-total")).isEqualTo(300.0);
      assertThat(readDouble("save-time-count")).isEqualTo(2.0);

      metrics.recordMessageRead(10);
      metrics.recordMessageRead(30);

      assertThat(readDouble("channel-message-read-time-avg")).isEqualTo(20.0);
      assertThat(readDouble("channel-message-read-time-max")).isEqualTo(30.0);
      assertThat(readDouble("channel-message-read-time-total")).isEqualTo(40.0);

      metrics.recordMessageProcess(5);
      metrics.recordMessageProcess(15);

      assertThat(readDouble("channel-message-process-time-avg")).isEqualTo(10.0);
      assertThat(readDouble("channel-message-process-time-max")).isEqualTo(15.0);
      assertThat(readDouble("channel-message-process-time-total")).isEqualTo(20.0);
    }
  }

  @Test
  public void testCounters() throws Exception {
    try (WorkerMetrics metrics = new WorkerMetrics(connector, taskId)) {
      metrics.incDataWritten(3);
      metrics.incDataWritten(2);
      // empty poll cycle still records 0 so data-written and data-complete stay comparable
      metrics.incDataWritten(0);
      metrics.incDataComplete();
      metrics.incDataComplete();

      assertThat(readDouble("data-written-total")).isEqualTo(5.0);
      assertThat(readDouble("data-complete-total")).isEqualTo(2.0);
    }
  }

  @Test
  public void testCloseAllowsReregistration() throws Exception {
    try (WorkerMetrics first = new WorkerMetrics(connector, taskId)) {
      assertThat(first).isNotNull();
    }
    try (WorkerMetrics second = new WorkerMetrics(connector, taskId)) {
      assertThat(second).isNotNull();
    }
  }

  private double readDouble(String name) throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName =
        new ObjectName(
            "iceberg.kafka.connect:type=worker-metrics,connector=" + connector + ",task=" + taskId);
    return ((Number) server.getAttribute(objectName, name)).doubleValue();
  }
}
