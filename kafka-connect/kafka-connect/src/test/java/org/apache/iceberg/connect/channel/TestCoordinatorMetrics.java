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
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;

public class TestCoordinatorMetrics {

  private static final String CONNECTOR = "test-connector";

  @Test
  public void testRegistersAndUnregistersMBeans() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName query =
        new ObjectName(
            "iceberg-kafka-connect-metrics:type=coordinator-metrics,connector=" + CONNECTOR);

    AtomicLong commitSize = new AtomicLong();
    AtomicLong readySize = new AtomicLong();
    try (CoordinatorMetrics metrics =
        new CoordinatorMetrics(CONNECTOR, commitSize::get, readySize::get)) {
      Set<ObjectName> registered = server.queryNames(query, null);
      assertThat(registered).isNotEmpty();
    }
    Set<ObjectName> afterClose = server.queryNames(query, null);
    assertThat(afterClose).isEmpty();
  }

  @Test
  public void testTimingMetrics() throws Exception {
    try (CoordinatorMetrics metrics = new CoordinatorMetrics(CONNECTOR, () -> 0L, () -> 0L)) {
      metrics.recordCommit(50);
      metrics.recordCommit(150);

      assertThat(readDouble("commit-time-avg")).isEqualTo(100.0);
      assertThat(readDouble("commit-time-max")).isEqualTo(150.0);
      assertThat(readDouble("commit-time-total")).isEqualTo(200.0);

      metrics.recordConsume(5);
      metrics.recordConsume(15);
      assertThat(readDouble("consume-available-time-avg")).isEqualTo(10.0);
      assertThat(readDouble("consume-available-time-max")).isEqualTo(15.0);
      assertThat(readDouble("consume-available-time-total")).isEqualTo(20.0);
    }
  }

  @Test
  public void testCounters() throws Exception {
    try (CoordinatorMetrics metrics = new CoordinatorMetrics(CONNECTOR, () -> 0L, () -> 0L)) {
      metrics.incStartCommit();
      metrics.incStartCommit();
      metrics.incStartCommit();
      metrics.incCommitComplete();

      assertThat(readDouble("start-commit-total")).isEqualTo(3.0);
      assertThat(readDouble("commit-complete-total")).isEqualTo(1.0);
    }
  }

  @Test
  public void testGaugesReadLazily() throws Exception {
    AtomicLong commitSize = new AtomicLong(0);
    AtomicLong readySize = new AtomicLong(0);
    try (CoordinatorMetrics metrics =
        new CoordinatorMetrics(CONNECTOR, commitSize::get, readySize::get)) {

      assertThat(readLong("commit-buffer-size")).isEqualTo(0L);
      assertThat(readLong("ready-buffer-size")).isEqualTo(0L);

      commitSize.set(7);
      readySize.set(3);
      assertThat(readLong("commit-buffer-size")).isEqualTo(7L);
      assertThat(readLong("ready-buffer-size")).isEqualTo(3L);

      commitSize.set(0);
      readySize.set(0);
      assertThat(readLong("commit-buffer-size")).isEqualTo(0L);
      assertThat(readLong("ready-buffer-size")).isEqualTo(0L);
    }
  }

  @Test
  public void testCloseAllowsReregistration() throws Exception {
    try (CoordinatorMetrics first = new CoordinatorMetrics(CONNECTOR, () -> 0L, () -> 0L)) {
      assertThat(first).isNotNull();
    }
    try (CoordinatorMetrics second = new CoordinatorMetrics(CONNECTOR, () -> 0L, () -> 0L)) {
      assertThat(second).isNotNull();
    }
  }

  private static double readDouble(String name) throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName =
        new ObjectName(
            "iceberg-kafka-connect-metrics:type=coordinator-metrics,connector=" + CONNECTOR);
    return ((Number) server.getAttribute(objectName, name)).doubleValue();
  }

  private static long readLong(String name) throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName =
        new ObjectName(
            "iceberg-kafka-connect-metrics:type=coordinator-metrics,connector=" + CONNECTOR);
    return ((Number) server.getAttribute(objectName, name)).longValue();
  }
}
