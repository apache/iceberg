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
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;

public class TestCoordinatorMetrics {

  private final String connector = "test-connector-" + UUID.randomUUID();

  @Test
  public void testRegistersAndUnregistersMBeans() throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    ObjectName query =
        new ObjectName(
            "iceberg.kafka.connect:type=coordinator-metrics,connector=" + connector + ",*");

    AtomicLong commitSize = new AtomicLong();
    AtomicLong readySize = new AtomicLong();
    try (CoordinatorMetrics metrics =
        new CoordinatorMetrics(connector, commitSize::get, readySize::get)) {
      Set<ObjectName> registered = server.queryNames(query, null);
      assertThat(registered).isNotEmpty();
    }
    Set<ObjectName> afterClose = server.queryNames(query, null);
    assertThat(afterClose).isEmpty();
  }

  @Test
  public void testTimingMetrics() throws Exception {
    try (CoordinatorMetrics metrics = new CoordinatorMetrics(connector, () -> 0L, () -> 0L)) {
      metrics.recordCommit(false, 50);
      metrics.recordCommit(false, 150);

      assertThat(readCommitDouble("full", "commit-time-avg")).isEqualTo(100.0);
      assertThat(readCommitDouble("full", "commit-time-max")).isEqualTo(150.0);
      assertThat(readCommitDouble("full", "commit-time-total")).isEqualTo(200.0);
      assertThat(readCommitDouble("full", "commit-time-count")).isEqualTo(2.0);

      metrics.recordCommit(true, 20);
      assertThat(readCommitDouble("partial", "commit-time-total")).isEqualTo(20.0);
      // partial commits do not pollute the full-commit total
      assertThat(readCommitDouble("full", "commit-time-total")).isEqualTo(200.0);

      metrics.recordMessageRead(5);
      metrics.recordMessageRead(15);
      assertThat(readDouble("channel-message-read-time-avg")).isEqualTo(10.0);
      assertThat(readDouble("channel-message-read-time-max")).isEqualTo(15.0);
      assertThat(readDouble("channel-message-read-time-total")).isEqualTo(20.0);
    }
  }

  @Test
  public void testCounters() throws Exception {
    try (CoordinatorMetrics metrics = new CoordinatorMetrics(connector, () -> 0L, () -> 0L)) {
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
        new CoordinatorMetrics(connector, commitSize::get, readySize::get)) {

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
    try (CoordinatorMetrics first = new CoordinatorMetrics(connector, () -> 0L, () -> 0L)) {
      assertThat(first).isNotNull();
    }
    try (CoordinatorMetrics second = new CoordinatorMetrics(connector, () -> 0L, () -> 0L)) {
      assertThat(second).isNotNull();
    }
  }

  private double readDouble(String name) throws Exception {
    return ((Number) attribute(connectorName(), name)).doubleValue();
  }

  private long readLong(String name) throws Exception {
    return ((Number) attribute(connectorName(), name)).longValue();
  }

  private double readCommitDouble(String commitMode, String name) throws Exception {
    return ((Number) attribute(connectorName() + ",commitMode=" + commitMode, name)).doubleValue();
  }

  private String connectorName() {
    return "iceberg.kafka.connect:type=coordinator-metrics,connector="
        + connector
        + ",task=coordinator";
  }

  private static Object attribute(String objectName, String name) throws Exception {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    return server.getAttribute(new ObjectName(objectName), name);
  }
}
