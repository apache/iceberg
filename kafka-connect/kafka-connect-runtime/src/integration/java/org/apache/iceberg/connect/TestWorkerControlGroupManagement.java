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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.junit.jupiter.api.Test;

/**
 * Regression test: the Worker's control-topic reader used consumer.subscribe() with a
 * single-member, never-reused, randomly-named group (IcebergSinkConfig.DEFAULT_CONTROL_GROUP_PREFIX
 * + UUID.randomUUID()). That group gets no benefit from consumer-group management (it never has
 * more than one member), but is still exposed to the full JoinGroup/SyncGroup protocol -- including
 * broker-side member-fencing failures that can permanently starve a task's commits. The fix is to
 * use consumer.assign() for this reader instead, which never registers a consumer group with the
 * broker at all.
 */
public class TestWorkerControlGroupManagement extends IntegrationTestBase {

  private static final String TEST_TABLE = "control_group_test";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(TEST_DB, TEST_TABLE);

  @Test
  public void testWorkerDoesNotRegisterEphemeralConsumerGroup() throws Exception {
    catalog().createTable(TABLE_IDENTIFIER, TestEvent.TEST_SCHEMA);

    runTest(null, true, ImmutableMap.of(), List.of(TABLE_IDENTIFIER));

    try (Admin admin = context().initLocalAdmin()) {
      Set<String> groupIds =
          admin.listConsumerGroups().all().get().stream()
              .map(ConsumerGroupListing::groupId)
              .collect(Collectors.toSet());

      assertThat(groupIds)
          .as(
              "Worker's control-topic consumer should never register a consumer group "
                  + "(it should use assign(), not subscribe())")
          .noneMatch(id -> id.startsWith(IcebergSinkConfig.DEFAULT_CONTROL_GROUP_PREFIX));
    }
  }

  @Override
  protected KafkaConnectUtils.Config createConfig(boolean useSchema) {
    return createCommonConfig(useSchema)
        .config("iceberg.tables", String.format("%s.%s", TEST_DB, TEST_TABLE));
  }

  @Override
  protected void sendEvents(boolean useSchema) {
    // send two events so both tasks (tasks.max=2, 2 topic partitions) receive at least one
    // record -- otherwise the task that gets zero records never creates its Worker, never
    // sends DataComplete, and the round waits forever (commit.timeout-ms is MAX_VALUE here)
    send(testTopic(), new TestEvent(1, "type1", Instant.now(), "hello world!"), useSchema);
    send(testTopic(), new TestEvent(2, "type2", Instant.now(), "having fun?"), useSchema);
  }

  @Override
  void dropTables() {
    catalog().dropTable(TABLE_IDENTIFIER);
  }
}
