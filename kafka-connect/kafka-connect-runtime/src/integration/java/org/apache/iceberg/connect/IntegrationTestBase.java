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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public class IntegrationTestBase {

  private static TestContext context;

  private Catalog catalog;
  private Admin admin;
  private String connectorName;
  private String testTopic;

  private KafkaProducer<String, String> producer;

  protected static final int TEST_TOPIC_PARTITIONS = 2;

  protected TestContext context() {
    return context;
  }

  protected Catalog catalog() {
    return catalog;
  }

  protected String connectorName() {
    return connectorName;
  }

  protected String testTopic() {
    return testTopic;
  }

  @BeforeAll
  public static void baseBeforeAll() {
    context = TestContext.instance();
  }

  @BeforeEach
  public void baseBefore() {
    this.catalog = context.initLocalCatalog();
    this.producer = context.initLocalProducer();
    this.admin = context.initLocalAdmin();
    this.connectorName = "test_connector-" + UUID.randomUUID();
    this.testTopic = "test-topic-" + UUID.randomUUID();
  }

  @AfterEach
  public void baseAfter() {
    try {
      if (catalog instanceof AutoCloseable) {
        ((AutoCloseable) catalog).close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    producer.close();
    admin.close();
  }

  protected void assertSnapshotProps(TableIdentifier tableIdentifier, String branch) {
    Table table = catalog.loadTable(tableIdentifier);
    Map<String, String> props = latestSnapshot(table, branch).summary();
    assertThat(props)
        .hasKeySatisfying(
            new Condition<>() {
              @Override
              public boolean matches(String str) {
                return str.startsWith("kafka.connect.offsets.");
              }
            });
    assertThat(props).containsKey("kafka.connect.commit-id");
  }

  protected List<DataFile> dataFiles(TableIdentifier tableIdentifier, String branch) {
    Table table = catalog.loadTable(tableIdentifier);
    return Lists.newArrayList(latestSnapshot(table, branch).addedDataFiles(table.io()));
  }

  protected List<DeleteFile> deleteFiles(TableIdentifier tableIdentifier, String branch) {
    Table table = catalog.loadTable(tableIdentifier);
    return Lists.newArrayList(latestSnapshot(table, branch).addedDeleteFiles(table.io()));
  }

  private Snapshot latestSnapshot(Table table, String branch) {
    return branch == null ? table.currentSnapshot() : table.snapshot(branch);
  }

  protected void createTopic(String topicName, int partitions) {
    try {
      admin
          .createTopics(ImmutableList.of(new NewTopic(topicName, partitions, (short) 1)))
          .all()
          .get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  protected void deleteTopic(String topicName) {
    try {
      admin.deleteTopics(ImmutableList.of(topicName)).all().get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  protected void send(String topicName, TestEvent event, boolean useSchema) {
    String eventStr = event.serialize(useSchema);
    try {
      producer.send(new ProducerRecord<>(topicName, Long.toString(event.id()), eventStr)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  protected void flush() {
    producer.flush();
  }
}
