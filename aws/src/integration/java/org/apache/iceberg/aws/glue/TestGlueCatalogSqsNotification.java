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

package org.apache.iceberg.aws.glue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.sqs.SQSListener;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.IncrementalScanEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class TestGlueCatalogSqsNotification extends GlueTestBase {

  private static final SqsClient sqs = clientFactory.sqs();
  private static final DataFile testDataFile = DataFiles.builder(partitionSpec)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withRecordCount(1)
      .build();
  private static final int RETRY = 3;
  private static final long RETRY_INTERVAL_MS = 1000;

  private String queueUrl;

  @Before
  public void before() {
    String queueName = getRandomName();
    sqs.createQueue(CreateQueueRequest.builder()
        .queueName(queueName)
        .build());
    this.queueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder()
        .queueName(queueName)
        .build())
        .queueUrl();
  }

  @After
  public void after() {
    DeleteQueueRequest deleteRequest = DeleteQueueRequest.builder().queueUrl(queueUrl).build();
    sqs.deleteQueue(deleteRequest);
  }

  @Test
  public void testNotifyOnCreateSnapshotEvent() throws IOException {
    Listeners.register(listener(), CreateSnapshotEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();

    List<Message> messages = receiveAllMessages(queueUrl);
    Assert.assertEquals(1, messages.size());

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode bodyNode = objectMapper.readTree(messages.get(0).body());

    String expectedMessage = "{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"operation\":\"append\",\"snapshot-id\":" + table.currentSnapshot().snapshotId() + "," +
            "\"sequence-number\":0,\"summary\":{\"added-data-files\":\"1\"," +
            "\"added-records\":\"1\",\"added-files-size\":\"10\"," +
            "\"changed-partition-count\":\"1\",\"total-records\":\"1\"," +
            "\"total-files-size\":\"10\",\"total-data-files\":\"1\"," +
            "\"total-delete-files\":\"0\",\"total-position-deletes\":\"0\"," +
            "\"total-equality-deletes\":\"0\"}}";

    Assert.assertEquals(expectedMessage, bodyNode.toString());
  }

  @Test
  public void testNotifyOnScanEvent() throws IOException {
    Listeners.register(listener(), ScanEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();
    table.refresh();

    Expression andExpression = Expressions.and(Expressions.equal("c1", "First"), Expressions.equal("c1", "Second"));
    table.newScan().filter(andExpression).planFiles();

    List<Message> messages = receiveAllMessages(queueUrl);
    Assert.assertEquals(1, messages.size());

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode bodyNode = objectMapper.readTree(messages.get(0).body());

    String expectedMessage = "{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"snapshot-id\":" + table.currentSnapshot().snapshotId() + "," +
            "\"expression\":{\"type\":\"and\"," +
            "\"left-operand\":{\"type\":\"unbounded-predicate\"," +
            "\"operation\":\"eq\",\"term\":{\"type\":\"named-reference\",\"value\":\"c1\"}," +
            "\"literals\":[{\"type\":\"string\",\"value\":\"First\"}]}," +
            "\"right-operand\":{\"type\":\"unbounded-predicate\"," +
            "\"operation\":\"eq\",\"term\":{\"type\":\"named-reference\",\"value\":\"c1\"}," +
            "\"literals\":[{\"type\":\"string\",\"value\":\"Second\"}]}}," +
            "\"projection\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c1\"," +
            "\"required\":true,\"type\":\"string\",\"doc\":\"c1\"}]}}";

    Assert.assertEquals(expectedMessage, bodyNode.toString());
  }

  @Test
  public void testNotifyOnIncrementalScan() throws IOException {
    Listeners.register(listener(), IncrementalScanEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();
    table.newAppend().appendFile(testDataFile).commit();
    table.refresh();

    Iterable<Snapshot> snapshots = table.snapshots();
    table.newScan().appendsBetween(
            Iterables.get(snapshots, 0).snapshotId(),
            Iterables.get(snapshots, 1).snapshotId())
            .planFiles();

    List<Message> messages = receiveAllMessages(queueUrl);
    Assert.assertEquals(1, messages.size());

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode bodyNode = objectMapper.readTree(messages.get(0).body());

    String expectedMessage = "{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"from-snapshot-id\":" + Iterables.get(snapshots, 0).snapshotId() + "," +
            "\"to-snapshot-id\":" + Iterables.get(snapshots, 1).snapshotId() + "," +
            "\"expression\":{\"type\":\"true\"}," +
            "\"projection\":{\"type\":\"struct\"," +
            "\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c1\"," +
            "\"required\":true,\"type\":\"string\",\"doc\":\"c1\"}]}}";

    Assert.assertEquals(expectedMessage, bodyNode.toString());
  }

  @Test
  public void testNotifyOnAllEvents() {
    Listeners.register(listener(), CreateSnapshotEvent.class);
    Listeners.register(listener(), ScanEvent.class);
    Listeners.register(listener(), IncrementalScanEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();
    table.newScan().planFiles();

    table.newAppend().appendFile(testDataFile).commit();
    table.refresh();

    Iterable<Snapshot> snapshots = table.snapshots();
    table.newScan().appendsBetween(
            Iterables.get(snapshots, 0).snapshotId(),
            Iterables.get(snapshots, 1).snapshotId())
            .planFiles();

    List<Message> messages = receiveAllMessages(queueUrl);
    Assert.assertEquals(4, messages.size());

    Set<String> actualBodyNodesMessages = messages.stream()
        .map(m -> {
          try {
            return JsonUtil.mapper().readTree(m.body()).toString();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        })
        .collect(Collectors.toSet());

    Set<String> expectedBodyNodesMessages = Sets.newHashSet(
        "{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"snapshot-id\":" + Iterables.get(snapshots, 0).snapshotId() + "," +
            "\"expression\":{\"type\":\"true\"},\"projection\":{\"type\":\"struct\"," +
            "\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c1\"," +
            "\"required\":true,\"type\":\"string\",\"doc\":\"c1\"}]}}",
        "{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"operation\":\"append\",\"snapshot-id\":" + Iterables.get(snapshots, 1).snapshotId() + "," +
            "\"sequence-number\":0,\"summary\":{\"added-data-files\":\"1\",\"added-records\":\"1\"," +
            "\"added-files-size\":\"10\",\"changed-partition-count\":\"1\",\"total-records\":\"2\"," +
            "\"total-files-size\":\"20\",\"total-data-files\":\"2\",\"total-delete-files\":\"0\"," +
            "\"total-position-deletes\":\"0\",\"total-equality-deletes\":\"0\"}}",
        "{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"from-snapshot-id\":" + Iterables.get(snapshots, 0).snapshotId() + "," +
            "\"to-snapshot-id\":" + Iterables.get(snapshots, 1).snapshotId() + "," +
            "\"expression\":{\"type\":\"true\"},\"projection\":{\"type\":\"struct\"," +
            "\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c1\"," +
            "\"required\":true,\"type\":\"string\",\"doc\":\"c1\"}]}}",
        "{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"operation\":\"append\",\"snapshot-id\":" + Iterables.get(snapshots, 0).snapshotId() + "," +
            "\"sequence-number\":0,\"summary\":{\"added-data-files\":\"1\",\"added-records\":\"1\"," +
            "\"added-files-size\":\"10\",\"changed-partition-count\":\"1\",\"total-records\":\"1\"," +
            "\"total-files-size\":\"10\",\"total-data-files\":\"1\",\"total-delete-files\":\"0\"," +
            "\"total-position-deletes\":\"0\",\"total-equality-deletes\":\"0\"}}"
    );

    Assert.assertEquals(expectedBodyNodesMessages, actualBodyNodesMessages);
  }

  public static List<Message> receiveAllMessages(String sqsUrl) {
    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
            .queueUrl(sqsUrl)
            .visibilityTimeout(100)
            .waitTimeSeconds(2)
            .maxNumberOfMessages(10)
            .build();

    List<Message> messages = Lists.newArrayList();
    messages.addAll(sqs.receiveMessage(receiveMessageRequest).messages());
    int prevCounter = -1;
    while (prevCounter != messages.size()) {
      prevCounter = messages.size();
      messages.addAll(sqs.receiveMessage(receiveMessageRequest).messages());
    }

    return messages;
  }

  private <T> Listener<T> listener() {
    return new SQSListener<>(queueUrl, sqs, RETRY, RETRY_INTERVAL_MS);
  }
}
