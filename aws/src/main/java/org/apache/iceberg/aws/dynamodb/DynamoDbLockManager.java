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
package org.apache.iceberg.aws.dynamodb;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LockManagers;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.RequestLimitExceededException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.TransactionConflictException;

/** DynamoDB implementation for the lock manager. */
public class DynamoDbLockManager extends LockManagers.BaseLockManager {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoDbLockManager.class);

  private static final String COL_LOCK_ENTITY_ID = "entityId";
  private static final String COL_LEASE_DURATION_MS = "leaseDurationMs";
  private static final String COL_VERSION = "version";
  private static final String COL_LOCK_OWNER_ID = "ownerId";

  private static final String CONDITION_LOCK_ID_MATCH =
      String.format("%s = :eid AND %s = :oid", COL_LOCK_ENTITY_ID, COL_LOCK_OWNER_ID);
  private static final String CONDITION_LOCK_ENTITY_NOT_EXIST =
      String.format("attribute_not_exists(%s)", COL_LOCK_ENTITY_ID);
  private static final String CONDITION_LOCK_ENTITY_NOT_EXIST_OR_VERSION_MATCH =
      String.format(
          "attribute_not_exists(%s) OR (%s = :eid AND %s = :vid)",
          COL_LOCK_ENTITY_ID, COL_LOCK_ENTITY_ID, COL_VERSION);

  private static final int LOCK_TABLE_CREATION_WAIT_ATTEMPTS_MAX = 5;
  private static final int RELEASE_RETRY_ATTEMPTS_MAX = 5;

  private static final List<KeySchemaElement> LOCK_TABLE_SCHEMA =
      Lists.newArrayList(
          KeySchemaElement.builder()
              .attributeName(COL_LOCK_ENTITY_ID)
              .keyType(KeyType.HASH)
              .build());

  private static final List<AttributeDefinition> LOCK_TABLE_COL_DEFINITIONS =
      Lists.newArrayList(
          AttributeDefinition.builder()
              .attributeName(COL_LOCK_ENTITY_ID)
              .attributeType(ScalarAttributeType.S)
              .build());

  private final Map<String, DynamoDbHeartbeat> heartbeats = Maps.newHashMap();

  private DynamoDbClient dynamo;
  private String lockTableName;

  /** constructor for dynamic initialization, {@link #initialize(Map)} must be called later. */
  public DynamoDbLockManager() {}

  /**
   * constructor used for testing purpose
   *
   * @param dynamo dynamo client
   * @param lockTableName lock table name
   */
  public DynamoDbLockManager(DynamoDbClient dynamo, String lockTableName) {
    super.initialize(Maps.newHashMap());
    this.dynamo = dynamo;
    this.lockTableName = lockTableName;
    ensureLockTableExistsOrCreate();
  }

  private void ensureLockTableExistsOrCreate() {

    if (tableExists(lockTableName)) {
      return;
    }

    LOG.info("Dynamo lock table {} not found, trying to create", lockTableName);
    dynamo.createTable(
        CreateTableRequest.builder()
            .tableName(lockTableName)
            .keySchema(lockTableSchema())
            .attributeDefinitions(lockTableColDefinitions())
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build());

    Tasks.foreach(lockTableName)
        .retry(LOCK_TABLE_CREATION_WAIT_ATTEMPTS_MAX)
        .throwFailureWhenFinished()
        .onlyRetryOn(IllegalStateException.class)
        .run(this::checkTableActive);
  }

  @VisibleForTesting
  boolean tableExists(String tableName) {
    try {
      dynamo.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
      return true;
    } catch (ResourceNotFoundException e) {
      return false;
    }
  }

  private void checkTableActive(String tableName) {
    try {
      DescribeTableResponse response =
          dynamo.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
      TableStatus currentStatus = response.table().tableStatus();
      if (!currentStatus.equals(TableStatus.ACTIVE)) {
        throw new IllegalStateException(
            String.format(
                "Dynamo table %s is not active, current status: %s", tableName, currentStatus));
      }
    } catch (ResourceNotFoundException e) {
      throw new IllegalStateException(String.format("Cannot find Dynamo table %s", tableName));
    }
  }

  @Override
  public void initialize(Map<String, String> properties) {
    super.initialize(properties);
    this.dynamo = AwsClientFactories.from(properties).dynamo();
    this.lockTableName = properties.get(CatalogProperties.LOCK_TABLE);
    Preconditions.checkNotNull(lockTableName, "DynamoDB lock table name must not be null");
    ensureLockTableExistsOrCreate();
  }

  @Override
  public boolean acquire(String entityId, String ownerId) {
    try {
      Tasks.foreach(entityId)
          .throwFailureWhenFinished()
          .retry(Integer.MAX_VALUE - 1)
          .exponentialBackoff(acquireIntervalMs(), acquireIntervalMs(), acquireTimeoutMs(), 1)
          .onlyRetryOn(
              ConditionalCheckFailedException.class,
              ProvisionedThroughputExceededException.class,
              TransactionConflictException.class,
              RequestLimitExceededException.class,
              InternalServerErrorException.class)
          .run(id -> acquireOnce(id, ownerId));
      return true;
    } catch (DynamoDbException e) {
      return false;
    }
  }

  @VisibleForTesting
  void acquireOnce(String entityId, String ownerId) {
    GetItemResponse response =
        dynamo.getItem(
            GetItemRequest.builder()
                .tableName(lockTableName)
                .consistentRead(true)
                .key(toKey(entityId))
                .build());

    if (!response.hasItem()) {
      dynamo.putItem(
          PutItemRequest.builder()
              .tableName(lockTableName)
              .item(toNewItem(entityId, ownerId, heartbeatTimeoutMs()))
              .conditionExpression(CONDITION_LOCK_ENTITY_NOT_EXIST)
              .build());
    } else {
      Map<String, AttributeValue> currentItem = response.item();

      try {
        Thread.sleep(Long.parseLong(currentItem.get(COL_LEASE_DURATION_MS).n()));
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            String.format(
                "Fail to acquire lock %s by %s, interrupted during sleep", entityId, ownerId),
            e);
      }

      dynamo.putItem(
          PutItemRequest.builder()
              .tableName(lockTableName)
              .item(toNewItem(entityId, ownerId, heartbeatTimeoutMs()))
              .conditionExpression(CONDITION_LOCK_ENTITY_NOT_EXIST_OR_VERSION_MATCH)
              .expressionAttributeValues(
                  ImmutableMap.of(
                      ":eid", AttributeValue.builder().s(entityId).build(),
                      ":vid", AttributeValue.builder().s(currentItem.get(COL_VERSION).s()).build()))
              .build());
    }

    startNewHeartbeat(entityId, ownerId);
  }

  private void startNewHeartbeat(String entityId, String ownerId) {
    if (heartbeats.containsKey(entityId)) {
      heartbeats.remove(entityId).cancel();
    }

    DynamoDbHeartbeat heartbeat =
        new DynamoDbHeartbeat(
            dynamo, lockTableName, heartbeatIntervalMs(), heartbeatTimeoutMs(), entityId, ownerId);
    heartbeat.schedule(scheduler());
    heartbeats.put(entityId, heartbeat);
  }

  @Override
  public boolean release(String entityId, String ownerId) {
    boolean succeeded = false;
    DynamoDbHeartbeat heartbeat = heartbeats.get(entityId);
    try {
      Tasks.foreach(entityId)
          .retry(RELEASE_RETRY_ATTEMPTS_MAX)
          .throwFailureWhenFinished()
          .onlyRetryOn(
              ProvisionedThroughputExceededException.class,
              TransactionConflictException.class,
              RequestLimitExceededException.class,
              InternalServerErrorException.class)
          .run(
              id ->
                  dynamo.deleteItem(
                      DeleteItemRequest.builder()
                          .tableName(lockTableName)
                          .key(toKey(id))
                          .conditionExpression(CONDITION_LOCK_ID_MATCH)
                          .expressionAttributeValues(toLockIdValues(id, ownerId))
                          .build()));
      succeeded = true;
    } catch (ConditionalCheckFailedException e) {
      LOG.error(
          "Failed to release lock for entity: {}, owner: {}, lock entity does not exist or owner not match",
          entityId,
          ownerId,
          e);
    } catch (DynamoDbException e) {
      LOG.error(
          "Failed to release lock for entity: {}, owner: {}, encountered unexpected DynamoDB exception",
          entityId,
          ownerId,
          e);
    } finally {
      if (heartbeat != null && heartbeat.ownerId().equals(ownerId)) {
        heartbeat.cancel();
      }
    }

    return succeeded;
  }

  private static Map<String, AttributeValue> toKey(String entityId) {
    return ImmutableMap.of(COL_LOCK_ENTITY_ID, AttributeValue.builder().s(entityId).build());
  }

  private static Map<String, AttributeValue> toNewItem(
      String entityId, String ownerId, long heartbeatTimeoutMs) {
    return ImmutableMap.of(
        COL_LOCK_ENTITY_ID, AttributeValue.builder().s(entityId).build(),
        COL_LOCK_OWNER_ID, AttributeValue.builder().s(ownerId).build(),
        COL_VERSION, AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
        COL_LEASE_DURATION_MS,
            AttributeValue.builder().n(Long.toString(heartbeatTimeoutMs)).build());
  }

  private static Map<String, AttributeValue> toLockIdValues(String entityId, String ownerId) {
    return ImmutableMap.of(
        ":eid", AttributeValue.builder().s(entityId).build(),
        ":oid", AttributeValue.builder().s(ownerId).build());
  }

  @Override
  public void close() throws Exception {
    dynamo.close();
    heartbeats.values().forEach(DynamoDbHeartbeat::cancel);
    heartbeats.clear();

    super.close();
  }

  /**
   * The lock table schema, for users who would like to create the table separately
   *
   * @return lock table schema
   */
  public static List<KeySchemaElement> lockTableSchema() {
    return LOCK_TABLE_SCHEMA;
  }

  /**
   * The lock table column definition, for users who whould like to create the table separately
   *
   * @return lock table column definition
   */
  public static List<AttributeDefinition> lockTableColDefinitions() {
    return LOCK_TABLE_COL_DEFINITIONS;
  }

  private static class DynamoDbHeartbeat implements Runnable {

    private final DynamoDbClient dynamo;
    private final String lockTableName;
    private final long intervalMs;
    private final long timeoutMs;
    private final String entityId;
    private final String ownerId;
    private ScheduledFuture<?> future;

    DynamoDbHeartbeat(
        DynamoDbClient dynamo,
        String lockTableName,
        long intervalMs,
        long timeoutMs,
        String entityId,
        String ownerId) {
      this.dynamo = dynamo;
      this.lockTableName = lockTableName;
      this.intervalMs = intervalMs;
      this.timeoutMs = timeoutMs;
      this.entityId = entityId;
      this.ownerId = ownerId;
      this.future = null;
    }

    @Override
    public void run() {
      try {
        dynamo.putItem(
            PutItemRequest.builder()
                .tableName(lockTableName)
                .item(toNewItem(entityId, ownerId, timeoutMs))
                .conditionExpression(CONDITION_LOCK_ID_MATCH)
                .expressionAttributeValues(toLockIdValues(entityId, ownerId))
                .build());
      } catch (ConditionalCheckFailedException e) {
        LOG.error(
            "Fail to heartbeat for entity: {}, owner: {} due to conditional check failure, "
                + "unsafe concurrent commits might be going on",
            entityId,
            ownerId,
            e);
      } catch (RuntimeException e) {
        LOG.error("Failed to heartbeat for entity: {}, owner: {}", entityId, ownerId, e);
      }
    }

    public String ownerId() {
      return ownerId;
    }

    public void schedule(ScheduledExecutorService scheduler) {
      future = scheduler.scheduleAtFixedRate(this, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void cancel() {
      if (future != null) {
        future.cancel(false);
      }
    }
  }
}
