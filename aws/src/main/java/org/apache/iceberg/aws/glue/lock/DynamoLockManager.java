/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.aws.glue.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.iceberg.aws.glue.util.AWSGlueConfig;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

/**
 * A DynamoDB implementation of Hive's lock interface.
 * We use 2 tables, a lockRequest table and a lockComponent table to keep track of all the locks.
 * The lockRequest table has the following schema:
 *   - hash key: lockId (String) - a random long number
 *   - values: a list of LockComponents, lastHeartbeatMillis
 *       and other input info such as request user, host, agentInfo, txnId for debug purpose
 * The lockComponent table has the following schema:
 *   - hash key: tableId (String) - databaseName|tableName
 *   - range key: partitionId (String) - partitionName|lockType
 *   - values: lockId, lastHeartbeatMillis
 * If the expected tables do not exist, we will create the tables and set billing mode to PAY_PER_REQUEST by default.
 * For production systems, it is recommended to change to provisioned throughput to ensure performance.
 *
 * By the nature of DynamoDB, we can not efficiently achieve transaction isolation between different callers.
 * In addition, the current implementation only supports exclusive locks.
 * However, we consider such implementation sufficient because Iceberg currently only requests exclusive table locks.
 * We will add support for shared read and write locks if necessary.
 * For people seeking a true lock impl, we can also implement an AuroraServerlessLockManager.
 * But in the end we expect Glue to someday provide such support natively.
 */
public class DynamoLockManager implements LockManager {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoLockManager.class);

  private static final String LOCK_ID_COL = "lockId";
  private static final String COMPONENTS_COL = "components";
  private static final String USER_COL = "user";
  private static final String HOST_COL = "host";
  private static final String AGENT_INFO_COL = "agentInfo";
  private static final String TXN_ID_COL = "txnId";
  private static final String TABLE_ID_COL = "tableId";
  private static final String PARTITION_ID_COL = "partitionId";
  private static final String LAST_HEARTBEAT_MILLIS_COL = "lastHeartbeatMillis";

  private static final String GLOBAL_LOCK_MARKER = "__GLOBAL_LOCK__";

  // schema and definitions are made public in case people want to create the tables separately
  public static final List<KeySchemaElement> LOCK_REQUEST_TABLE_SCHEMA = Lists.newArrayList(
      KeySchemaElement.builder()
          .attributeName(LOCK_ID_COL)
          .keyType(KeyType.HASH)
          .build()
  );

  public static final List<AttributeDefinition> LOCK_REQUEST_TABLE_COL_DEFINITIONS = Lists.newArrayList(
      AttributeDefinition.builder()
          .attributeName(LOCK_ID_COL)
          .attributeType(ScalarAttributeType.S)
          .build()
  );

  public static final List<KeySchemaElement> LOCK_COMPONENT_TABLE_SCHEMA = Lists.newArrayList(
      KeySchemaElement.builder()
          .attributeName(TABLE_ID_COL)
          .keyType(KeyType.HASH)
          .build(),
      KeySchemaElement.builder()
          .attributeName(PARTITION_ID_COL)
          .keyType(KeyType.RANGE)
          .build()
  );

  public static final List<AttributeDefinition> LOCK_COMPONENT_TABLE_COL_DEFINITIONS = Lists.newArrayList(
      AttributeDefinition.builder()
          .attributeName(TABLE_ID_COL)
          .attributeType(ScalarAttributeType.S)
          .build(),
      AttributeDefinition.builder()
          .attributeName(PARTITION_ID_COL)
          .attributeType(ScalarAttributeType.S)
          .build()
  );

  private final String requestTableName;
  private final String componentTableName;
  private final int componentLockReleaseRetryMax;
  private final long waitIntervalMillis;
  private final long timeoutMillis;
  private final DynamoDbClient dynamo;

  public DynamoLockManager(Configuration conf) {
    this(conf, DynamoDbClient.create());
  }

  DynamoLockManager(Configuration conf, DynamoDbClient dynamo) {
    this.dynamo = dynamo;
    this.requestTableName = conf.get(
        AWSGlueConfig.AWS_GLUE_LOCK_REQUEST_DYNAMO_TABLE_NAME,
        AWSGlueConfig.AWS_GLUE_LOCK_REQUEST_DYNAMO_TABLE_NAME_DEFAULT);
    this.componentTableName = conf.get(
        AWSGlueConfig.AWS_GLUE_LOCK_COMPONENT_DYNAMO_TABLE_NAME,
        AWSGlueConfig.AWS_GLUE_LOCK_COMPONENT_DYNAMO_TABLE_NAME_DEFAULT);
    this.componentLockReleaseRetryMax = conf.getInt(
        AWSGlueConfig.AWS_GLUE_LOCK_RELEASE_RETRY_MAX,
        AWSGlueConfig.AWS_GLUE_LOCK_RELEASE_RETRY_MAX_DEFAULT);
    this.waitIntervalMillis = conf.getLong(
        AWSGlueConfig.AWS_GLUE_LOCK_WAIT_INTERVAL_MILLIS,
        AWSGlueConfig.AWS_GLUE_LOCK_WAIT_INTERVAL_MILLIS_DEFAULT);
    this.timeoutMillis = conf.getLong(
        AWSGlueConfig.AWS_GLUE_LOCK_TIMEOUT_MILLIS,
        AWSGlueConfig.AWS_GLUE_LOCK_TIMEOUT_MILLIS_DEFAULT);
    if (conf.getBoolean(
        AWSGlueConfig.AWS_GLUE_LOCK_DYNAMO_INITIALIZE_TABLES,
        AWSGlueConfig.AWS_GLUE_LOCK_DYNAMO_INITIALIZE_TABLES_DEFAULT)) {
      ensureTableExists(requestTableName, LOCK_REQUEST_TABLE_SCHEMA, LOCK_REQUEST_TABLE_COL_DEFINITIONS);
      ensureTableExists(componentTableName, LOCK_COMPONENT_TABLE_SCHEMA, LOCK_COMPONENT_TABLE_COL_DEFINITIONS);
    }
  }

  /**
   * try lock given the lock request.
   * We first add an entry in lockRequest table with a random lockId
   * and then try to acquire lock for each lockComponent
   * @param lockRequest lock request
   * @return lock response of lock id and status
   */
  @Override
  public LockResponse lock(LockRequest lockRequest) {
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(lockRequest.getComponent()),
            "there is no component to lock");
    String lockId = generateRandomLongId();
    List<DynamoLockComponent> dynamoLockComponents = lockRequest.getComponent().stream()
        .map(DynamoLockComponent::fromHive)
        .collect(Collectors.toList());
    insertLockRequest(lockRequest, dynamoLockComponents, lockId);
    return tryLock(dynamoLockComponents, lockId);
  }

  /**
   * check lock status
   * @param lockId lock id
   * @return lock response with id and status
   */
  @Override
  public LockResponse checkLock(long lockId) {
    String lockIdStr = Long.toString(lockId);
    List<DynamoLockComponent> components = getLockComponents(lockIdStr);
    if (components.isEmpty()) {
      LockResponse response = new LockResponse();
      response.setLockid(lockId);
      response.setState(LockState.NOT_ACQUIRED);
      return response;
    } else {
      return tryLock(getLockComponents(lockIdStr), lockIdStr);
    }
  }

  /**
   * unlock
   * @param lockId lock id
   */
  @Override
  public void unlock(long lockId) {
    String lockIdStr = Long.toString(lockId);
    List<DynamoLockComponent> components = getLockComponents(lockIdStr);
    if (!components.isEmpty()) {
      forceReleaseAllComponentLocks(components, lockIdStr);
      deleteLockRequest(lockIdStr);
    }
  }

  private LockResponse tryLock(List<DynamoLockComponent> dynamoLockComponents, String lockId) {
    LockResponse response = new LockResponse();
    response.setLockid(Long.parseLong(lockId));
    List<DynamoLockComponent> succeededComponents = tryBatchAcquireComponentLocks(dynamoLockComponents, lockId);
    if (succeededComponents.size() < dynamoLockComponents.size()) {
      forceReleaseAllComponentLocks(succeededComponents, lockId);
      response.setState(LockState.WAITING);
    } else {
      response.setState(LockState.ACQUIRED);
    }
    return response;
  }

  private void insertLockRequest(LockRequest lockRequest, List<DynamoLockComponent> lockComponents, String lockId) {
    Map<String, AttributeValue> requestItem = new HashMap<>();
    requestItem.put(LOCK_ID_COL, AttributeValue.builder().s(lockId).build());
    requestItem.put(COMPONENTS_COL, AttributeValue.builder().l(lockComponents.stream()
        .map(c -> AttributeValue.builder().s(c.toString()).build())
        .collect(Collectors.toList())
    ).build());
    requestItem.put(USER_COL, AttributeValue.builder().s(lockRequest.getUser()).build());
    requestItem.put(HOST_COL, AttributeValue.builder().s(lockRequest.getHostname()).build());
    requestItem.put(AGENT_INFO_COL, AttributeValue.builder().s(lockRequest.getAgentInfo()).build());
    requestItem.put(TXN_ID_COL, AttributeValue.builder().n(Long.toString(lockRequest.getTxnid())).build());
    requestItem.put(LAST_HEARTBEAT_MILLIS_COL,
            AttributeValue.builder().n(Long.toString(System.currentTimeMillis())).build());
    dynamo.putItem(PutItemRequest.builder()
        .tableName(requestTableName)
        .item(requestItem)
        .build());
  }

  private List<DynamoLockComponent> getLockComponents(String lockId) {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put(LOCK_ID_COL, AttributeValue.builder().s(lockId).build());
    GetItemResponse response = dynamo.getItem(GetItemRequest.builder()
        .tableName(requestTableName)
        .key(key)
        .build());
    List<DynamoLockComponent> result = new ArrayList<>();
    if (response.hasItem()) {
      Map<String, AttributeValue> item = response.item();
      AttributeValue value = item.get(COMPONENTS_COL);
      for (AttributeValue v : value.l()) {
        result.add(DynamoLockComponent.fromJson(v.s()));
      }
      long lastHeartbeat = Long.parseLong(item.get(LAST_HEARTBEAT_MILLIS_COL).n());
      if (lastHeartbeat + timeoutMillis > System.currentTimeMillis()) {
        // update access heartbeat
        Map<String, AttributeValue> newItem = new HashMap<>(item);
        newItem.put(LAST_HEARTBEAT_MILLIS_COL,
                AttributeValue.builder().n(Long.toString(System.currentTimeMillis())).build());
        dynamo.putItem(PutItemRequest.builder()
                .tableName(requestTableName)
                .item(newItem)
                .build());
      } else {
        // delete old lock
        Map<String, AttributeValue> deleteKey = new HashMap<>();
        deleteKey.put(LOCK_ID_COL, item.get(LOCK_ID_COL));
        dynamo.deleteItem(DeleteItemRequest.builder()
                .tableName(requestTableName)
                .key(deleteKey)
                .build());
        forceReleaseAllComponentLocks(result, lockId);
        result.clear();
      }
    }
    return result;
  }

  private void deleteLockRequest(String lockId) {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put(LOCK_ID_COL, AttributeValue.builder().s(lockId).build());
    dynamo.deleteItem(DeleteItemRequest.builder()
        .tableName(requestTableName)
        .key(key)
        .build());
  }

  private boolean tryAcquireComponentLock(DynamoLockComponent component, String lockId) {
    Map<String, AttributeValue> componentItem = new HashMap<>();
    componentItem.put(TABLE_ID_COL, AttributeValue.builder().s(getTableId(component)).build());
    componentItem.put(PARTITION_ID_COL, AttributeValue.builder().s(getPartitionId(component)).build());
    componentItem.put(LOCK_ID_COL, AttributeValue.builder().s(lockId).build());
    componentItem.put(LAST_HEARTBEAT_MILLIS_COL, AttributeValue.builder().n(
        Long.toString(System.currentTimeMillis())
    ).build());

    try {
      Map<String, AttributeValue> expressionValues = new HashMap<>();
      expressionValues.put(":lid", AttributeValue.builder().s(lockId).build());
      expressionValues.put(":ts", AttributeValue.builder().n(Long.toString(
          System.currentTimeMillis() - timeoutMillis
      )).build());

      dynamo.putItem(PutItemRequest.builder()
          .tableName(componentTableName)
          .item(componentItem)
          // if there is no lock, or the lock is already acquired and heartbeat is not passed
          // this also refreshes the heartbeat
          .conditionExpression("attribute_not_exists(" +
              LOCK_ID_COL + ") OR (" +
              LOCK_ID_COL + " = :lid AND " +
              LAST_HEARTBEAT_MILLIS_COL + " > :ts)")
          .expressionAttributeValues(expressionValues)
          .build());
      return true;
    } catch (Exception e) {
      // most likely it's ConditionalCheckFailedException, but acquisition can fail for any exception
      LOG.debug("Acquiring lock {} for {} failed", component, lockId, e);
      return false;
    }
  }

  /**
   * batch acquire component locks in the given order.
   * Acquisition process won't continue if one acquisition fails.
   * @param components components to lock
   * @param lockId lock id
   * @return succeeded locks
   */
  private List<DynamoLockComponent> tryBatchAcquireComponentLocks(
      List<DynamoLockComponent> components, String lockId) {
    List<DynamoLockComponent> succeedLocks = new ArrayList<>();
    for (DynamoLockComponent component : components) {
      if (tryAcquireComponentLock(component, lockId)) {
        succeedLocks.add(component);
      } else {
        break;
      }
    }
    return succeedLocks;
  }

  private boolean tryReleaseComponentLock(DynamoLockComponent component, String lockId) {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put(TABLE_ID_COL, AttributeValue.builder().s(getTableId(component)).build());
    key.put(PARTITION_ID_COL, AttributeValue.builder().s(getPartitionId(component)).build());
    Map<String, AttributeValue> expressionValues = new HashMap<>();
    expressionValues.put(":lid", AttributeValue.builder().s(lockId).build());
    expressionValues.put(":ts", AttributeValue.builder().n(Long.toString(
        System.currentTimeMillis() - timeoutMillis
    )).build());
    try {
      dynamo.deleteItem(DeleteItemRequest.builder()
          .tableName(componentTableName)
          .key(key)
          // if lock id is correct, or if there is any expired lock
          .conditionExpression(LOCK_ID_COL + " = :lid OR " +
              LAST_HEARTBEAT_MILLIS_COL + " < :ts")
          .expressionAttributeValues(expressionValues)
          .build());
      return true;
    } catch (ConditionalCheckFailedException e) {
      // some other process has the lock, or the lock does not exist, no need to retry
      LOG.debug("Acquiring lock {} for {} failed due to conditional check", component, lockId, e);
      return true;
    } catch (Exception e) {
      // all the other exceptions mean unlock failed and should retry
      LOG.debug("Release lock {} for {} failed unexpectedly", component, lockId, e);
      return false;
    }
  }

  /**
   * Force to release all the component locks in the given list.
   * We will try to unlock until the configured max retry is exceeded.
   * @param components components
   * @param lockId lock id
   */
  private void forceReleaseAllComponentLocks(List<DynamoLockComponent> components, String lockId) {
    for (DynamoLockComponent component : components) {
      int retry = componentLockReleaseRetryMax;
      boolean succeeded = false;
      while (retry-- > 0) {
        try {
          succeeded = tryReleaseComponentLock(component, lockId);
          if (succeeded) {
            break;
          }
          Thread.sleep(waitIntervalMillis);
        } catch (InterruptedException e) {
          LOG.debug("interrupted when forcing release of component {} for lock {}", component, lockId, e);
        }
      }
      if (!succeeded) {
        throw new InconsistentLockStateException(component, lockId, componentLockReleaseRetryMax);
      }
    }
  }

  private String generateRandomLongId() {
    return Long.toString(ThreadLocalRandom.current().nextLong());
  }

  private String getTableId(DynamoLockComponent component) {
    String tableName = component.getLockLevel().equals(LockLevel.DB) ? GLOBAL_LOCK_MARKER : component.getTableName();
    return String.format("%s|%s", component.getDbName(), tableName);
  }

  private String getPartitionId(DynamoLockComponent component) {
    String partitionName = component.getPartitionName() == null ? GLOBAL_LOCK_MARKER : component.getPartitionName();
    return String.format("%s|%s", partitionName, component.getLockType());
  }

  private void ensureTableExists(
      String tableName,
      List<KeySchemaElement> schema,
      List<AttributeDefinition> definitions) {
    try {
      dynamo.describeTable(DescribeTableRequest.builder()
          .tableName(tableName)
          .build());
    } catch (ResourceNotFoundException e) {
      LOG.info("Glue lock DynamoDB table <{}> not found, try to create", tableName);
      dynamo.createTable(CreateTableRequest.builder()
          .tableName(tableName)
          .keySchema(schema)
          .attributeDefinitions(definitions)
          .billingMode(BillingMode.PAY_PER_REQUEST)
          .build());

      boolean isTableActive = false;
      while (!isTableActive) {
        LOG.info("waiting for table <{}> to be active", tableName);
        try {
          Thread.sleep(waitIntervalMillis);
        } catch (InterruptedException ie) {
          LOG.warn("Glue lock DynamoDB table creation sleep interrupted", e);
        }
        DescribeTableResponse describeTableResponse = dynamo.describeTable(DescribeTableRequest.builder()
            .tableName(tableName)
            .build());
        isTableActive = describeTableResponse.table().tableStatus().equals(TableStatus.ACTIVE);
      }
    }
  }

  @Override
  public void close() {
    dynamo.close();
  }
}
