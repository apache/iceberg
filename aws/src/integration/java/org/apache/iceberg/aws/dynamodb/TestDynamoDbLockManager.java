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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

public class TestDynamoDbLockManager {

  private static final ForkJoinPool POOL = new ForkJoinPool(16);

  private static String lockTableName;
  private static DynamoDbClient dynamo;

  private DynamoDbLockManager lockManager;
  private String entityId;
  private String ownerId;

  @BeforeAll
  public static void beforeClass() {
    lockTableName = genTableName();
    dynamo = AwsClientFactories.defaultFactory().dynamo();
  }

  @BeforeEach
  public void before() {
    lockManager = new DynamoDbLockManager(dynamo, lockTableName);
    entityId = UUID.randomUUID().toString();
    ownerId = UUID.randomUUID().toString();
  }

  @AfterAll
  public static void afterClass() {
    dynamo.deleteTable(DeleteTableRequest.builder().tableName(lockTableName).build());
  }

  @Test
  public void testTableCreation() {
    assertThat(lockManager.tableExists(lockTableName)).isTrue();
  }

  @Test
  public void testAcquireOnceSingleProcess() {
    lockManager.acquireOnce(entityId, ownerId);
    Map<String, AttributeValue> key = Maps.newHashMap();
    key.put("entityId", AttributeValue.builder().s(entityId).build());
    GetItemResponse response =
        dynamo.getItem(GetItemRequest.builder().tableName(lockTableName).key(key).build());
    assertThat(response.hasItem()).as("should have item in dynamo after acquire").isTrue();
    assertThat(response.item())
        .hasEntrySatisfying(
            "entityId", attributeValue -> assertThat(attributeValue.s()).isEqualTo(entityId))
        .hasEntrySatisfying(
            "ownerId", attributeValue -> assertThat(attributeValue.s()).isEqualTo(ownerId))
        .hasEntrySatisfying("version", attributeValue -> assertThat(attributeValue).isNotNull())
        .hasEntrySatisfying(
            "leaseDurationMs", attributeValue -> assertThat(attributeValue).isNotNull());
  }

  @Test
  public void testAcquireOnceMultiProcesses() throws Exception {
    List<Boolean> results =
        POOL.submit(
                () ->
                    IntStream.range(0, 16)
                        .parallel()
                        .mapToObj(
                            i -> {
                              try {
                                DynamoDbLockManager threadLocalLockManager =
                                    new DynamoDbLockManager(dynamo, lockTableName);
                                threadLocalLockManager.acquireOnce(
                                    entityId, UUID.randomUUID().toString());
                                return true;
                              } catch (ConditionalCheckFailedException e) {
                                return false;
                              }
                            })
                        .collect(Collectors.toList()))
            .get();
    assertThat(results).as("should have only 1 process succeeded in acquisition").hasSize(1);
  }

  @Test
  public void testReleaseAndAcquire() {
    assertThat(lockManager.acquire(entityId, ownerId)).isTrue();
    assertThat(lockManager.release(entityId, ownerId)).isTrue();
    assertThat(lockManager.acquire(entityId, ownerId)).isTrue();
  }

  @Test
  public void testReleaseWithWrongOwner() {
    assertThat(lockManager.acquire(entityId, ownerId)).isTrue();
    assertThat(lockManager.release(entityId, UUID.randomUUID().toString())).isFalse();
  }

  @Test
  @SuppressWarnings({"DangerousCompletableFutureUsage", "FutureReturnValueIgnored"})
  public void testAcquireSingleProcess() throws Exception {
    assertThat(lockManager.acquire(entityId, ownerId)).isTrue();
    String oldOwner = ownerId;

    CompletableFuture.supplyAsync(
        () -> {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          assertThat(lockManager.release(entityId, oldOwner)).isTrue();
          return null;
        });

    ownerId = UUID.randomUUID().toString();
    long start = System.currentTimeMillis();
    assertThat(lockManager.acquire(entityId, ownerId)).isTrue();
    assertThat(System.currentTimeMillis() - start).isGreaterThanOrEqualTo(5000);
  }

  @Test
  public void testAcquireMultiProcessAllSucceed() throws Exception {
    lockManager.initialize(
        ImmutableMap.of(
            CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS, "500",
            CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS, "100000000",
            CatalogProperties.LOCK_TABLE, lockTableName));
    long start = System.currentTimeMillis();
    List<Boolean> results =
        POOL.submit(
                () ->
                    IntStream.range(0, 16)
                        .parallel()
                        .mapToObj(
                            i -> {
                              DynamoDbLockManager threadLocalLockManager =
                                  new DynamoDbLockManager(dynamo, lockTableName);
                              String owner = UUID.randomUUID().toString();
                              boolean succeeded = threadLocalLockManager.acquire(entityId, owner);
                              if (succeeded) {
                                try {
                                  Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                  throw new RuntimeException(e);
                                }
                                assertThat(threadLocalLockManager.release(entityId, owner))
                                    .isTrue();
                              }
                              return succeeded;
                            })
                        .collect(Collectors.toList()))
            .get();
    assertThat(results).as("all lock acquire should succeed sequentially").hasSize(16);
    assertThat(System.currentTimeMillis() - start).isGreaterThanOrEqualTo(16000);
  }

  @Test
  public void testAcquireMultiProcessOnlyOneSucceed() throws Exception {
    lockManager.initialize(
        ImmutableMap.of(
            CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS,
            "10000",
            CatalogProperties.LOCK_TABLE,
            lockTableName));

    List<Boolean> results =
        POOL.submit(
                () ->
                    IntStream.range(0, 16)
                        .parallel()
                        .mapToObj(
                            i -> {
                              DynamoDbLockManager threadLocalLockManager =
                                  new DynamoDbLockManager(dynamo, lockTableName);
                              return threadLocalLockManager.acquire(entityId, ownerId);
                            })
                        .collect(Collectors.toList()))
            .get();
    assertThat(results).as("only 1 thread should have acquired the lock").hasSize(1);
  }

  @Test
  public void testTableCreationFailure() {
    DynamoDbClient dynamo2 = Mockito.mock(DynamoDbClient.class);
    Mockito.doThrow(ResourceNotFoundException.class)
        .when(dynamo2)
        .describeTable(Mockito.any(DescribeTableRequest.class));
    assertThatThrownBy(() -> new DynamoDbLockManager(dynamo2, lockTableName))
        .as("should fail to initialize the lock manager")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot find Dynamo table");
  }

  private static String genTableName() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
