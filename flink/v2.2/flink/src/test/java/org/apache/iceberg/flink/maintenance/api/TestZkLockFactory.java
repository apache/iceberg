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
package org.apache.iceberg.flink.maintenance.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator5.org.apache.curator.RetryPolicy;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.RetryNTimes;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.RetryOneTime;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.RetryUntilElapsed;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestZkLockFactory extends TestLockFactoryBase {

  private TestingServer zkTestServer;

  @Override
  TriggerLockFactory lockFactory(String tableName) {
    return new ZkLockFactory(
        zkTestServer.getConnectString(),
        tableName,
        5000,
        3000,
        1000,
        3,
        ZKRetryPolicies.EXPONENTIAL_BACKOFF,
        2000);
  }

  @BeforeEach
  @Override
  void before() {
    try {
      zkTestServer = new TestingServer();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    super.before();
  }

  @AfterEach
  public void after() throws IOException {
    super.after();
    if (zkTestServer != null) {
      zkTestServer.close();
    }
  }

  private static Stream<Arguments> retryPolicyProvider() {
    return Stream.of(
        Arguments.of(ZKRetryPolicies.ONE_TIME, RetryOneTime.class),
        Arguments.of(ZKRetryPolicies.N_TIME, RetryNTimes.class),
        Arguments.of(ZKRetryPolicies.EXPONENTIAL_BACKOFF, ExponentialBackoffRetry.class),
        Arguments.of(
            ZKRetryPolicies.BOUNDED_EXPONENTIAL_BACKOFF, BoundedExponentialBackoffRetry.class),
        Arguments.of(ZKRetryPolicies.UNTIL_ELAPSED, RetryUntilElapsed.class));
  }

  @ParameterizedTest(name = "{0} should create {1}")
  @MethodSource("retryPolicyProvider")
  @DisplayName(
      "Verify ZkLockFactory creates correct Curator RetryPolicy for each ZKRetryPolicies enum")
  void testRetryPolicyCreationAndType(
      ZKRetryPolicies policy, Class<? extends RetryPolicy> expectedClass) {
    ZkLockFactory factory =
        new ZkLockFactory("localhost:2181", "test", 3000, 3000, 1000, 3, policy, 2000);

    RetryPolicy retryPolicy = factory.createRetryPolicy();

    assertThat(retryPolicy)
        .as("RetryPolicy should not be null for policy %s", policy)
        .isNotNull()
        .as("Expected %s for policy %s", expectedClass.getSimpleName(), policy)
        .isInstanceOf(expectedClass);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "non_existing_policy"})
  void testInvalidOrMissingRetryPolicyFallsBackToDefault(String retryPolicyConfig) {
    Map<String, String> options = Maps.newHashMap();
    if (retryPolicyConfig != null) {
      options.put("iceberg.maintenance.lock.zookeeper.retry-policy", retryPolicyConfig);
    }

    LockConfig config = new LockConfig(mock(Table.class), options, new Configuration());

    ZKRetryPolicies policy = config.zkRetryPolicy();

    assertThat(policy)
        .as("Invalid, empty, or missing retry-policy should fallback to EXPONENTIAL_BACKOFF")
        .isEqualTo(ZKRetryPolicies.EXPONENTIAL_BACKOFF);
  }
}
