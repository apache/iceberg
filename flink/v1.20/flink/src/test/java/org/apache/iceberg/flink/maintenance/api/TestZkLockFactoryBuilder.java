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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Map;
import org.apache.curator.test.TestingServer;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestZkLockFactoryBuilder {

  private TestingServer zkTestServer;

  @BeforeEach
  void before() {
    try {
      zkTestServer = new TestingServer();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void after() throws IOException {
    if (zkTestServer != null) {
      zkTestServer.close();
    }
  }

  @Test
  void testBuildWithMissingUri() {
    ZkLockFactoryBuilder builder = new ZkLockFactoryBuilder();
    Map<String, String> config = Maps.newHashMap();
    config.put(BaseLockFactoryBuilder.LOCK_TYPE_KEY, ZkLockFactoryBuilder.ZK_LOCK);

    assertThatThrownBy(() -> builder.build(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "Zk lock requires %s parameter",
                BaseLockFactoryBuilder.CONFIG_PREFIX + ZkLockFactoryBuilder.ZK_URI));
  }

  @Test
  void testBuildSuccessfully() {
    ZkLockFactoryBuilder builder = new ZkLockFactoryBuilder();
    Map<String, String> config = Maps.newHashMap();
    config.put(BaseLockFactoryBuilder.LOCK_TYPE_KEY, ZkLockFactoryBuilder.ZK_LOCK);
    config.put(ZkLockFactoryBuilder.ZK_URI, zkTestServer.getConnectString());
    config.put(BaseLockFactoryBuilder.LOCK_ID, "test-lock-id");

    TriggerLockFactory factory = builder.build(config);
    assertThat(factory).isNotNull();
  }
}
