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

import java.io.IOException;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class TestZkLockFactory extends TestLockFactoryBase {

  private TestingServer zkTestServer;

  @Override
  TriggerLockFactory lockFactory(String tableName) {
    return new ZkLockFactory(zkTestServer.getConnectString(), tableName, 5000, 3000, 1000, 3);
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
}
