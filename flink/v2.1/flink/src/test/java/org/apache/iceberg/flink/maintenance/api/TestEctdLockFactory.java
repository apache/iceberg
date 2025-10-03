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

import io.etcd.jetcd.launcher.Etcd;
import io.etcd.jetcd.launcher.EtcdCluster;
import java.io.IOException;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class TestEctdLockFactory extends TestLockFactoryBase {
  private EtcdCluster etcdCluster;

  @Override
  TriggerLockFactory lockFactory(String tableName) {
    String endpoints =
        etcdCluster.clientEndpoints().stream()
            .map(Object::toString)
            .collect(Collectors.joining(","));
    return new EtcdLockFactory(endpoints, tableName, 5000, 30000, 10000, 2);
  }

  @BeforeEach
  @Override
  void before() {
    try {
      etcdCluster = new Etcd.Builder().withNodes(1).withMountedDataDirectory(false).build();
      etcdCluster.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    super.before();
  }

  @AfterEach
  public void after() throws IOException {
    if (etcdCluster != null) {
      etcdCluster.close();
    }

    super.after();
  }
}
