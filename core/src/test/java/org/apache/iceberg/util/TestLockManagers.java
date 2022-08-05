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
package org.apache.iceberg.util;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestLockManagers {

  @Test
  public void testLoadDefaultLockManager() {
    Assertions.assertThat(LockManagers.defaultLockManager())
        .isInstanceOf(LockManagers.InMemoryLockManager.class);
  }

  @Test
  public void testLoadCustomLockManager() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.LOCK_IMPL, CustomLockManager.class.getName());
    Assertions.assertThat(LockManagers.from(properties)).isInstanceOf(CustomLockManager.class);
  }

  static class CustomLockManager implements LockManager {

    @Override
    public boolean acquire(String entityId, String ownerId) {
      return false;
    }

    @Override
    public boolean release(String entityId, String ownerId) {
      return false;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void initialize(Map<String, String> properties) {}
  }
}
