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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestLockManagers {

  @Test
  public void testLoadDefaultLockManager() {
    assertThat(LockManagers.defaultLockManager())
        .isInstanceOf(LockManagers.InMemoryLockManager.class);
  }

  @Test
  public void testLoadCustomLockManager() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.LOCK_IMPL, CustomLockManager.class.getName());
    assertThat(LockManagers.from(properties)).isInstanceOf(CustomLockManager.class);
  }

  @Test
  public void testClosingOneManagerDoesNotAffectAnother() throws Exception {
    Map<String, String> properties = Maps.newHashMap();
    LockManagers.InMemoryLockManager manager1 = new LockManagers.InMemoryLockManager(properties);
    LockManagers.InMemoryLockManager manager2 = new LockManagers.InMemoryLockManager(properties);

    // Both managers access the shared scheduler
    assertThat(manager1.acquire("entity1", "owner1")).isTrue();
    assertThat(manager2.acquire("entity2", "owner2")).isTrue();

    // Close manager1 - should NOT shut down the shared scheduler
    manager1.release("entity1", "owner1");
    manager1.close();

    // manager2 should still be able to acquire locks (scheduler still alive)
    assertThat(manager2.acquire("entity3", "owner3")).isTrue();
    manager2.release("entity3", "owner3");

    // Cleanup
    manager2.release("entity2", "owner2");
    manager2.close();
  }

  @Test
  public void testClosingAllManagersShutsDownScheduler() throws Exception {
    Map<String, String> properties = Maps.newHashMap();
    LockManagers.InMemoryLockManager manager1 = new LockManagers.InMemoryLockManager(properties);
    LockManagers.InMemoryLockManager manager2 = new LockManagers.InMemoryLockManager(properties);

    // Both managers access the shared scheduler
    assertThat(manager1.acquire("entity1", "owner1")).isTrue();
    assertThat(manager2.acquire("entity2", "owner2")).isTrue();

    // Release locks and close both managers
    manager1.release("entity1", "owner1");
    manager1.close();
    manager2.release("entity2", "owner2");
    manager2.close();

    // After both are closed, a new manager should still be able to create a fresh scheduler
    LockManagers.InMemoryLockManager manager3 = new LockManagers.InMemoryLockManager(properties);
    assertThat(manager3.acquire("entity3", "owner3")).isTrue();
    manager3.release("entity3", "owner3");
    manager3.close();
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
