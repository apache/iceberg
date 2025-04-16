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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestZkLockFactory {
  private TestingServer zkTestServer;
  private ZkLockFactory lockFactory;
  private final String testLockId = "tableName";

  @Before
  public void setUp() throws Exception {
    // Start test Zookeeper server
    zkTestServer = new TestingServer();
    // Create lock factory instance
    lockFactory =
        new ZkLockFactory(
            zkTestServer.getConnectString(),
            testLockId,
            5000,
            3000,
            new ZkLockFactory.ExponentialBackoffRetryWrapper(1000, 3));
    lockFactory.open();
  }

  @After
  public void tearDown() throws IOException {
    if (lockFactory != null) {
      lockFactory.close();
    }
    if (zkTestServer != null) {
      zkTestServer.close();
    }
  }

  @Test
  public void testCreateLock() {
    // Test creating normal lock
    TriggerLockFactory.Lock lock = lockFactory.createLock();
    assertNotNull("Lock object should not be null", lock);

    // Test basic lock functionality
    assertTrue("First lock attempt should succeed", lock.tryLock());
    assertTrue("Lock should be held after acquisition", lock.isHeld());
    lock.unlock();
    assertFalse("Lock should not be held after release", lock.isHeld());
  }

  @Test
  public void testCreateRecoveryLock() {
    // Test creating recovery lock
    TriggerLockFactory.Lock recoveryLock = lockFactory.createRecoveryLock();
    assertNotNull("Recovery lock object should not be null", recoveryLock);

    // Test basic recovery lock functionality
    assertTrue("First recovery lock attempt should succeed", recoveryLock.tryLock());
    assertTrue("Recovery lock should be held after acquisition", recoveryLock.isHeld());
    recoveryLock.unlock();
    assertFalse("Recovery lock should not be held after release", recoveryLock.isHeld());
  }

  @Test
  public void testLockExclusivity() {
    TriggerLockFactory.Lock lock1 = lockFactory.createLock();
    TriggerLockFactory.Lock lock2 = lockFactory.createLock();

    // First lock acquisition should succeed
    assertTrue(lock1.tryLock());
    // Second lock acquisition should fail
    assertFalse(lock2.tryLock());

    // After releasing first lock, second can be acquired
    lock1.unlock();
    assertTrue(lock2.tryLock());
    lock2.unlock();
  }

  @Test
  public void testDifferentLockTypes() {
    TriggerLockFactory.Lock normalLock = lockFactory.createLock();
    TriggerLockFactory.Lock recoveryLock = lockFactory.createRecoveryLock();

    assertTrue(normalLock.tryLock());
    assertTrue(recoveryLock.tryLock());

    normalLock.unlock();
    recoveryLock.unlock();
  }

  @Test
  public void testReopenFactory() throws IOException {
    lockFactory.close();
    lockFactory.open();

    TriggerLockFactory.Lock lock = lockFactory.createLock();
    assertTrue(lock.tryLock());
    lock.unlock();
  }
}
