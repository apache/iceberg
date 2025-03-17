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

import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

abstract class TestLockFactoryBase {
  protected TriggerLockFactory lockFactory;

  abstract TriggerLockFactory lockFactory();

  @BeforeEach
  void before() {
    this.lockFactory = lockFactory();
    lockFactory.open();
  }

  @AfterEach
  void after() throws IOException {
    lockFactory.close();
  }

  @Test
  void testTryLock() {
    TriggerLockFactory.Lock lock1 = lockFactory.createLock();
    TriggerLockFactory.Lock lock2 = lockFactory.createLock();
    assertThat(lock1.tryLock()).isTrue();
    assertThat(lock1.tryLock()).isFalse();
    assertThat(lock2.tryLock()).isFalse();
  }

  @Test
  void testUnLock() {
    TriggerLockFactory.Lock lock = lockFactory.createLock();
    assertThat(lock.tryLock()).isTrue();

    lock.unlock();
    assertThat(lock.tryLock()).isTrue();
  }

  @Test
  void testNoConflictWithRecoveryLock() {
    TriggerLockFactory.Lock lock1 = lockFactory.createLock();
    TriggerLockFactory.Lock lock2 = lockFactory.createRecoveryLock();
    assertThat(lock1.tryLock()).isTrue();
    assertThat(lock2.tryLock()).isTrue();
  }

  @Test
  void testDoubleUnLock() {
    TriggerLockFactory.Lock lock = lockFactory.createLock();
    assertThat(lock.tryLock()).isTrue();

    lock.unlock();
    lock.unlock();
    assertThat(lock.tryLock()).isTrue();
    assertThat(lock.tryLock()).isFalse();
  }
}
