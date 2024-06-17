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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.concurrent.Semaphore;
import org.apache.flink.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The locks are based on static {@link Semaphore} objects. We expect that the {@link
 * TriggerManager} and the LockRemover operators will be placed on the same TaskManager (JVM), as
 * they are both global. In this case JVM based locking should be enough to allow communication
 * between the operators.
 */
@Internal
public class JVMBasedLockFactory implements TriggerLockFactory {
  private static final Logger LOG = LoggerFactory.getLogger(JVMBasedLockFactory.class);
  private static final String RUNNING_LOCK_NAME = "running";
  private static final String RECOVERING_LOCK_NAME = "recovering";
  private static final Semaphore runningLock = new Semaphore(1);
  private static final Semaphore recoveringLock = new Semaphore(1);

  @Override
  public TriggerLockFactory.Lock createLock() {
    return new Lock(runningLock, RUNNING_LOCK_NAME);
  }

  @Override
  public TriggerLockFactory.Lock createRecoveryLock() {
    return new Lock(recoveringLock, RECOVERING_LOCK_NAME);
  }

  public static class Lock implements TriggerLockFactory.Lock {
    private final Semaphore semaphore;
    private final String name;

    public Lock(Semaphore semaphore, String name) {
      this.semaphore = semaphore;
      this.name = name;
    }

    @Override
    public boolean tryLock() {
      boolean acquired = semaphore.tryAcquire();
      LOG.info("Trying to acquire {} lock with result {}", name, acquired);
      return acquired;
    }

    @Override
    public void unlock() {
      synchronized (semaphore) {
        // Make sure that the available permits never increase above 1
        if (semaphore.availablePermits() == 0) {
          semaphore.release();
        }
      }

      LOG.info("Released {} lock", name);
    }

    @Override
    public boolean isHeld() {
      return semaphore.availablePermits() < 1;
    }
  }
}
