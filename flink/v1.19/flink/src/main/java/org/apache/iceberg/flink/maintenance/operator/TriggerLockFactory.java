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

import java.io.Serializable;
import org.apache.flink.annotation.Experimental;

/** Lock interface for handling locks for the Flink Table Maintenance jobs. */
@Experimental
public interface TriggerLockFactory extends Serializable {
  Lock createLock();

  Lock createRecoveryLock();

  interface Lock {
    /**
     * Tries to acquire a lock with a given key. Anyone already holding a lock would prevent
     * acquiring this lock. Not reentrant.
     *
     * @return <code>true</code> if the lock is acquired by this job, <code>false</code> if the lock
     *     is already held by someone
     */
    boolean tryLock();

    /**
     * Checks if the lock is already taken.
     *
     * @return <code>true</code> if the lock is held by someone
     */
    boolean isHeld();

    /** Releases the lock. Should not fail if the lock is not held by anyone. */
    void unlock();
  }
}
