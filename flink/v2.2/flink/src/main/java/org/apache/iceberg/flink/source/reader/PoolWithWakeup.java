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
package org.apache.iceberg.flink.source.reader;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.flink.connector.file.src.util.Pool;

/**
 * A recyclable object pool similar to Flink's {@link Pool}, but whose blocking {@link #pollEntry()}
 * can be unblocked by {@link #wakeUp()} without interrupting the waiting thread.
 */
class PoolWithWakeup<T> {

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition notEmpty = lock.newCondition();
  private final Deque<T> entries;
  private final Pool.Recycler<T> recycler = this::addBack;

  // Set by wakeUp() and consumed by pollEntry(). Guarded by lock.
  private boolean wokenUp;

  PoolWithWakeup(int poolCapacity) {
    this.entries = new ArrayDeque<>(poolCapacity);
  }

  /** Adds an entry to the pool. Used to populate the pool initially. */
  void add(T entry) {
    addBack(entry);
  }

  /**
   * Returns a recycler that adds entries back to the pool once they are no longer in use. A blocked
   * {@link #pollEntry()} is woken up when an entry is recycled.
   */
  Pool.Recycler<T> recycler() {
    return recycler;
  }

  /**
   * Retrieves the next available entry, blocking until one is recycled or until {@link #wakeUp()}
   * is called.
   *
   * @return the next entry, or {@code null} if the pool was woken up while waiting
   */
  T pollEntry() throws InterruptedException {
    lock.lock();
    try {
      while (entries.isEmpty()) {
        if (wokenUp) {
          // Consume the wakeup signal and return without an entry.
          wokenUp = false;
          return null;
        }

        notEmpty.await();
      }

      return entries.pollFirst();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Unblocks a thread currently waiting in {@link #pollEntry()} without interrupting it. If no
   * thread is currently blocked, the next {@link #pollEntry()} call that would otherwise block
   * returns {@code null} once. Safe to call from a thread other than the one blocked in {@code
   * pollEntry()}.
   */
  void wakeUp() {
    lock.lock();
    try {
      wokenUp = true;
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
  }

  private void addBack(T entry) {
    lock.lock();
    try {
      entries.addLast(entry);
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
  }
}
