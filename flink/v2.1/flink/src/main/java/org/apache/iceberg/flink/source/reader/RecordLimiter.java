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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.annotation.Internal;

@Internal
class RecordLimiter {
  private final long limit;
  private final AtomicLong counter;

  static RecordLimiter create(long limit) {
    return new RecordLimiter(limit);
  }

  private RecordLimiter(long limit) {
    this.limit = limit;
    this.counter = new AtomicLong(0);
  }

  public boolean reachedLimit() {
    return limit > 0 && counter.get() >= limit;
  }

  public void increment() {
    counter.incrementAndGet();
  }
}
