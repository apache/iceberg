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
package org.apache.iceberg.gcp.bigquery.util;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/** Detects retries by counting callable invocations. */
public class RetryDetector {
  private final AtomicInteger attempts = new AtomicInteger(0);

  public <T> Callable<T> wrap(Callable<T> delegate) {
    return () -> {
      attempts.getAndIncrement();
      return delegate.call();
    };
  }

  public boolean retried() {
    return attempts.get() > 1;
  }

  public int attempts() {
    return attempts.get();
  }
}
