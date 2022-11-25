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
package org.apache.iceberg.metrics;

import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.immutables.value.Value;

/** A serializable version of a {@link Counter} that carries its result. */
@Value.Immutable
public interface CounterResult {

  Unit unit();

  long value();

  static CounterResult fromCounter(Counter counter) {
    Preconditions.checkArgument(null != counter, "Invalid counter: null");
    if (counter.isNoop()) {
      return null;
    }

    return ImmutableCounterResult.builder().unit(counter.unit()).value(counter.value()).build();
  }

  static CounterResult of(Unit unit, long value) {
    return ImmutableCounterResult.builder().unit(unit).value(value).build();
  }
}
