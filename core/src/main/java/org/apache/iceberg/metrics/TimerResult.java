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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.immutables.value.Value;

/** A serializable version of a {@link Timer} that carries its result. */
@Value.Immutable
public interface TimerResult {

  TimeUnit timeUnit();

  Duration totalDuration();

  long count();

  static TimerResult fromTimer(Timer timer) {
    Preconditions.checkArgument(null != timer, "Invalid timer: null");
    if (timer.isNoop()) {
      return null;
    }

    return ImmutableTimerResult.builder()
        .timeUnit(timer.unit())
        .totalDuration(timer.totalDuration())
        .count(timer.count())
        .build();
  }

  static TimerResult of(TimeUnit timeUnit, Duration duration, long count) {
    return ImmutableTimerResult.builder()
        .timeUnit(timeUnit)
        .totalDuration(duration)
        .count(count)
        .build();
  }
}
