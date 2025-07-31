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
package org.apache.iceberg.flink.util;

import java.util.concurrent.TimeUnit;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Gauge;

/**
 * This gauge measures the elapsed time between now and last recorded time set by {@link
 * ElapsedTimeGauge#refreshLastRecordedTime()}.
 */
@Internal
public class ElapsedTimeGauge implements Gauge<Long> {
  private final TimeUnit reportUnit;
  private volatile long lastRecordedTimeNano;

  public ElapsedTimeGauge(TimeUnit timeUnit) {
    this.reportUnit = timeUnit;
    refreshLastRecordedTime();
  }

  public void refreshLastRecordedTime() {
    this.lastRecordedTimeNano = System.nanoTime();
  }

  @Override
  public Long getValue() {
    return reportUnit.convert(System.nanoTime() - lastRecordedTimeNano, TimeUnit.NANOSECONDS);
  }
}
