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
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.metrics.MetricsContext;

@Internal
public class ReaderMetricsContext implements MetricsContext {
  public static final String ASSIGNED_SPLITS = "assignedSplits";
  public static final String ASSIGNED_BYTES = "assignedBytes";
  public static final String FINISHED_SPLITS = "finishedSplits";
  public static final String FINISHED_BYTES = "finishedBytes";
  public static final String SPLIT_READER_FETCH_CALLS = "splitReaderFetchCalls";

  private final AtomicLong assignedSplits;
  private final AtomicLong assignedBytes;
  private final AtomicLong finishedSplits;
  private final AtomicLong finishedBytes;
  private final AtomicLong splitReaderFetchCalls;

  public ReaderMetricsContext(MetricGroup metricGroup) {
    MetricGroup readerMetricGroup = metricGroup.addGroup("IcebergSourceReader");
    this.assignedSplits = new AtomicLong();
    this.assignedBytes = new AtomicLong();
    this.finishedSplits = new AtomicLong();
    this.finishedBytes = new AtomicLong();
    this.splitReaderFetchCalls = new AtomicLong();
    readerMetricGroup.gauge(ASSIGNED_SPLITS, assignedSplits::get);
    readerMetricGroup.gauge(ASSIGNED_BYTES, assignedBytes::get);
    readerMetricGroup.gauge(FINISHED_SPLITS, finishedSplits::get);
    readerMetricGroup.gauge(FINISHED_BYTES, finishedBytes::get);
    readerMetricGroup.gauge(SPLIT_READER_FETCH_CALLS, splitReaderFetchCalls::get);
  }

  @Override
  public <T extends Number> Counter<T> counter(String name, Class<T> type, Unit unit) {
    switch (name) {
      case ASSIGNED_SPLITS:
        ValidationException.check(type == Long.class, "'%s' requires Long type", ASSIGNED_SPLITS);
        return (Counter<T>) longCounter(assignedSplits::addAndGet, assignedSplits::get);
      case ASSIGNED_BYTES:
        ValidationException.check(type == Long.class, "'%s' requires Integer type", ASSIGNED_BYTES);
        return (Counter<T>) longCounter(assignedBytes::addAndGet, assignedBytes::get);
      case FINISHED_SPLITS:
        ValidationException.check(type == Long.class, "'%s' requires Long type", FINISHED_SPLITS);
        return (Counter<T>) longCounter(finishedSplits::addAndGet, finishedSplits::get);
      case FINISHED_BYTES:
        ValidationException.check(type == Long.class, "'%s' requires Integer type", FINISHED_BYTES);
        return (Counter<T>) longCounter(finishedBytes::addAndGet, finishedBytes::get);
      case SPLIT_READER_FETCH_CALLS:
        ValidationException.check(type == Long.class, "'%s' requires Integer type", SPLIT_READER_FETCH_CALLS);
        return (Counter<T>) longCounter(splitReaderFetchCalls::addAndGet, splitReaderFetchCalls::get);
      default:
        throw new IllegalArgumentException(String.format("Unsupported counter: '%s'", name));
    }
  }

  private Counter<Long> longCounter(Consumer<Long> consumer, Supplier<Long> supplier) {
    return new Counter<Long>() {
      @Override
      public void increment() {
        increment(1L);
      }

      @Override
      public void increment(Long amount) {
        consumer.accept(amount);
      }

      @Override
      public Long value() {
        return supplier.get();
      }
    };
  }
}
