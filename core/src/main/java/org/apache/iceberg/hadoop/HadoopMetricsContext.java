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
package org.apache.iceberg.hadoop;

import java.util.Map;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIOMetricsContext;

/**
 * FileIO Metrics implementation that delegates to Hadoop FileSystem statistics implementation using
 * the provided scheme.
 */
public class HadoopMetricsContext implements FileIOMetricsContext {
  public static final String SCHEME = "io.metrics-scheme";

  private String scheme;
  private transient volatile FileSystem.Statistics statistics;

  public HadoopMetricsContext(String scheme) {
    ValidationException.check(
        scheme != null, "Scheme is required for Hadoop FileSystem metrics reporting");

    this.scheme = scheme;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    // FileIO has no specific implementation class, but Hadoop will
    // still track and report for the provided scheme.
    this.scheme = properties.getOrDefault(SCHEME, scheme);
    this.statistics = FileSystem.getStatistics(scheme, null);
  }

  /**
   * The Hadoop implementation delegates to the FileSystem.Statistics implementation and therefore
   * does not require support for operations like unit() as the counter values are not directly
   * consumed.
   *
   * @param name name of the metric
   * @param unit ignored
   * @return counter
   */
  @Override
  public org.apache.iceberg.metrics.Counter counter(String name, Unit unit) {
    switch (name) {
      case READ_BYTES:
        return counter(statistics()::incrementBytesRead, statistics()::getBytesRead);
      case READ_OPERATIONS:
        return counter((long x) -> statistics.incrementReadOps((int) x), statistics()::getReadOps);
      case WRITE_BYTES:
        return counter(statistics()::incrementBytesWritten, statistics()::getBytesWritten);
      case WRITE_OPERATIONS:
        return counter(
            (long x) -> statistics.incrementWriteOps((int) x), statistics()::getWriteOps);
      default:
        throw new IllegalArgumentException(String.format("Unsupported counter: '%s'", name));
    }
  }

  private org.apache.iceberg.metrics.Counter counter(LongConsumer consumer, LongSupplier supplier) {
    return new org.apache.iceberg.metrics.Counter() {
      @Override
      public void increment() {
        increment(1L);
      }

      @Override
      public void increment(long amount) {
        consumer.accept(amount);
      }

      @Override
      public long value() {
        return supplier.getAsLong();
      }
    };
  }

  private FileSystem.Statistics statistics() {
    if (statistics == null) {
      synchronized (this) {
        if (statistics == null) {
          this.statistics = FileSystem.getStatistics(scheme, null);
        }
      }
    }

    return statistics;
  }
}
