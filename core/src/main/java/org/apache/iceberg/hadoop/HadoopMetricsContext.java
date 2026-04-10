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
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.StorageStatistics;
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

  /**
   * Custom StorageStatistics implementation that wraps FileSystem.Statistics.
   * Unlike FileSystemStorageStatistics, this provides public access to the wrapped Statistics.
   */
  private static class IcebergStorageStatistics extends StorageStatistics {
    private final FileSystem.Statistics statistics;

    IcebergStorageStatistics(String scheme, FileSystem.Statistics stats) {
      super(scheme);
      this.statistics = stats;
    }

    FileSystem.Statistics getStatistics() {
      return statistics;
    }

    @Override
    public java.util.Iterator<LongStatistic> getLongStatistics() {
      return java.util.Arrays.asList(
              new LongStatistic("bytesRead", statistics.getBytesRead()),
              new LongStatistic("bytesWritten", statistics.getBytesWritten()),
              new LongStatistic("readOps", statistics.getReadOps()),
              new LongStatistic("largeReadOps", statistics.getLargeReadOps()),
              new LongStatistic("writeOps", statistics.getWriteOps()))
          .iterator();
    }

    @Override
    public Long getLong(String key) {
      switch (key) {
        case "bytesRead":
          return statistics.getBytesRead();
        case "bytesWritten":
          return statistics.getBytesWritten();
        case "readOps":
          return (long) statistics.getReadOps();
        case "largeReadOps":
          return (long) statistics.getLargeReadOps();
        case "writeOps":
          return (long) statistics.getWriteOps();
        default:
          return null;
      }
    }

    @Override
    public boolean isTracked(String key) {
      return "bytesRead".equals(key)
          || "bytesWritten".equals(key)
          || "readOps".equals(key)
          || "largeReadOps".equals(key)
          || "writeOps".equals(key);
    }

    @Override
    public void reset() {
      statistics.reset();
    }
  }

  public HadoopMetricsContext(String scheme) {
    ValidationException.check(
        scheme != null, "Scheme is required for Hadoop FileSystem metrics reporting");

    this.scheme = scheme;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    // Use GlobalStorageStatistics for per-scheme separation.
    // FileSystem.getStatistics(scheme, null) uses Class as the map key, causing all schemes
    // to share one Statistics instance. GlobalStorageStatistics is keyed by scheme string.
    this.scheme = properties.getOrDefault(SCHEME, scheme);
    FileSystem.Statistics newStats = new FileSystem.Statistics(this.scheme);
    StorageStatistics registered =
        GlobalStorageStatistics.INSTANCE.put(
            this.scheme, () -> new IcebergStorageStatistics(this.scheme, newStats));
    if (registered instanceof IcebergStorageStatistics) {
      this.statistics = ((IcebergStorageStatistics) registered).getStatistics();
    } else {
      this.statistics = newStats;
    }
  }

  /**
   * The Hadoop implementation delegates to the FileSystem.Statistics implementation and therefore
   * does not require support for operations like unit() and count() as the counter values are not
   * directly consumed.
   *
   * @param name name of the metric
   * @param type numeric type of the counter value
   * @param unit ignored
   * @param <T> Counter numeric type
   * @return counter
   */
  @Override
  @SuppressWarnings("unchecked")
  public <T extends Number> Counter<T> counter(String name, Class<T> type, Unit unit) {
    switch (name) {
      case READ_BYTES:
        ValidationException.check(type == Long.class, "'%s' requires Long type", READ_BYTES);
        return (Counter<T>) longCounter(statistics()::incrementBytesRead);
      case READ_OPERATIONS:
        ValidationException.check(
            type == Integer.class, "'%s' requires Integer type", READ_OPERATIONS);
        return (Counter<T>) integerCounter(statistics()::incrementReadOps);
      case WRITE_BYTES:
        ValidationException.check(type == Long.class, "'%s' requires Long type", WRITE_BYTES);
        return (Counter<T>) longCounter(statistics()::incrementBytesWritten);
      case WRITE_OPERATIONS:
        ValidationException.check(
            type == Integer.class, "'%s' requires Integer type", WRITE_OPERATIONS);
        return (Counter<T>) integerCounter(statistics()::incrementWriteOps);
      default:
        throw new IllegalArgumentException(String.format("Unsupported counter: '%s'", name));
    }
  }

  private Counter<Long> longCounter(Consumer<Long> consumer) {
    return new Counter<Long>() {
      @Override
      public void increment() {
        increment(1L);
      }

      @Override
      public void increment(Long amount) {
        consumer.accept(amount);
      }
    };
  }

  private Counter<Integer> integerCounter(Consumer<Integer> consumer) {
    return new Counter<Integer>() {
      @Override
      public void increment() {
        increment(1);
      }

      @Override
      public void increment(Integer amount) {
        consumer.accept(amount);
      }
    };
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
          FileSystem.Statistics newStats = new FileSystem.Statistics(this.scheme);
          StorageStatistics registered =
              GlobalStorageStatistics.INSTANCE.put(
                  this.scheme, () -> new IcebergStorageStatistics(this.scheme, newStats));
          if (registered instanceof IcebergStorageStatistics) {
            this.statistics = ((IcebergStorageStatistics) registered).getStatistics();
          } else {
            this.statistics = newStats;
          }
        }
      }
    }

    return statistics;
  }
}
