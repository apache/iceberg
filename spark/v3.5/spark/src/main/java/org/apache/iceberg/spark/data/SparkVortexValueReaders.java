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
package org.apache.iceberg.spark.data;

import dev.vortex.api.Array;
import dev.vortex.api.DType;
import java.util.function.BiFunction;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.vortex.VortexValueReader;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkVortexValueReaders {
  private SparkVortexValueReaders() {}

  public static VortexValueReader<UTF8String> utf8String() {
    return UTF8Reader.INSTANCE;
  }

  public static VortexValueReader<Integer> date(DType.TimeUnit timeUnit) {
    return new DateReader(timeUnit);
  }

  public static VortexValueReader<Long> timestamp(DType.TimeUnit timeUnit) {
    // Spark timestamp has µs precision
    return new TimestampReader(timeUnit);
  }

  static class UTF8Reader implements VortexValueReader<UTF8String> {
    static final UTF8Reader INSTANCE = new UTF8Reader();

    private UTF8Reader() {}

    @Override
    public UTF8String readNonNull(Array array, int row) {
      // TODO(aduffy): do this zero-copy by getting Vortex to give us back the pointer + len
      //  to the decompressed string.
      String value = array.getUTF8(row);
      return UTF8String.fromString(value);
    }
  }

  // Spark expects DateType as Integer number of days since UNIX epoch
  static class DateReader implements VortexValueReader<Integer> {
    private final BiFunction<Array, Integer, Integer> reader;

    private DateReader(DType.TimeUnit timeUnit) {
      switch (timeUnit) {
        case MILLISECONDS:
          this.reader =
              (array, row) -> {
                long millis = array.getLong(row);
                return DateTimeUtil.microsToDays(millis * 1000);
              };
          break;
        case DAYS:
          this.reader = Array::getInt;
          break;
        default:
          throw new IllegalArgumentException("Unsupported time unit for DATE: " + timeUnit);
      }
    }

    @Override
    public Integer readNonNull(Array array, int row) {
      return this.reader.apply(array, row);
    }
  }

  static class TimestampReader implements VortexValueReader<Long> {
    private final BiFunction<Array, Integer, Long> reader;

    private TimestampReader(DType.TimeUnit vortexTimeUnit) {
      switch (vortexTimeUnit) {
        case NANOSECONDS:
          // Round nanoseconds to microsecond
          this.reader = (array, row) -> Math.floorDiv(array.getLong(row), 1_000L);
          break;
        case MICROSECONDS:
          // Microseconds measurements
          this.reader = Array::getLong;
          break;
        case MILLISECONDS:
          // Milliseconds -> Microseconds
          this.reader = (array, row) -> Math.multiplyExact(array.getLong(row), 1_000L);
          break;
        case SECONDS:
          // Seconds -> Microseconds
          this.reader = (array, row) -> Math.multiplyExact(array.getLong(row), 1_000_000L);
          break;
        case DAYS:
          // Days -> Microseconds
          this.reader = (array, row) -> Math.multiplyExact(array.getLong(row), 86_400_000_000L);
          break;
        default:
          throw new IllegalStateException("Unexpected value: " + vortexTimeUnit);
      }
    }

    @Override
    public Long readNonNull(Array array, int row) {
      return this.reader.apply(array, row);
    }
  }
}
