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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestParquetInt96Timestamps {
  private static final BigInteger NANOS_PER_MICRO = BigInteger.valueOf(1000);

  @Test
  void preservesTheFullMicrosecondRange() {
    long[] values = {Long.MIN_VALUE, Long.MAX_VALUE, -86_400_000_001L, -1, 0, 1, 86_400_000_001L};
    for (long value : values) {
      assertThat(
              ParquetUtil.extractTimestampInt96(
                  buffer(BigInteger.valueOf(value).multiply(NANOS_PER_MICRO))))
          .isEqualTo(value);
    }
    Random random = new Random(82745);
    for (int index = 0; index < 500; index += 1) {
      long value = random.nextLong();
      assertThat(
              ParquetUtil.extractTimestampInt96(
                  buffer(BigInteger.valueOf(value).multiply(NANOS_PER_MICRO))))
          .isEqualTo(value);
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {1, -1, 1001, -1001, 86_400_000_000_001L, -86_400_000_000_001L})
  void truncatesSubMicrosecondPrecision(long nanos) {
    assertThat(ParquetUtil.extractTimestampInt96(buffer(BigInteger.valueOf(nanos))))
        .isEqualTo(Math.floorDiv(nanos, 1000));
  }

  @ParameterizedTest
  @ValueSource(longs = {Long.MIN_VALUE, Long.MAX_VALUE})
  void truncatesSubMicrosecondsAtRangeBoundaries(long micros) {
    BigInteger nanos = BigInteger.valueOf(micros).multiply(NANOS_PER_MICRO);
    assertThat(
            ParquetUtil.extractTimestampInt96(
                buffer(nanos.add(NANOS_PER_MICRO.subtract(BigInteger.ONE)))))
        .isEqualTo(micros);
  }

  @Test
  void rejectsOneNanosecondBelowMicrosecondRange() {
    BigInteger nanos =
        BigInteger.valueOf(Long.MIN_VALUE).multiply(NANOS_PER_MICRO).subtract(BigInteger.ONE);
    assertThatThrownBy(() -> ParquetUtil.extractTimestampInt96(buffer(nanos)))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsMicrosecondOverflow(boolean upper) {
    BigInteger micros =
        upper
            ? BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)
            : BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
    assertThatThrownBy(
            () -> ParquetUtil.extractTimestampInt96(buffer(micros.multiply(NANOS_PER_MICRO))))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
  }

  @Test
  void preservesTheFullNanosecondRange() {
    long[] values = {
      Long.MIN_VALUE,
      Long.MAX_VALUE,
      -86_400_000_000_001L,
      -1_000_000_001L,
      -1_000_000_000L,
      -999_999_999L,
      -1,
      0,
      1,
      1001,
      999_999_999L,
      1_000_000_000L,
      1_000_000_001L,
      86_400_000_000_001L
    };
    for (long value : values) {
      assertThat(ParquetUtil.extractTimestampInt96Nanos(buffer(BigInteger.valueOf(value))))
          .isEqualTo(value);
    }

    Random random = new Random(90281);
    for (int index = 0; index < 500; index += 1) {
      long value = random.nextLong();
      assertThat(ParquetUtil.extractTimestampInt96Nanos(buffer(BigInteger.valueOf(value))))
          .isEqualTo(value);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsNanosecondOverflow(boolean upper) {
    BigInteger nanos =
        upper
            ? BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)
            : BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
    assertThatThrownBy(() -> ParquetUtil.extractTimestampInt96Nanos(buffer(nanos)))
        .isInstanceOf(ArithmeticException.class)
        .hasMessageContaining("overflow");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void consumesOnlyTheInt96Bytes(boolean direct) {
    ByteBuffer bytes = direct ? ByteBuffer.allocateDirect(24) : ByteBuffer.allocate(24);
    bytes.order(ByteOrder.LITTLE_ENDIAN);
    bytes.position(5);
    bytes.put(Int96TestUtil.encode(BigInteger.valueOf(-1000)).getBytes());
    bytes.limit(17).position(5);
    ByteBuffer readOnly = bytes.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
    assertThat(ParquetUtil.extractTimestampInt96(readOnly)).isEqualTo(-1);
    assertThat(readOnly.position()).isEqualTo(17);
    assertThat(readOnly.limit()).isEqualTo(17);
    assertThat(readOnly.order()).isEqualTo(ByteOrder.LITTLE_ENDIAN);
  }

  private static ByteBuffer buffer(BigInteger nanos) {
    return Int96TestUtil.encode(nanos).toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
  }
}
