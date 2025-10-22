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
package org.apache.iceberg.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestTransformTypeId {

  @Test
  public void testTruncateTypeIds() {
    Truncate<Integer> truncateInt = Truncate.get(Types.IntegerType.get(), 10);
    assertThat(truncateInt.sourceTypeId()).isEqualTo(Type.TypeID.INTEGER);
    assertThat(truncateInt.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    Truncate<Long> truncateLong = Truncate.get(Types.LongType.get(), 10);
    assertThat(truncateLong.sourceTypeId()).isEqualTo(Type.TypeID.LONG);
    assertThat(truncateLong.resultTypeId()).isEqualTo(Type.TypeID.LONG);

    Truncate<CharSequence> truncateString = Truncate.get(Types.StringType.get(), 10);
    assertThat(truncateString.sourceTypeId()).isEqualTo(Type.TypeID.STRING);
    assertThat(truncateString.resultTypeId()).isEqualTo(Type.TypeID.STRING);

    Truncate<java.nio.ByteBuffer> truncateBinary = Truncate.get(Types.BinaryType.get(), 10);
    assertThat(truncateBinary.sourceTypeId()).isEqualTo(Type.TypeID.BINARY);
    assertThat(truncateBinary.resultTypeId()).isEqualTo(Type.TypeID.BINARY);

    Truncate<java.math.BigDecimal> truncateDecimal = Truncate.get(Types.DecimalType.of(10, 2), 10);
    assertThat(truncateDecimal.sourceTypeId()).isEqualTo(Type.TypeID.DECIMAL);
    assertThat(truncateDecimal.resultTypeId()).isEqualTo(Type.TypeID.DECIMAL);
  }

  @Test
  public void testBucketTypeIds() {
    Bucket<Integer> bucketInt = Bucket.get(Types.IntegerType.get(), 10);
    assertThat(bucketInt.sourceTypeId()).isEqualTo(Type.TypeID.INTEGER);
    assertThat(bucketInt.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    Bucket<Long> bucketLong = Bucket.get(Types.LongType.get(), 10);
    assertThat(bucketLong.sourceTypeId()).isEqualTo(Type.TypeID.LONG);
    assertThat(bucketLong.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    Bucket<CharSequence> bucketString = Bucket.get(Types.StringType.get(), 10);
    assertThat(bucketString.sourceTypeId()).isEqualTo(Type.TypeID.STRING);
    assertThat(bucketString.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    Bucket<java.nio.ByteBuffer> bucketBinary = Bucket.get(Types.BinaryType.get(), 10);
    assertThat(bucketBinary.sourceTypeId()).isEqualTo(Type.TypeID.BINARY);
    assertThat(bucketBinary.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    Bucket<java.util.UUID> bucketUUID = Bucket.get(Types.UUIDType.get(), 10);
    assertThat(bucketUUID.sourceTypeId()).isEqualTo(Type.TypeID.UUID);
    assertThat(bucketUUID.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    Bucket<java.math.BigDecimal> bucketDecimal = Bucket.get(Types.DecimalType.of(10, 2), 10);
    assertThat(bucketDecimal.sourceTypeId()).isEqualTo(Type.TypeID.DECIMAL);
    assertThat(bucketDecimal.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);
  }

  @Test
  public void testDatesTypeIds() {
    assertThat(Dates.YEAR.sourceTypeId()).isEqualTo(Type.TypeID.DATE);
    assertThat(Dates.YEAR.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    assertThat(Dates.MONTH.sourceTypeId()).isEqualTo(Type.TypeID.DATE);
    assertThat(Dates.MONTH.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    assertThat(Dates.DAY.sourceTypeId()).isEqualTo(Type.TypeID.DATE);
    assertThat(Dates.DAY.resultTypeId()).isEqualTo(Type.TypeID.DATE);
  }

  @Test
  public void testTimestampsTypeIds() {
    assertThat(Timestamps.MICROS_TO_YEAR.sourceTypeId()).isEqualTo(Type.TypeID.TIMESTAMP);
    assertThat(Timestamps.MICROS_TO_YEAR.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    assertThat(Timestamps.MICROS_TO_MONTH.sourceTypeId()).isEqualTo(Type.TypeID.TIMESTAMP);
    assertThat(Timestamps.MICROS_TO_MONTH.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    assertThat(Timestamps.MICROS_TO_DAY.sourceTypeId()).isEqualTo(Type.TypeID.TIMESTAMP);
    assertThat(Timestamps.MICROS_TO_DAY.resultTypeId()).isEqualTo(Type.TypeID.DATE);

    assertThat(Timestamps.MICROS_TO_HOUR.sourceTypeId()).isEqualTo(Type.TypeID.TIMESTAMP);
    assertThat(Timestamps.MICROS_TO_HOUR.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    assertThat(Timestamps.NANOS_TO_YEAR.sourceTypeId()).isEqualTo(Type.TypeID.TIMESTAMP_NANO);
    assertThat(Timestamps.NANOS_TO_YEAR.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    assertThat(Timestamps.NANOS_TO_MONTH.sourceTypeId()).isEqualTo(Type.TypeID.TIMESTAMP_NANO);
    assertThat(Timestamps.NANOS_TO_MONTH.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);

    assertThat(Timestamps.NANOS_TO_DAY.sourceTypeId()).isEqualTo(Type.TypeID.TIMESTAMP_NANO);
    assertThat(Timestamps.NANOS_TO_DAY.resultTypeId()).isEqualTo(Type.TypeID.DATE);

    assertThat(Timestamps.NANOS_TO_HOUR.sourceTypeId()).isEqualTo(Type.TypeID.TIMESTAMP_NANO);
    assertThat(Timestamps.NANOS_TO_HOUR.resultTypeId()).isEqualTo(Type.TypeID.INTEGER);
  }

  @Test
  public void testUnboundTransformsReturnNull() {
    // Unbound transforms should return null for both methods
    assertThat(Truncate.get(10).sourceTypeId()).isNull();
    assertThat(Truncate.get(10).resultTypeId()).isNull();

    assertThat(Bucket.get(10).sourceTypeId()).isNull();
    assertThat(Bucket.get(10).resultTypeId()).isNull();

    assertThat(Days.get().sourceTypeId()).isNull();
    assertThat(Days.get().resultTypeId()).isNull();

    assertThat(Hours.get().sourceTypeId()).isNull();
    assertThat(Hours.get().resultTypeId()).isNull();

    assertThat(Months.get().sourceTypeId()).isNull();
    assertThat(Months.get().resultTypeId()).isNull();

    assertThat(Years.get().sourceTypeId()).isNull();
    assertThat(Years.get().resultTypeId()).isNull();

    assertThat(Identity.get().sourceTypeId()).isNull();
    assertThat(Identity.get().resultTypeId()).isNull();

    assertThat(VoidTransform.get().sourceTypeId()).isNull();
    assertThat(VoidTransform.get().resultTypeId()).isNull();
  }
}
