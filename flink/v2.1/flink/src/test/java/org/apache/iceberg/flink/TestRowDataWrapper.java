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
package org.apache.iceberg.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.RecordWrapperTestBase;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.data.RandomRowData;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.jupiter.api.Test;

public class TestRowDataWrapper extends RecordWrapperTestBase {

  /**
   * Flink's time type has been truncated to millis seconds, so we need a customized assert method
   * to check the values.
   */
  @Override
  public void testTime() {
    generateAndValidate(
        new Schema(TIME.fields()),
        (message, expectedWrapper, actualWrapper) -> {
          for (int pos = 0; pos < TIME.fields().size(); pos++) {
            Object expected = expectedWrapper.get().get(pos, Object.class);
            Object actual = actualWrapper.get().get(pos, Object.class);
            if (expected == actual) {
              return;
            }

            assertThat(actual).isNotNull();
            assertThat(expected).isNotNull();

            int expectedMilliseconds = (int) ((long) expected / 1000_000);
            int actualMilliseconds = (int) ((long) actual / 1000_000);
            assertThat(actualMilliseconds).as(message).isEqualTo(expectedMilliseconds);
          }
        });
  }

  /** Test that nanosecond precision timestamps are preserved correctly. */
  @Test
  public void testNanosecondTimestampPrecision() {
    // Test multiple timestamp values with different nanosecond precisions
    // Focus on last non-zero digit to validate nanosecond precision preservation
    LocalDateTime[] testTimes = {
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 0),           // No nanoseconds
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 123456789),   // Last non-zero: 9
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 999999999),   // Last non-zero: 9
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 1),           // Last non-zero: 1
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 123456),      // Last non-zero: 6
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 100000000),   // Last non-zero: 1
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 200000000),   // Last non-zero: 2
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 300000000),   // Last non-zero: 3
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 400000000),   // Last non-zero: 4
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 500000000),   // Last non-zero: 5
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 600000000),   // Last non-zero: 6
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 700000000),   // Last non-zero: 7
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 800000000),   // Last non-zero: 8
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 900000000),   // Last non-zero: 9
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 123456789),   // Last non-zero: 9
        LocalDateTime.of(2023, 1, 1, 12, 0, 0, 987654321)    // Last non-zero: 1
    };

    for (LocalDateTime testTime : testTimes) {
      testTimestampPrecision(testTime);
    }
  }

  private void testTimestampPrecision(LocalDateTime testTime) {
    // Create schemas for both precision types
    Schema microsSchema = new Schema(
        Types.NestedField.required(1, "timestamp_micros", Types.TimestampType.withoutZone()));
    Schema nanosSchema = new Schema(
        Types.NestedField.required(1, "timestamp_nanos", Types.TimestampNanoType.withoutZone()));

    // Create wrappers
    RowDataWrapper microsWrapper = new RowDataWrapper(
        FlinkSchemaUtil.convert(microsSchema), microsSchema.asStruct());
    RowDataWrapper nanosWrapper = new RowDataWrapper(
        FlinkSchemaUtil.convert(nanosSchema), nanosSchema.asStruct());

    // Create RowData with the test timestamp
    GenericRowData microsRowData = new GenericRowData(1);
    microsRowData.setField(0, TimestampData.fromLocalDateTime(testTime));
    
    GenericRowData nanosRowData = new GenericRowData(1);
    nanosRowData.setField(0, TimestampData.fromLocalDateTime(testTime));

    // Wrap and extract values
    StructLike microsStructLike = microsWrapper.wrap(microsRowData);
    StructLike nanosStructLike = nanosWrapper.wrap(nanosRowData);

    Long microsValue = microsStructLike.get(0, Long.class);
    Long nanosValue = nanosStructLike.get(0, Long.class);

    // Verify values are not null
    assertThat(microsValue).isNotNull();
    assertThat(nanosValue).isNotNull();

    // Calculate expected values using DateTimeUtil
    long expectedMicros = DateTimeUtil.microsFromTimestamp(testTime);
    long expectedNanos = DateTimeUtil.nanosFromTimestamp(testTime);

    // Verify microsecond precision
    assertThat(microsValue)
        .as("Microsecond value should match DateTimeUtil.microsFromTimestamp for %s", testTime)
        .isEqualTo(expectedMicros);

    // Verify nanosecond precision
    assertThat(nanosValue)
        .as("Nanosecond value should match DateTimeUtil.nanosFromTimestamp for %s", testTime)
        .isEqualTo(expectedNanos);

    // Verify nanosecond precision using last non-zero digit
    int originalNanos = testTime.getNano();
    int lastNonZeroDigit = getLastNonZeroDigit(originalNanos);
    
    if (originalNanos > 0) {
      // Extract the last non-zero digit from the nanosecond value
      int actualLastNonZeroDigit = getLastNonZeroDigit((int) (nanosValue % 1_000_000_000));
      
      assertThat(actualLastNonZeroDigit)
          .as("Last non-zero digit should be preserved for nanosecond precision. Original: %d, Actual: %d, Timestamp: %s", 
              lastNonZeroDigit, actualLastNonZeroDigit, testTime)
          .isEqualTo(lastNonZeroDigit);
    }

    // Verify that nanosecond precision is actually higher than microsecond precision
    // when there are sub-microsecond components
    int nanosInMicro = testTime.getNano() % 1000;
    if (nanosInMicro > 0) {
      assertThat(nanosValue)
          .as("Nanosecond value should be different from microsecond value when sub-microsecond precision exists for %s", testTime)
          .isNotEqualTo(microsValue);
    }

    // Verify the relationship between microsecond and nanosecond values
    // Nanosecond value should be 1000x microsecond value (plus any sub-microsecond precision)
    long expectedNanosFromMicros = microsValue * 1000 + nanosInMicro;
    assertThat(nanosValue)
        .as("Nanosecond value should be 1000x microsecond value plus sub-microsecond precision for %s", testTime)
        .isEqualTo(expectedNanosFromMicros);
  }

  /**
   * Extract the last non-zero digit from a number.
   * For example: 123456789 -> 9, 100000000 -> 1, 123456 -> 6
   */
  private int getLastNonZeroDigit(int value) {
    if (value == 0) {
      return 0;
    }
    
    while (value > 0) {
      int digit = value % 10;
      if (digit != 0) {
        return digit;
      }
      value /= 10;
    }
    
    return 0;
  }

  @Test
  public void testNanosecondTimestampPrecisionWithTimeZone() {
    // Test with timezone-aware timestamps using last non-zero digit validation
    OffsetDateTime[] testTimes = {
        OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 123456789, ZoneOffset.UTC),  // Last non-zero: 9
        OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 987654321, ZoneOffset.UTC),  // Last non-zero: 1
        OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 100000000, ZoneOffset.UTC),  // Last non-zero: 1
        OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 200000000, ZoneOffset.UTC),  // Last non-zero: 2
        OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 300000000, ZoneOffset.UTC),  // Last non-zero: 3
        OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 400000000, ZoneOffset.UTC),  // Last non-zero: 4
        OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 500000000, ZoneOffset.UTC),  // Last non-zero: 5
        OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 600000000, ZoneOffset.UTC),  // Last non-zero: 6
        OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 700000000, ZoneOffset.UTC),  // Last non-zero: 7
        OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 800000000, ZoneOffset.UTC),  // Last non-zero: 8
        OffsetDateTime.of(2023, 1, 1, 12, 0, 0, 900000000, ZoneOffset.UTC)   // Last non-zero: 9
    };

    for (OffsetDateTime testTime : testTimes) {
      testTimestampPrecisionWithTimeZone(testTime);
    }
  }

  private void testTimestampPrecisionWithTimeZone(OffsetDateTime testTime) {
    Schema microsSchema = new Schema(
        Types.NestedField.required(1, "timestamp_micros", Types.TimestampType.withZone()));
    Schema nanosSchema = new Schema(
        Types.NestedField.required(1, "timestamp_nanos", Types.TimestampNanoType.withZone()));

    RowDataWrapper microsWrapper = new RowDataWrapper(
        FlinkSchemaUtil.convert(microsSchema), microsSchema.asStruct());
    RowDataWrapper nanosWrapper = new RowDataWrapper(
        FlinkSchemaUtil.convert(nanosSchema), nanosSchema.asStruct());

    GenericRowData microsRowData = new GenericRowData(1);
    microsRowData.setField(0, TimestampData.fromLocalDateTime(testTime.toLocalDateTime()));
    
    GenericRowData nanosRowData = new GenericRowData(1);
    nanosRowData.setField(0, TimestampData.fromLocalDateTime(testTime.toLocalDateTime()));

    StructLike microsStructLike = microsWrapper.wrap(microsRowData);
    StructLike nanosStructLike = nanosWrapper.wrap(nanosRowData);

    Long microsValue = microsStructLike.get(0, Long.class);
    Long nanosValue = nanosStructLike.get(0, Long.class);

    assertThat(microsValue).isNotNull();
    assertThat(nanosValue).isNotNull();

    long expectedMicros = DateTimeUtil.microsFromTimestamptz(testTime);
    long expectedNanos = DateTimeUtil.nanosFromTimestamptz(testTime);

    assertThat(microsValue).isEqualTo(expectedMicros);
    assertThat(nanosValue).isEqualTo(expectedNanos);

    // Verify nanosecond precision using last non-zero digit
    int originalNanos = testTime.getNano();
    int lastNonZeroDigit = getLastNonZeroDigit(originalNanos);
    
    if (originalNanos > 0) {
      // Extract the last non-zero digit from the nanosecond value
      int actualLastNonZeroDigit = getLastNonZeroDigit((int) (nanosValue % 1_000_000_000));
      
      assertThat(actualLastNonZeroDigit)
          .as("Last non-zero digit should be preserved for timezone-aware nanosecond precision. Original: %d, Actual: %d, Timestamp: %s", 
              lastNonZeroDigit, actualLastNonZeroDigit, testTime)
          .isEqualTo(lastNonZeroDigit);
    }
  }

  @Test
  public void testNanosecondTimestampPrecisionEdgeCases() {
    // Test edge cases with last non-zero digit validation
    LocalDateTime[] edgeCases = {
        LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0),           // Epoch - no nanoseconds
        LocalDateTime.of(2038, 1, 19, 3, 14, 7, 999999999), // Near 32-bit limit - Last non-zero: 9
        LocalDateTime.of(2000, 2, 29, 12, 0, 0, 123456789), // Leap year - Last non-zero: 9
        LocalDateTime.of(2023, 12, 31, 23, 59, 59, 999999999), // End of year - Last non-zero: 9
        LocalDateTime.of(1970, 1, 1, 0, 0, 0, 1),           // Epoch + 1 nanosecond - Last non-zero: 1
        LocalDateTime.of(2038, 1, 19, 3, 14, 7, 100000000), // Near 32-bit limit - Last non-zero: 1
        LocalDateTime.of(2000, 2, 29, 12, 0, 0, 200000000), // Leap year - Last non-zero: 2
        LocalDateTime.of(2023, 12, 31, 23, 59, 59, 300000000) // End of year - Last non-zero: 3
    };

    for (LocalDateTime testTime : edgeCases) {
      testTimestampPrecision(testTime);
    }
  }

  @Override
  protected void generateAndValidate(
      Schema schema, RecordWrapperTestBase.AssertMethod assertMethod) {
    int numRecords = 100;
    Iterable<Record> recordList = RandomGenericData.generate(schema, numRecords, 101L);
    Iterable<RowData> rowDataList = RandomRowData.generate(schema, numRecords, 101L);

    InternalRecordWrapper recordWrapper = new InternalRecordWrapper(schema.asStruct());
    RowDataWrapper rowDataWrapper =
        new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());

    Iterator<Record> actual = recordList.iterator();
    Iterator<RowData> expected = rowDataList.iterator();

    StructLikeWrapper actualWrapper = StructLikeWrapper.forType(schema.asStruct());
    StructLikeWrapper expectedWrapper = StructLikeWrapper.forType(schema.asStruct());
    for (int i = 0; i < numRecords; i++) {
      assertThat(actual).hasNext();
      assertThat(expected).hasNext();

      StructLike recordStructLike = recordWrapper.wrap(actual.next());
      StructLike rowDataStructLike = rowDataWrapper.wrap(expected.next());

      assertMethod.assertEquals(
          "Should have expected StructLike values",
          expectedWrapper.set(rowDataStructLike),
          actualWrapper.set(recordStructLike));
    }

    assertThat(actual).isExhausted();
    assertThat(expected).isExhausted();
  }
}
