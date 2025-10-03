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
import java.util.Iterator;
import org.apache.flink.table.data.RowData;
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
    // Create a specific timestamp with nanosecond precision
    LocalDateTime testTime = LocalDateTime.of(2025, 10, 2, 10, 15, 30, 123456789);
    long expectedMicros = DateTimeUtil.microsFromTimestamp(testTime);
    long expectedNanos = DateTimeUtil.nanosFromTimestamp(testTime);

    // Microsecond precision schema (should use microsFromTimestamp)
    Schema microsSchema =
        new Schema(
            Types.NestedField.required(1, "timestamp_micros", Types.TimestampType.withoutZone()));

    // Nanosecond precision schema (should use nanosFromTimestamp)
    Schema nanosSchema =
        new Schema(
            Types.NestedField.required(
                1, "timestamp_nanos", Types.TimestampNanoType.withoutZone()));

    // Create wrappers for both schemas
    RowDataWrapper microsWrapper =
        new RowDataWrapper(FlinkSchemaUtil.convert(microsSchema), microsSchema.asStruct());
    RowDataWrapper nanosWrapper =
        new RowDataWrapper(FlinkSchemaUtil.convert(nanosSchema), nanosSchema.asStruct());

    // Test with the same seed to get comparable data
    RowData microsRowData = RandomRowData.generate(microsSchema, 1, 42L).iterator().next();
    RowData nanosRowData = RandomRowData.generate(nanosSchema, 1, 42L).iterator().next();

    StructLike microsStructLike = microsWrapper.wrap(microsRowData);
    StructLike nanosStructLike = nanosWrapper.wrap(nanosRowData);

    Long microsValue = microsStructLike.get(0, Long.class);
    Long nanosValue = nanosStructLike.get(0, Long.class);

    // Verify both values are not null
    assertThat(microsValue).isNotNull();
    assertThat(nanosValue).isNotNull();

    // Calculate the actual ratio
    double ratio = (double) nanosValue / microsValue;

    // For the same timestamp, nanosecond precision should produce values that are
    // approximately 1000x larger than microsecond precision
    assertThat(nanosValue).isGreaterThan(microsValue); // Nanosecond should be larger

    // The ratio should be close to 1000 (nanoseconds vs microseconds)
    // Allow some tolerance since we're using random data
    assertThat(ratio).isGreaterThan(100.0); // At least 100x larger
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
