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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalTime;
import java.util.Arrays;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.TimeType$;
import org.junit.jupiter.api.Test;

public class TestStructInternalRowTime {

  private static final LocalTime TIME = LocalTime.of(10, 20, 30, 123_456_000);

  private static final Types.StructType TIME_STRUCT =
      Types.StructType.of(Types.NestedField.optional(1, "time_col", Types.TimeType.get()));

  @Test
  public void testGetLongConvertsMicrosToNanos() {
    GenericRecord rec = GenericRecord.create(TIME_STRUCT);
    // Iceberg's internal representation stores time as microseconds from midnight
    rec.set(0, DateTimeUtil.microsFromTime(TIME));

    InternalRow row = new StructInternalRow(TIME_STRUCT).setStruct(rec);

    assertThat(row.getLong(0))
        .as("Time value should be converted to the nanoseconds Spark expects")
        .isEqualTo(TIME.toNanoOfDay());
  }

  @Test
  public void testGetLongConvertsLocalTimeToNanos() {
    GenericRecord rec = GenericRecord.create(TIME_STRUCT);
    // Iceberg generics represent time as LocalTime
    rec.set(0, TIME);

    InternalRow row = new StructInternalRow(TIME_STRUCT).setStruct(rec);

    assertThat(row.getLong(0))
        .as("LocalTime value should be converted to the nanoseconds Spark expects")
        .isEqualTo(TIME.toNanoOfDay());
  }

  @Test
  public void testGetWithTimeTypeReturnsNanos() {
    GenericRecord rec = GenericRecord.create(TIME_STRUCT);
    rec.set(0, DateTimeUtil.microsFromTime(TIME));

    InternalRow row = new StructInternalRow(TIME_STRUCT).setStruct(rec);

    assertThat(row.get(0, TimeType$.MODULE$.apply()))
        .as("Generic get with TimeType should return nanoseconds, not null")
        .isEqualTo(TIME.toNanoOfDay());
  }

  @Test
  public void testTimeArrayConvertsMicrosToNanos() {
    Types.StructType structType =
        Types.StructType.of(
            Types.NestedField.optional(
                1, "times", Types.ListType.ofOptional(2, Types.TimeType.get())));

    LocalTime otherTime = LocalTime.of(23, 59, 59, 999_999_000);
    GenericRecord rec = GenericRecord.create(structType);
    rec.set(
        0,
        Arrays.asList(DateTimeUtil.microsFromTime(TIME), DateTimeUtil.microsFromTime(otherTime)));

    InternalRow row = new StructInternalRow(structType).setStruct(rec);
    ArrayData array = row.getArray(0);

    assertThat(array.getLong(0)).isEqualTo(TIME.toNanoOfDay());
    assertThat(array.getLong(1)).isEqualTo(otherTime.toNanoOfDay());
  }
}
