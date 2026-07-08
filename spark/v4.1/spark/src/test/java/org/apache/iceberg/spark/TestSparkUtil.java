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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalTime;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.junit.jupiter.api.Test;

public class TestSparkUtil {

  private static final LocalTime TIME = LocalTime.of(10, 20, 30, 123_456_000);

  @Test
  public void testInternalToSparkConvertsTimeToNanos() {
    Object converted =
        SparkUtil.internalToSpark(Types.TimeType.get(), DateTimeUtil.microsFromTime(TIME));

    assertThat(converted)
        .as("Time value should be converted to the nanoseconds Spark expects")
        .isEqualTo(TIME.toNanoOfDay());
  }

  @Test
  public void testInternalToSparkConvertsNestedTimeToNanos() {
    Types.StructType structType =
        Types.StructType.of(Types.NestedField.optional(1, "t", Types.TimeType.get()));
    GenericRecord rec = GenericRecord.create(structType);
    rec.set(0, DateTimeUtil.microsFromTime(TIME));

    GenericInternalRow row = (GenericInternalRow) SparkUtil.internalToSpark(structType, rec);

    assertThat(row.getLong(0))
        .as("Nested time value should be converted to the nanoseconds Spark expects")
        .isEqualTo(TIME.toNanoOfDay());
  }
}
