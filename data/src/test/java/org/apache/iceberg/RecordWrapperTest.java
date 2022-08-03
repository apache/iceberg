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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.Assert;
import org.junit.Test;

public abstract class RecordWrapperTest {

  private static final Types.StructType PRIMITIVE_WITHOUT_TIME =
      Types.StructType.of(
          required(100, "id", Types.LongType.get()),
          optional(101, "data", Types.StringType.get()),
          required(102, "b", Types.BooleanType.get()),
          optional(103, "i", Types.IntegerType.get()),
          required(104, "l", Types.LongType.get()),
          optional(105, "f", Types.FloatType.get()),
          required(106, "d", Types.DoubleType.get()),
          optional(107, "date", Types.DateType.get()),
          required(108, "ts_tz", Types.TimestampType.withZone()),
          required(110, "s", Types.StringType.get()),
          required(112, "fixed", Types.FixedType.ofLength(7)),
          optional(113, "bytes", Types.BinaryType.get()),
          required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
          required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
          required(116, "dec_38_10", Types.DecimalType.of(38, 10)) // maximum precision
          );

  private static final Types.StructType TIMESTAMP_WITHOUT_ZONE =
      Types.StructType.of(
          required(101, "ts0", Types.TimestampType.withoutZone()),
          required(102, "ts1", Types.TimestampType.withoutZone()));

  protected static final Types.StructType TIME =
      Types.StructType.of(
          required(100, "time0", Types.TimeType.get()),
          optional(101, "time1", Types.TimeType.get()));

  @Test
  public void testSimpleStructWithoutTime() {
    generateAndValidate(new Schema(PRIMITIVE_WITHOUT_TIME.fields()));
  }

  @Test
  public void testTimestampWithoutZone() {
    generateAndValidate(new Schema(TIMESTAMP_WITHOUT_ZONE.fields()));
  }

  @Test
  public void testTime() {
    generateAndValidate(new Schema(TIME.fields()));
  }

  @Test
  public void testNestedSchema() {
    Types.StructType structType =
        Types.StructType.of(
            required(0, "id", Types.LongType.get()),
            required(
                1,
                "level1",
                Types.StructType.of(
                    optional(
                        2,
                        "level2",
                        Types.StructType.of(
                            required(
                                3,
                                "level3",
                                Types.StructType.of(
                                    optional(
                                        4,
                                        "level4",
                                        Types.StructType.of(
                                            required(
                                                5,
                                                "level5",
                                                Types.StructType.of(
                                                    PRIMITIVE_WITHOUT_TIME.fields())))))))))));

    generateAndValidate(new Schema(structType.fields()));
  }

  private void generateAndValidate(Schema schema) {
    generateAndValidate(schema, Assert::assertEquals);
  }

  public interface AssertMethod {
    void assertEquals(String message, StructLikeWrapper expected, StructLikeWrapper actual);
  }

  protected abstract void generateAndValidate(Schema schema, AssertMethod assertMethod);
}
