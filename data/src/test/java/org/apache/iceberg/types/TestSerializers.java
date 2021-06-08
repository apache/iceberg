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

package org.apache.iceberg.types;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSerializers {

  private static final Types.StructType SUPPORTED_PRIMITIVES = Types.StructType.of(
      required(100, "id", Types.LongType.get()),
      optional(101, "data", Types.StringType.get()),
      required(102, "b", Types.BooleanType.get()),
      optional(103, "i", Types.IntegerType.get()),
      required(104, "l", Types.LongType.get()),
      optional(105, "f", Types.FloatType.get()),
      required(106, "d", Types.DoubleType.get()),
      optional(107, "date", Types.DateType.get()),
      required(108, "ts_tz", Types.TimestampType.withZone()),
      required(109, "ts", Types.TimestampType.withoutZone()),
      required(110, "s", Types.StringType.get()),
      required(112, "fixed", Types.FixedType.ofLength(7)),
      optional(113, "bytes", Types.BinaryType.get()),
      required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
      required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
      required(116, "dec_38_10", Types.DecimalType.of(38, 10)), // maximum precision
      required(117, "time", Types.TimeType.get())
  );

  @Test
  public void testSerializers() {
    Schema iSchema = new Schema(SUPPORTED_PRIMITIVES.fields());
    List<Record> records = RandomGenericData.generate(iSchema, 100, 1_000_000_1);

    Serializer<StructLike> serializer = Serializers.forType(iSchema.asStruct());
    InternalRecordWrapper recordWrapper = new InternalRecordWrapper(iSchema.asStruct());
    for (Record expectedRecord : records) {
      byte[] expectedData = serializer.serialize(recordWrapper.wrap(expectedRecord));
      Record actualRecord = (Record) serializer.deserialize(expectedData);
      byte[] actualData = serializer.serialize(actualRecord);
      Assert.assertArrayEquals("Should have the expected serialized data", expectedData, actualData);
    }
  }
}
