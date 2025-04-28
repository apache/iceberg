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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class GenericRecordCopyTest {

  @Test
  public void deepCopyValues() {
    Schema schema =
        new Schema(
            optional(0, "binaryData", Types.BinaryType.get()),
            optional(
                1,
                "structData",
                Types.StructType.of(required(100, "structInnerData", Types.StringType.get()))),
            optional(
                2,
                "mapData",
                Types.MapType.ofOptional(101, 102, Types.StringType.get(), Types.StringType.get())),
            optional(3, "listData", Types.ListType.ofOptional(103, Types.StringType.get())),
            optional(4, "fixedData", Types.FixedType.ofLength(11)),
            optional(5, "dateData", Types.DateType.get()),
            optional(6, "timeData", Types.TimeType.get()),
            optional(7, "timestampWithoutZoneData", Types.TimestampType.withoutZone()),
            optional(8, "timestampWithZoneData", Types.TimestampType.withZone()));

    GenericRecord original = GenericRecord.create(schema);
    original.setField("binaryData", ByteBuffer.wrap("binaryData_0".getBytes()));
    Record structRecord = GenericRecord.create(schema.findType("structData").asStructType());
    structRecord.setField("structInnerData", "structInnerData_1");
    original.setField("structData", structRecord);
    original.setField("mapData", Maps.asMap(Sets.newHashSet("mapData_2"), (k) -> k));
    original.setField("listData", Lists.newArrayList("listData_3"));
    original.setField("fixedData", "fixedData_4".getBytes());
    original.setField("dateData", LocalDate.now());
    original.setField("timeData", LocalTime.now());
    original.setField("timestampWithoutZoneData", LocalDateTime.now());
    original.setField("timestampWithZoneData", LocalDateTime.now());

    GenericRecord shallowCopy = original.copy();
    for (Types.NestedField field : schema.columns()) {
      assertThat(shallowCopy.getField(field.name()) == original.getField(field.name())).isTrue();
    }

    GenericRecord deepCopy = original.deepCopyValues();
    for (Types.NestedField field : schema.columns()) {
      System.out.println(field.name());
      assertThat(deepCopy.getField(field.name())).isEqualTo(original.getField(field.name()));
      assertThat(deepCopy.getField(field.name()) == original.getField(field.name())).isFalse();
    }
  }
}
