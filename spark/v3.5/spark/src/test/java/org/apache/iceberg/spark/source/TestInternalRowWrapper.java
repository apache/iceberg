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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.iceberg.RecordWrapperTestBase;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestInternalRowWrapper extends RecordWrapperTestBase {

  @Disabled
  @Override
  public void testTimestampWithoutZone() {
    // Spark does not support timestamp without zone.
  }

  @Disabled
  @Override
  public void testTime() {
    // Spark does not support time fields.
  }

  @Disabled
  @Override
  public void testTimestampNanoWithoutZone() {
    // Spark does not support nanosecond timestamp without zone.
  }

  @Disabled
  @Override
  public void testTimestampNanoWithZone() {
    // Spark does not support nanosecond timestamp with zone.
  }

  @Test
  void wrapsAFileColumn() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));
    InternalRowWrapper wrapper =
        new InternalRowWrapper(SparkSchemaUtil.convert(schema), schema.asStruct());

    InternalRow photo =
        new GenericInternalRow(
            new Object[] {
              UTF8String.fromString("s3://bucket/photo.png"),
              0L,
              12L,
              UTF8String.fromString("image/png"),
              UTF8String.fromString("d41d8cd9"),
              new byte[] {1, 2}
            });

    StructLike wrapped = wrapper.wrap(new GenericInternalRow(new Object[] {1L, photo}));
    StructLike wrappedPhoto = wrapped.get(1, StructLike.class);

    assertThat(wrapped.get(0, Long.class)).isEqualTo(1L);
    assertThat(wrappedPhoto.get(0, String.class)).isEqualTo("s3://bucket/photo.png");
    assertThat(wrappedPhoto.get(1, Long.class)).isEqualTo(0L);
    assertThat(wrappedPhoto.get(2, Long.class)).isEqualTo(12L);
    assertThat(wrappedPhoto.get(3, String.class)).isEqualTo("image/png");
    assertThat(wrappedPhoto.get(4, String.class)).isEqualTo("d41d8cd9");
    assertThat(wrappedPhoto.get(5, ByteBuffer.class)).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2}));
  }

  @Override
  protected void generateAndValidate(Schema schema, AssertMethod assertMethod) {
    int numRecords = 100;
    Iterable<Record> recordList = RandomGenericData.generate(schema, numRecords, 101L);
    Iterable<InternalRow> rowList = RandomData.generateSpark(schema, numRecords, 101L);

    InternalRecordWrapper recordWrapper = new InternalRecordWrapper(schema.asStruct());
    InternalRowWrapper rowWrapper =
        new InternalRowWrapper(SparkSchemaUtil.convert(schema), schema.asStruct());

    Iterator<Record> actual = recordList.iterator();
    Iterator<InternalRow> expected = rowList.iterator();

    StructLikeWrapper actualWrapper = StructLikeWrapper.forType(schema.asStruct());
    StructLikeWrapper expectedWrapper = StructLikeWrapper.forType(schema.asStruct());
    for (int i = 0; i < numRecords; i++) {
      assertThat(actual).as("Should have more records").hasNext();
      assertThat(expected).as("Should have more InternalRow").hasNext();

      StructLike recordStructLike = recordWrapper.wrap(actual.next());
      StructLike rowStructLike = rowWrapper.wrap(expected.next());

      assertMethod.assertEquals(
          "Should have expected StructLike values",
          actualWrapper.set(recordStructLike),
          expectedWrapper.set(rowStructLike));
    }

    assertThat(actual).as("Shouldn't have more record").isExhausted();
    assertThat(expected).as("Shouldn't have more InternalRow").isExhausted();
  }
}
