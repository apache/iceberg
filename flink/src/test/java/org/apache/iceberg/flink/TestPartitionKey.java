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

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.data.RandomData;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestPartitionKey {

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "dateType", Types.DateType.get()),
      Types.NestedField.optional(3, "timeType", Types.TimeType.get()),
      Types.NestedField.optional(4, "timestampWithoutZone", Types.TimestampType.withoutZone()),
      Types.NestedField.required(5, "timestampWithZone", Types.TimestampType.withZone()),
      Types.NestedField.optional(6, "fixedType", Types.FixedType.ofLength(5)),
      Types.NestedField.optional(7, "uuidType", Types.UUIDType.get()),
      Types.NestedField.optional(8, "binaryType", Types.BinaryType.get()),
      Types.NestedField.optional(9, "decimalType1", Types.DecimalType.of(3, 14)),
      Types.NestedField.optional(10, "decimalType2", Types.DecimalType.of(10, 20)),
      Types.NestedField.optional(11, "decimalType3", Types.DecimalType.of(38, 19)),
      Types.NestedField.optional(12, "floatType", Types.FloatType.get()),
      Types.NestedField.required(13, "doubleType", Types.DoubleType.get())
  );

  private static final String[] SUPPORTED_PRIMITIVES = new String[] {
      "id", "dateType", "timeType", "timestampWithoutZone", "timestampWithZone", "fixedType", "uuidType",
      "binaryType", "decimalType1", "decimalType2", "decimalType3", "floatType", "doubleType"
  };

  private static final Schema NESTED_SCHEMA = new Schema(
      Types.NestedField.required(1, "structType", Types.StructType.of(
          Types.NestedField.optional(2, "innerStringType", Types.StringType.get()),
          Types.NestedField.optional(3, "innerIntegerType", Types.IntegerType.get())
      ))
  );

  @Test
  public void testNullPartitionValue() {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("data")
        .build();

    List<Row> rows = Lists.newArrayList(
        Row.of(1, "a"),
        Row.of(2, "b"),
        Row.of(3, null)
    );

    RowWrapper rowWrapper = new RowWrapper(schema.asStruct());

    for (Row row : rows) {
      PartitionKey partitionKey = new PartitionKey(spec, schema);
      partitionKey.partition(rowWrapper.wrap(row));
      Assert.assertEquals(partitionKey.size(), 1);
      Assert.assertEquals(partitionKey.get(0, String.class), row.getField(1));
    }
  }

  @Test
  public void testPartitionWithOneNestedField() {
    RowWrapper rowWrapper = new RowWrapper(NESTED_SCHEMA.asStruct());
    Iterable<Row> rows = RandomData.generate(NESTED_SCHEMA, 10, 1991);

    PartitionSpec spec1 = PartitionSpec.builderFor(NESTED_SCHEMA)
        .identity("structType.innerStringType")
        .build();
    PartitionSpec spec2 = PartitionSpec.builderFor(NESTED_SCHEMA)
        .identity("structType.innerIntegerType")
        .build();

    for (Row row : rows) {
      Row innerRow = (Row) row.getField(0);

      PartitionKey partitionKey1 = new PartitionKey(spec1, NESTED_SCHEMA);
      partitionKey1.partition(rowWrapper.wrap(row));
      Object innerStringValue = innerRow.getField(0);
      Assert.assertEquals(partitionKey1.size(), 1);
      Assert.assertEquals(partitionKey1.get(0, String.class), innerStringValue);

      PartitionKey partitionKey2 = new PartitionKey(spec2, NESTED_SCHEMA);
      partitionKey2.partition(rowWrapper.wrap(row));
      Object innerIntegerValue = innerRow.getField(1);
      Assert.assertEquals(partitionKey2.size(), 1);
      Assert.assertEquals(partitionKey2.get(0, Integer.class), innerIntegerValue);
    }
  }

  @Test
  public void testPartitionMultipleNestedField() {
    RowWrapper rowWrapper = new RowWrapper(NESTED_SCHEMA.asStruct());
    Iterable<Row> rows = RandomData.generate(NESTED_SCHEMA, 10, 1992);

    PartitionSpec spec1 = PartitionSpec.builderFor(NESTED_SCHEMA)
        .identity("structType.innerIntegerType")
        .identity("structType.innerStringType")
        .build();
    PartitionSpec spec2 = PartitionSpec.builderFor(NESTED_SCHEMA)
        .identity("structType.innerStringType")
        .identity("structType.innerIntegerType")
        .build();

    PartitionKey pk1 = new PartitionKey(spec1, NESTED_SCHEMA);
    PartitionKey pk2 = new PartitionKey(spec2, NESTED_SCHEMA);

    for (Row row : rows) {
      Row innerRow = (Row) row.getField(0);

      pk1.partition(rowWrapper.wrap(row));
      Assert.assertEquals(2, pk1.size());
      Assert.assertEquals(innerRow.getField(1), pk1.get(0, Integer.class));
      Assert.assertEquals(innerRow.getField(0), pk1.get(1, String.class));

      pk2.partition(rowWrapper.wrap(row));
      Assert.assertEquals(2, pk2.size());
      Assert.assertEquals(innerRow.getField(0), pk2.get(0, String.class));
      Assert.assertEquals(innerRow.getField(1), pk2.get(1, Integer.class));
    }
  }

  private static Object transform(Object value, Type type) {
    if (value == null) {
      return null;
    }
    switch (type.typeId()) {
      case DATE:
        return DateTimeUtil.daysFromDate((LocalDate) value);
      case TIME:
        return DateTimeUtil.microsFromTime((LocalTime) value);
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return DateTimeUtil.microsFromTimestamptz((OffsetDateTime) value);
        } else {
          return DateTimeUtil.microsFromTimestamp((LocalDateTime) value);
        }
      case FIXED:
        return ByteBuffer.wrap((byte[]) value);
      default:
        return value;
    }
  }

  @Test
  public void testPartitionValueTypes() {
    RowWrapper rowWrapper = new RowWrapper(SCHEMA.asStruct());
    Iterable<Row> rows = RandomData.generate(SCHEMA, 10, 1993);

    for (int i = 0; i < SUPPORTED_PRIMITIVES.length; i++) {
      String column = SUPPORTED_PRIMITIVES[i];

      PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity(column).build();
      Type type = spec.schema().findType(column);
      Class<?>[] javaClasses = spec.javaClasses();

      PartitionKey pk = new PartitionKey(spec, SCHEMA);

      for (Row row : rows) {
        pk.partition(rowWrapper.wrap(row));
        Object expected = row.getField(i);
        Assert.assertEquals("Partition with column " + column + " should have one field.", 1, pk.size());
        Assert.assertEquals("Partition with column " + column + " should have the expected values",
            transform(expected, type), pk.get(0, javaClasses[0]));
      }
    }
  }

  @Test
  public void testNestedPartitionValues() {
    Schema nestedSchema = new Schema(Types.NestedField.optional(1001, "nested", SCHEMA.asStruct()));
    RowWrapper rowWrapper = new RowWrapper(nestedSchema.asStruct());
    Iterable<Row> rows = RandomData.generate(nestedSchema, 10, 1994);

    for (int i = 0; i < SUPPORTED_PRIMITIVES.length; i++) {
      String column = String.format("nested.%s", SUPPORTED_PRIMITIVES[i]);

      PartitionSpec spec = PartitionSpec.builderFor(nestedSchema).identity(column).build();
      Type type = spec.schema().findType(column);
      Class<?>[] javaClasses = spec.javaClasses();

      PartitionKey pk = new PartitionKey(spec, nestedSchema);

      for (Row row : rows) {
        pk.partition(rowWrapper.wrap(row));

        Object expected = ((Row) row.getField(0)).getField(i);
        Assert.assertEquals("Partition with nested column " + column + " should have one field.",
            1, pk.size());
        Assert.assertEquals("Partition with nested column " + column + "should have the expected values.",
            transform(expected, type), pk.get(0, javaClasses[0]));
      }
    }
  }
}
