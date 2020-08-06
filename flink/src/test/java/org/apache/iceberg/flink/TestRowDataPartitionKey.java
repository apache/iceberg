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
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.data.RandomRowData;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestRowDataPartitionKey {
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(0, "boolType", Types.BooleanType.get()),
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "longType", Types.LongType.get()),
      Types.NestedField.required(3, "dateType", Types.DateType.get()),
      Types.NestedField.required(4, "timeType", Types.TimeType.get()),
      Types.NestedField.required(5, "stringType", Types.StringType.get()),
      Types.NestedField.required(6, "timestampWithoutZone", Types.TimestampType.withoutZone()),
      Types.NestedField.required(7, "timestampWithZone", Types.TimestampType.withZone()),
      Types.NestedField.required(8, "fixedType", Types.FixedType.ofLength(5)),
      Types.NestedField.required(9, "uuidType", Types.UUIDType.get()),
      Types.NestedField.required(10, "binaryType", Types.BinaryType.get()),
      Types.NestedField.required(11, "decimalType1", Types.DecimalType.of(18, 3)),
      Types.NestedField.required(12, "decimalType2", Types.DecimalType.of(10, 5)),
      Types.NestedField.required(13, "decimalType3", Types.DecimalType.of(38, 19)),
      Types.NestedField.required(14, "floatType", Types.FloatType.get()),
      Types.NestedField.required(15, "doubleType", Types.DoubleType.get())
  );

  private static final List<String> SUPPORTED_PRIMITIVES = SCHEMA.asStruct().fields().stream()
      .map(Types.NestedField::name).collect(Collectors.toList());

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

    List<RowData> rows = Lists.newArrayList(
        GenericRowData.of(1, StringData.fromString("a")),
        GenericRowData.of(2, StringData.fromString("b")),
        GenericRowData.of(3, null)
    );

    RowDataWrapper rowWrapper = new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());

    for (RowData row : rows) {
      PartitionKey partitionKey = new PartitionKey(spec, schema);
      partitionKey.partition(rowWrapper.wrap(row));
      Assert.assertEquals(partitionKey.size(), 1);

      String expectedStr = row.isNullAt(1) ? null : row.getString(1).toString();
      Assert.assertEquals(expectedStr, partitionKey.get(0, String.class));
    }
  }

  @Test
  public void testPartitionWithOneNestedField() {
    RowDataWrapper rowWrapper = new RowDataWrapper(FlinkSchemaUtil.convert(NESTED_SCHEMA), NESTED_SCHEMA.asStruct());
    Iterable<RowData> rows = RandomRowData.generate(NESTED_SCHEMA, 10, 1991);

    PartitionSpec spec1 = PartitionSpec.builderFor(NESTED_SCHEMA)
        .identity("structType.innerStringType")
        .build();
    PartitionSpec spec2 = PartitionSpec.builderFor(NESTED_SCHEMA)
        .identity("structType.innerIntegerType")
        .build();

    for (RowData row : rows) {
      RowData innerRow = row.getRow(0, 2);

      PartitionKey partitionKey1 = new PartitionKey(spec1, NESTED_SCHEMA);
      partitionKey1.partition(rowWrapper.wrap(row));
      Assert.assertEquals(partitionKey1.size(), 1);

      String expectedStr = innerRow.isNullAt(0) ? null : innerRow.getString(0).toString();
      Assert.assertEquals(expectedStr, partitionKey1.get(0, String.class));

      PartitionKey partitionKey2 = new PartitionKey(spec2, NESTED_SCHEMA);
      partitionKey2.partition(rowWrapper.wrap(row));
      Assert.assertEquals(partitionKey2.size(), 1);

      Integer expectedInt = innerRow.isNullAt(1) ? null : innerRow.getInt(1);
      Assert.assertEquals(expectedInt, partitionKey2.get(0, Integer.class));
    }
  }

  @Test
  public void testPartitionMultipleNestedField() {
    RowDataWrapper rowWrapper = new RowDataWrapper(FlinkSchemaUtil.convert(NESTED_SCHEMA), NESTED_SCHEMA.asStruct());
    Iterable<RowData> rows = RandomRowData.generate(NESTED_SCHEMA, 10, 1992);

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

    for (RowData row : rows) {
      RowData innerRow = row.getRow(0, 2);

      pk1.partition(rowWrapper.wrap(row));
      Assert.assertEquals(2, pk1.size());

      Integer expectedInt = innerRow.isNullAt(1) ? null : innerRow.getInt(1);
      Assert.assertEquals(expectedInt, pk1.get(0, Integer.class));

      String expectedStr = innerRow.isNullAt(0) ? null : innerRow.getString(0).toString();
      Assert.assertEquals(expectedStr, pk1.get(1, String.class));

      pk2.partition(rowWrapper.wrap(row));
      Assert.assertEquals(2, pk2.size());

      expectedStr = innerRow.isNullAt(0) ? null : innerRow.getString(0).toString();
      Assert.assertEquals(expectedStr, pk2.get(0, String.class));

      expectedInt = innerRow.isNullAt(1) ? null : innerRow.getInt(1);
      Assert.assertEquals(expectedInt, pk2.get(1, Integer.class));
    }
  }

  private static Object transform(Object value, Type type) {
    if (value == null) {
      return null;
    }
    switch (type.typeId()) {
      case STRING:
        return value.toString();
      case FIXED:
      case BINARY:
        return ByteBuffer.wrap((byte[]) value);
      case UUID:
        return UUID.nameUUIDFromBytes((byte[]) value);
      case TIME:
        int intValue = (int) value;
        return (long) intValue;
      case TIMESTAMP:
        TimestampData timestampData = (TimestampData) value;
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return timestampData.getMillisecond() * 1000 + Math.floorDiv(timestampData.getNanoOfMillisecond(), 1000);
        } else {
          return DateTimeUtil.microsFromTimestamp(timestampData.toLocalDateTime());
        }
      case DECIMAL:
        DecimalData decimalData = (DecimalData) value;
        return decimalData.toBigDecimal();
      default:
        return value;
    }
  }

  @Test
  public void testPartitionValueTypes() {
    RowType rowType = FlinkSchemaUtil.convert(SCHEMA);
    RowDataWrapper rowWrapper = new RowDataWrapper(rowType, SCHEMA.asStruct());
    Iterable<RowData> rows = RandomRowData.generate(SCHEMA, 10, 1993);

    for (int i = 0; i < SUPPORTED_PRIMITIVES.size(); i++) {
      String column = SUPPORTED_PRIMITIVES.get(i);

      PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity(column).build();
      Type type = spec.schema().findType(column);
      Class<?>[] javaClasses = spec.javaClasses();

      PartitionKey pk = new PartitionKey(spec, SCHEMA);

      for (RowData row : rows) {
        pk.partition(rowWrapper.wrap(row));
        Object expected = RowData.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(row);
        Assert.assertEquals("Partition with column " + column + " should have one field.", 1, pk.size());
        Assert.assertEquals("Partition with column " + column + " should have the expected values",
            transform(expected, type), pk.get(0, javaClasses[0]));
      }
    }
  }

  @Test
  public void testNestedPartitionValues() {
    Schema nestedSchema = new Schema(Types.NestedField.optional(1001, "nested", SCHEMA.asStruct()));
    RowType rowType = FlinkSchemaUtil.convert(nestedSchema);
    RowType nestedRowType = (RowType) rowType.getTypeAt(0);

    RowDataWrapper rowWrapper = new RowDataWrapper(rowType, nestedSchema.asStruct());
    Iterable<RowData> rows = RandomRowData.generate(nestedSchema, 10, 1994);

    for (int i = 0; i < SUPPORTED_PRIMITIVES.size(); i++) {
      String column = String.format("nested.%s", SUPPORTED_PRIMITIVES.get(i));

      PartitionSpec spec = PartitionSpec.builderFor(nestedSchema).identity(column).build();
      Type type = spec.schema().findType(column);
      Class<?>[] javaClasses = spec.javaClasses();

      PartitionKey pk = new PartitionKey(spec, nestedSchema);

      for (RowData row : rows) {
        pk.partition(rowWrapper.wrap(row));

        Object expected = RowData.createFieldGetter(nestedRowType.getTypeAt(i), i)
            .getFieldOrNull(row.getRow(0, SUPPORTED_PRIMITIVES.size()));
        Assert.assertEquals("Partition with nested column " + column + " should have one field.",
            1, pk.size());
        Assert.assertEquals("Partition with nested column " + column + "should have the expected values.",
            transform(expected, type), pk.get(0, javaClasses[0]));
      }
    }
  }
}
