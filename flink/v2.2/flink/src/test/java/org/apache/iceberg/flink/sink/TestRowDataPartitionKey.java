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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.RandomRowData;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestRowDataPartitionKey {
  private static final Schema SCHEMA =
      new Schema(
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
          Types.NestedField.required(15, "doubleType", Types.DoubleType.get()));

  private static final List<String> SUPPORTED_PRIMITIVES =
      SCHEMA.asStruct().fields().stream().map(Types.NestedField::name).collect(Collectors.toList());

  private static final Schema NESTED_SCHEMA =
      new Schema(
          Types.NestedField.required(
              1,
              "structType",
              Types.StructType.of(
                  Types.NestedField.optional(2, "innerStringType", Types.StringType.get()),
                  Types.NestedField.optional(3, "innerIntegerType", Types.IntegerType.get()))));

  @Test
  public void testNullPartitionValue() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("data").build();

    List<RowData> rows =
        Lists.newArrayList(
            GenericRowData.of(1, StringData.fromString("a")),
            GenericRowData.of(2, StringData.fromString("b")),
            GenericRowData.of(3, null));

    RowDataWrapper rowWrapper =
        new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());

    for (RowData row : rows) {
      PartitionKey partitionKey = new PartitionKey(spec, schema);
      partitionKey.partition(rowWrapper.wrap(row));
      assertThat(partitionKey.size()).isEqualTo(1);

      String expectedStr = row.isNullAt(1) ? null : row.getString(1).toString();
      assertThat(partitionKey.get(0, String.class)).isEqualTo(expectedStr);
    }
  }

  @Test
  public void testPartitionWithOneNestedField() {
    RowDataWrapper rowWrapper =
        new RowDataWrapper(FlinkSchemaUtil.convert(NESTED_SCHEMA), NESTED_SCHEMA.asStruct());
    List<Record> records = RandomGenericData.generate(NESTED_SCHEMA, 10, 1991);
    List<RowData> rows = Lists.newArrayList(RandomRowData.convert(NESTED_SCHEMA, records));

    PartitionSpec spec1 =
        PartitionSpec.builderFor(NESTED_SCHEMA).identity("structType.innerStringType").build();
    PartitionSpec spec2 =
        PartitionSpec.builderFor(NESTED_SCHEMA).identity("structType.innerIntegerType").build();

    for (int i = 0; i < rows.size(); i++) {
      RowData row = rows.get(i);
      Record record = (Record) records.get(i).get(0);

      PartitionKey partitionKey1 = new PartitionKey(spec1, NESTED_SCHEMA);
      partitionKey1.partition(rowWrapper.wrap(row));
      assertThat(partitionKey1.size()).isEqualTo(1);

      assertThat(partitionKey1.get(0, String.class)).isEqualTo(record.get(0));

      PartitionKey partitionKey2 = new PartitionKey(spec2, NESTED_SCHEMA);
      partitionKey2.partition(rowWrapper.wrap(row));
      assertThat(partitionKey2.size()).isEqualTo(1);

      assertThat(partitionKey2.get(0, Integer.class)).isEqualTo(record.get(1));
    }
  }

  @Test
  public void testPartitionMultipleNestedField() {
    RowDataWrapper rowWrapper =
        new RowDataWrapper(FlinkSchemaUtil.convert(NESTED_SCHEMA), NESTED_SCHEMA.asStruct());
    List<Record> records = RandomGenericData.generate(NESTED_SCHEMA, 10, 1992);
    List<RowData> rows = Lists.newArrayList(RandomRowData.convert(NESTED_SCHEMA, records));

    PartitionSpec spec1 =
        PartitionSpec.builderFor(NESTED_SCHEMA)
            .identity("structType.innerIntegerType")
            .identity("structType.innerStringType")
            .build();
    PartitionSpec spec2 =
        PartitionSpec.builderFor(NESTED_SCHEMA)
            .identity("structType.innerStringType")
            .identity("structType.innerIntegerType")
            .build();

    PartitionKey pk1 = new PartitionKey(spec1, NESTED_SCHEMA);
    PartitionKey pk2 = new PartitionKey(spec2, NESTED_SCHEMA);

    for (int i = 0; i < rows.size(); i++) {
      RowData row = rows.get(i);
      Record record = (Record) records.get(i).get(0);

      pk1.partition(rowWrapper.wrap(row));
      assertThat(pk1.size()).isEqualTo(2);

      assertThat(pk1.get(0, Integer.class)).isEqualTo(record.get(1));
      assertThat(pk1.get(1, String.class)).isEqualTo(record.get(0));

      pk2.partition(rowWrapper.wrap(row));
      assertThat(pk2.size()).isEqualTo(2);

      assertThat(pk2.get(0, String.class)).isEqualTo(record.get(0));
      assertThat(pk2.get(1, Integer.class)).isEqualTo(record.get(1));
    }
  }

  @Test
  public void testPartitionValueTypes() {
    RowType rowType = FlinkSchemaUtil.convert(SCHEMA);
    RowDataWrapper rowWrapper = new RowDataWrapper(rowType, SCHEMA.asStruct());
    InternalRecordWrapper recordWrapper = new InternalRecordWrapper(SCHEMA.asStruct());

    List<Record> records = RandomGenericData.generate(SCHEMA, 10, 1993);
    List<RowData> rows = Lists.newArrayList(RandomRowData.convert(SCHEMA, records));

    for (String column : SUPPORTED_PRIMITIVES) {
      PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity(column).build();
      Class<?>[] javaClasses = spec.javaClasses();

      PartitionKey pk = new PartitionKey(spec, SCHEMA);
      PartitionKey expectedPK = new PartitionKey(spec, SCHEMA);

      for (int j = 0; j < rows.size(); j++) {
        RowData row = rows.get(j);
        Record record = records.get(j);

        pk.partition(rowWrapper.wrap(row));
        expectedPK.partition(recordWrapper.wrap(record));

        assertThat(pk.size())
            .as("Partition with column " + column + " should have one field.")
            .isEqualTo(1);

        if (column.equals("timeType")) {
          assertThat(pk.get(0, Long.class) / 1000)
              .as("Partition with column " + column + " should have the expected values")
              .isEqualTo(expectedPK.get(0, Long.class) / 1000);
        } else {
          assertThat(pk.get(0, javaClasses[0]))
              .as("Partition with column " + column + " should have the expected values")
              .isEqualTo(expectedPK.get(0, javaClasses[0]));
        }
      }
    }
  }

  @Test
  public void testNestedPartitionValues() {
    Schema nestedSchema = new Schema(Types.NestedField.optional(1001, "nested", SCHEMA.asStruct()));
    RowType rowType = FlinkSchemaUtil.convert(nestedSchema);

    RowDataWrapper rowWrapper = new RowDataWrapper(rowType, nestedSchema.asStruct());
    InternalRecordWrapper recordWrapper = new InternalRecordWrapper(nestedSchema.asStruct());

    List<Record> records = RandomGenericData.generate(nestedSchema, 10, 1994);
    List<RowData> rows = Lists.newArrayList(RandomRowData.convert(nestedSchema, records));

    for (String supportedPrimitive : SUPPORTED_PRIMITIVES) {
      String column = String.format("nested.%s", supportedPrimitive);

      PartitionSpec spec = PartitionSpec.builderFor(nestedSchema).identity(column).build();
      Class<?>[] javaClasses = spec.javaClasses();

      PartitionKey pk = new PartitionKey(spec, nestedSchema);
      PartitionKey expectedPK = new PartitionKey(spec, nestedSchema);

      for (int j = 0; j < rows.size(); j++) {
        pk.partition(rowWrapper.wrap(rows.get(j)));
        expectedPK.partition(recordWrapper.wrap(records.get(j)));

        assertThat(pk.size())
            .as("Partition with nested column " + column + " should have one field.")
            .isEqualTo(1);

        if (column.equals("nested.timeType")) {
          assertThat(pk.get(0, Long.class) / 1000)
              .as("Partition with nested column " + column + " should have the expected values.")
              .isEqualTo(expectedPK.get(0, Long.class) / 1000);
        } else {
          assertThat(pk.get(0, javaClasses[0]))
              .as("Partition with nested column " + column + " should have the expected values.")
              .isEqualTo(expectedPK.get(0, javaClasses[0]));
        }
      }
    }
  }
}
