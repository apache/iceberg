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
package org.apache.iceberg.mr.hive.serde.objectinspector;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hive.HiveVersion;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestIcebergObjectInspector {

  private final Schema schema =
      new Schema(
          required(1, "binary_field", Types.BinaryType.get(), "binary comment"),
          required(2, "boolean_field", Types.BooleanType.get(), "boolean comment"),
          required(3, "date_field", Types.DateType.get(), "date comment"),
          required(4, "decimal_field", Types.DecimalType.of(38, 18), "decimal comment"),
          required(5, "double_field", Types.DoubleType.get(), "double comment"),
          required(6, "fixed_field", Types.FixedType.ofLength(3), "fixed comment"),
          required(7, "float_field", Types.FloatType.get(), "float comment"),
          required(8, "integer_field", Types.IntegerType.get(), "integer comment"),
          required(9, "long_field", Types.LongType.get(), "long comment"),
          required(10, "string_field", Types.StringType.get(), "string comment"),
          required(11, "timestamp_field", Types.TimestampType.withoutZone(), "timestamp comment"),
          required(12, "timestamptz_field", Types.TimestampType.withZone(), "timestamptz comment"),
          required(13, "uuid_field", Types.UUIDType.get(), "uuid comment"),
          required(
              14,
              "list_field",
              Types.ListType.ofRequired(15, Types.StringType.get()),
              "list comment"),
          required(
              16,
              "map_field",
              Types.MapType.ofRequired(17, 18, Types.StringType.get(), Types.IntegerType.get()),
              "map comment"),
          required(
              19,
              "struct_field",
              Types.StructType.of(
                  Types.NestedField.required(
                      20, "nested_field", Types.StringType.get(), "nested field comment")),
              "struct comment"),
          required(21, "time_field", Types.TimeType.get(), "time comment"));

  @SuppressWarnings("MethodLength")
  @Test
  public void testIcebergObjectInspector() {
    ObjectInspector oi = IcebergObjectInspector.create(schema);
    assertThat(oi).isNotNull();
    assertThat(oi.getCategory()).isEqualTo(ObjectInspector.Category.STRUCT);

    StructObjectInspector soi = (StructObjectInspector) oi;

    // binary
    StructField binaryField = soi.getStructFieldRef("binary_field");
    assertThat(binaryField.getFieldID()).isEqualTo(1);
    assertThat(binaryField.getFieldName()).isEqualTo("binary_field");
    assertThat(binaryField.getFieldComment()).isEqualTo("binary comment");
    assertThat(binaryField.getFieldObjectInspector()).isEqualTo(IcebergBinaryObjectInspector.get());

    // boolean
    StructField booleanField = soi.getStructFieldRef("boolean_field");
    assertThat(booleanField.getFieldID()).isEqualTo(2);
    assertThat(booleanField.getFieldName()).isEqualTo("boolean_field");
    assertThat(booleanField.getFieldComment()).isEqualTo("boolean comment");
    assertThat(booleanField.getFieldObjectInspector())
        .isEqualTo(getPrimitiveObjectInspector(boolean.class));

    // date
    StructField dateField = soi.getStructFieldRef("date_field");
    assertThat(dateField.getFieldID()).isEqualTo(3);
    assertThat(dateField.getFieldName()).isEqualTo("date_field");
    assertThat(dateField.getFieldComment()).isEqualTo("date comment");
    if (HiveVersion.min(HiveVersion.HIVE_3)) {
      assertThat(dateField.getFieldObjectInspector().getClass().getName())
          .isEqualTo(
              "org.apache.iceberg.mr.hive.serde.objectinspector.IcebergDateObjectInspectorHive3");
    } else {
      assertThat(dateField.getFieldObjectInspector().getClass().getName())
          .isEqualTo("org.apache.iceberg.mr.hive.serde.objectinspector.IcebergDateObjectInspector");
    }

    // decimal
    StructField decimalField = soi.getStructFieldRef("decimal_field");
    assertThat(decimalField.getFieldID()).isEqualTo(4);
    assertThat(decimalField.getFieldName()).isEqualTo("decimal_field");
    assertThat(decimalField.getFieldComment()).isEqualTo("decimal comment");
    assertThat(decimalField.getFieldObjectInspector())
        .isEqualTo(IcebergDecimalObjectInspector.get(38, 18));

    // double
    StructField doubleField = soi.getStructFieldRef("double_field");
    assertThat(doubleField.getFieldID()).isEqualTo(5);
    assertThat(doubleField.getFieldName()).isEqualTo("double_field");
    assertThat(doubleField.getFieldComment()).isEqualTo("double comment");
    assertThat(doubleField.getFieldObjectInspector())
        .isEqualTo(getPrimitiveObjectInspector(double.class));

    // fixed
    StructField fixedField = soi.getStructFieldRef("fixed_field");
    assertThat(fixedField.getFieldID()).isEqualTo(6);
    assertThat(fixedField.getFieldName()).isEqualTo("fixed_field");
    assertThat(fixedField.getFieldComment()).isEqualTo("fixed comment");
    assertThat(fixedField.getFieldObjectInspector()).isEqualTo(IcebergFixedObjectInspector.get());

    // float
    StructField floatField = soi.getStructFieldRef("float_field");
    assertThat(floatField.getFieldID()).isEqualTo(7);
    assertThat(floatField.getFieldName()).isEqualTo("float_field");
    assertThat(floatField.getFieldComment()).isEqualTo("float comment");
    assertThat(floatField.getFieldObjectInspector())
        .isEqualTo(getPrimitiveObjectInspector(float.class));

    // integer
    StructField integerField = soi.getStructFieldRef("integer_field");
    assertThat(integerField.getFieldID()).isEqualTo(8);
    assertThat(integerField.getFieldName()).isEqualTo("integer_field");
    assertThat(integerField.getFieldComment()).isEqualTo("integer comment");
    assertThat(integerField.getFieldObjectInspector())
        .isEqualTo(getPrimitiveObjectInspector(int.class));

    // long
    StructField longField = soi.getStructFieldRef("long_field");
    assertThat(longField.getFieldID()).isEqualTo(9);
    assertThat(longField.getFieldName()).isEqualTo("long_field");
    assertThat(longField.getFieldComment()).isEqualTo("long comment");
    assertThat(longField.getFieldObjectInspector())
        .isEqualTo(getPrimitiveObjectInspector(long.class));

    // string
    StructField stringField = soi.getStructFieldRef("string_field");
    assertThat(stringField.getFieldID()).isEqualTo(10);
    assertThat(stringField.getFieldName()).isEqualTo("string_field");
    assertThat(stringField.getFieldComment()).isEqualTo("string comment");
    assertThat(stringField.getFieldObjectInspector())
        .isEqualTo(getPrimitiveObjectInspector(String.class));

    // timestamp without tz
    StructField timestampField = soi.getStructFieldRef("timestamp_field");
    assertThat(timestampField.getFieldID()).isEqualTo(11);
    assertThat(timestampField.getFieldName()).isEqualTo("timestamp_field");
    assertThat(timestampField.getFieldComment()).isEqualTo("timestamp comment");
    if (HiveVersion.min(HiveVersion.HIVE_3)) {
      assertThat(timestampField.getFieldObjectInspector().getClass().getSimpleName())
          .isEqualTo("IcebergTimestampObjectInspectorHive3");
    } else {
      assertThat(timestampField.getFieldObjectInspector())
          .isEqualTo(IcebergTimestampObjectInspector.get());
    }

    // timestamp with tz
    StructField timestampTzField = soi.getStructFieldRef("timestamptz_field");
    assertThat(timestampTzField.getFieldID()).isEqualTo(12);
    assertThat(timestampTzField.getFieldName()).isEqualTo("timestamptz_field");
    assertThat(timestampTzField.getFieldComment()).isEqualTo("timestamptz comment");
    if (HiveVersion.min(HiveVersion.HIVE_3)) {
      assertThat(timestampTzField.getFieldObjectInspector().getClass().getSimpleName())
          .isEqualTo("IcebergTimestampWithZoneObjectInspectorHive3");
    } else {
      assertThat(timestampTzField.getFieldObjectInspector())
          .isEqualTo(IcebergTimestampWithZoneObjectInspector.get());
    }

    // UUID
    StructField uuidField = soi.getStructFieldRef("uuid_field");
    assertThat(uuidField.getFieldID()).isEqualTo(13);
    assertThat(uuidField.getFieldName()).isEqualTo("uuid_field");
    assertThat(uuidField.getFieldComment()).isEqualTo("uuid comment");
    assertThat(uuidField.getFieldObjectInspector()).isEqualTo(IcebergUUIDObjectInspector.get());

    // list
    StructField listField = soi.getStructFieldRef("list_field");
    assertThat(listField.getFieldID()).isEqualTo(14);
    assertThat(listField.getFieldName()).isEqualTo("list_field");
    assertThat(listField.getFieldComment()).isEqualTo("list comment");
    assertThat(listField.getFieldObjectInspector()).isEqualTo(getListObjectInspector(String.class));

    // map
    StructField mapField = soi.getStructFieldRef("map_field");
    assertThat(mapField.getFieldID()).isEqualTo(16);
    assertThat(mapField.getFieldName()).isEqualTo("map_field");
    assertThat(mapField.getFieldComment()).isEqualTo("map comment");
    assertThat(mapField.getFieldObjectInspector())
        .isEqualTo(getMapObjectInspector(String.class, int.class));

    // struct
    StructField structField = soi.getStructFieldRef("struct_field");
    assertThat(structField.getFieldID()).isEqualTo(19);
    assertThat(structField.getFieldName()).isEqualTo("struct_field");
    assertThat(structField.getFieldComment()).isEqualTo("struct comment");

    ObjectInspector expectedObjectInspector =
        new IcebergRecordObjectInspector(
            (Types.StructType) schema.findType(19),
            ImmutableList.of(getPrimitiveObjectInspector(String.class)));
    assertThat(structField.getFieldObjectInspector()).isEqualTo(expectedObjectInspector);

    // time
    StructField timeField = soi.getStructFieldRef("time_field");
    assertThat(timeField.getFieldID()).isEqualTo(21);
    assertThat(timeField.getFieldName()).isEqualTo("time_field");
    assertThat(timeField.getFieldComment()).isEqualTo("time comment");
    assertThat(timeField.getFieldObjectInspector()).isEqualTo(IcebergTimeObjectInspector.get());
  }

  private static ObjectInspector getPrimitiveObjectInspector(Class<?> clazz) {
    PrimitiveTypeInfo typeInfo =
        (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(clazz);
    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(typeInfo);
  }

  private static ObjectInspector getListObjectInspector(Class<?> clazz) {
    return ObjectInspectorFactory.getStandardListObjectInspector(
        getPrimitiveObjectInspector(clazz));
  }

  private static ObjectInspector getMapObjectInspector(Class<?> keyClazz, Class<?> valueClazz) {
    return ObjectInspectorFactory.getStandardMapObjectInspector(
        getPrimitiveObjectInspector(keyClazz), getPrimitiveObjectInspector(valueClazz));
  }
}
