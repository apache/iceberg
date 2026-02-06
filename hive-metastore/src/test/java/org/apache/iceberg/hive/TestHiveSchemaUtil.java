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
package org.apache.iceberg.hive;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestHiveSchemaUtil {
  private static final Schema SIMPLE_ICEBERG_SCHEMA =
      new Schema(
          optional(0, "customer_id", Types.LongType.get(), "customer comment"),
          optional(1, "first_name", Types.StringType.get(), "first name comment"));

  private static final Schema COMPLEX_ICEBERG_SCHEMA =
      new Schema(
          optional(0, "id", Types.LongType.get(), ""),
          optional(1, "name", Types.StringType.get(), ""),
          optional(
              2,
              "employee_info",
              Types.StructType.of(
                  optional(3, "employer", Types.StringType.get()),
                  optional(4, "id", Types.LongType.get()),
                  optional(5, "address", Types.StringType.get())),
              ""),
          optional(
              6,
              "places_lived",
              Types.ListType.ofOptional(
                  10,
                  Types.StructType.of(
                      optional(7, "street", Types.StringType.get()),
                      optional(8, "city", Types.StringType.get()),
                      optional(9, "country", Types.StringType.get()))),
              ""),
          optional(
              11,
              "memorable_moments",
              Types.MapType.ofOptional(
                  15,
                  16,
                  Types.StringType.get(),
                  Types.StructType.of(
                      optional(12, "year", Types.IntegerType.get()),
                      optional(13, "place", Types.StringType.get()),
                      optional(14, "details", Types.StringType.get()))),
              ""),
          optional(
              17,
              "current_address",
              Types.StructType.of(
                  optional(
                      18,
                      "street_address",
                      Types.StructType.of(
                          optional(19, "street_number", Types.IntegerType.get()),
                          optional(20, "street_name", Types.StringType.get()),
                          optional(21, "street_type", Types.StringType.get()))),
                  optional(22, "country", Types.StringType.get()),
                  optional(23, "postal_code", Types.StringType.get())),
              ""));

  private static final List<FieldSchema> SIMPLE_HIVE_SCHEMA =
      ImmutableList.of(
          new FieldSchema("customer_id", serdeConstants.BIGINT_TYPE_NAME, "customer comment"),
          new FieldSchema("first_name", serdeConstants.STRING_TYPE_NAME, "first name comment"));

  private static final List<FieldSchema> COMPLEX_HIVE_SCHEMA =
      ImmutableList.of(
          new FieldSchema("id", "bigint", ""),
          new FieldSchema("name", "string", ""),
          new FieldSchema("employee_info", "struct<employer:string,id:bigint,address:string>", ""),
          new FieldSchema(
              "places_lived", "array<struct<street:string,city:string,country:string>>", ""),
          new FieldSchema(
              "memorable_moments", "map<string,struct<year:int,place:string,details:string>>", ""),
          new FieldSchema(
              "current_address",
              "struct<street_address:struct<street_number:int,street_name:string,"
                  + "street_type:string>,country:string,postal_code:string>",
              ""));

  @Test
  public void testSimpleSchemaConvertToIcebergSchema() {
    assertThat(HiveSchemaUtil.convert(SIMPLE_HIVE_SCHEMA).asStruct())
        .isEqualTo(SIMPLE_ICEBERG_SCHEMA.asStruct());
  }

  @Test
  public void testSimpleSchemaConvertToIcebergSchemaFromNameAndTypeLists() {
    List<String> names =
        SIMPLE_HIVE_SCHEMA.stream().map(field -> field.getName()).collect(Collectors.toList());
    List<TypeInfo> types =
        SIMPLE_HIVE_SCHEMA.stream()
            .map(field -> TypeInfoUtils.getTypeInfoFromTypeString(field.getType()))
            .collect(Collectors.toList());
    List<String> comments =
        SIMPLE_HIVE_SCHEMA.stream().map(FieldSchema::getComment).collect(Collectors.toList());
    assertThat(HiveSchemaUtil.convert(names, types, comments).asStruct())
        .isEqualTo(SIMPLE_ICEBERG_SCHEMA.asStruct());
  }

  @Test
  public void testComplexSchemaConvertToIcebergSchema() {
    assertThat(HiveSchemaUtil.convert(COMPLEX_HIVE_SCHEMA).asStruct())
        .isEqualTo(COMPLEX_ICEBERG_SCHEMA.asStruct());
  }

  @Test
  public void testSchemaConvertToIcebergSchemaForEveryPrimitiveType() {
    Schema schemaWithEveryType = HiveSchemaUtil.convert(getSupportedFieldSchemas());
    assertThat(schemaWithEveryType.asStruct()).isEqualTo(getSchemaWithSupportedTypes().asStruct());
  }

  @Test
  public void testNotSupportedTypes() {
    for (FieldSchema notSupportedField : getNotSupportedFieldSchemas()) {
      assertThatThrownBy(
              () ->
                  HiveSchemaUtil.convert(
                      Lists.newArrayList(Collections.singletonList(notSupportedField))))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageStartingWith("Unsupported Hive type");
    }
  }

  @Test
  public void testSimpleSchemaConvertToHiveSchema() {
    assertThat(HiveSchemaUtil.convert(SIMPLE_ICEBERG_SCHEMA)).isEqualTo(SIMPLE_HIVE_SCHEMA);
  }

  @Test
  public void testComplexSchemaConvertToHiveSchema() {
    assertThat(HiveSchemaUtil.convert(COMPLEX_ICEBERG_SCHEMA)).isEqualTo(COMPLEX_HIVE_SCHEMA);
  }

  @Test
  public void testSimpleTypeAndTypeInfoConvert() {
    // Test for every supported type
    List<FieldSchema> fieldSchemas = getSupportedFieldSchemas();
    List<Types.NestedField> nestedFields = getSchemaWithSupportedTypes().columns();
    for (int i = 0; i < fieldSchemas.size(); ++i) {
      checkConvert(
          TypeInfoUtils.getTypeInfoFromTypeString(fieldSchemas.get(i).getType()),
          nestedFields.get(i).type());
    }
  }

  @Test
  public void testComplexTypeAndTypeInfoConvert() {
    for (int i = 0; i < COMPLEX_HIVE_SCHEMA.size(); ++i) {
      checkConvert(
          TypeInfoUtils.getTypeInfoFromTypeString(COMPLEX_HIVE_SCHEMA.get(i).getType()),
          COMPLEX_ICEBERG_SCHEMA.columns().get(i).type());
    }
  }

  @Test
  public void testConversionWithoutLastComment() {
    Schema expected =
        new Schema(
            optional(0, "customer_id", Types.LongType.get(), "customer comment"),
            optional(1, "first_name", Types.StringType.get(), null));

    Schema schema =
        HiveSchemaUtil.convert(
            Arrays.asList("customer_id", "first_name"),
            Arrays.asList(
                TypeInfoUtils.getTypeInfoFromTypeString(serdeConstants.BIGINT_TYPE_NAME),
                TypeInfoUtils.getTypeInfoFromTypeString(serdeConstants.STRING_TYPE_NAME)),
            Collections.singletonList("customer comment"));

    assertThat(schema.asStruct()).isEqualTo(expected.asStruct());
  }

  protected List<FieldSchema> getSupportedFieldSchemas() {
    List<FieldSchema> fields = Lists.newArrayListWithCapacity(10);
    fields.add(new FieldSchema("c_float", serdeConstants.FLOAT_TYPE_NAME, "float comment"));
    fields.add(new FieldSchema("c_double", serdeConstants.DOUBLE_TYPE_NAME, "double comment"));
    fields.add(new FieldSchema("c_boolean", serdeConstants.BOOLEAN_TYPE_NAME, "boolean comment"));
    fields.add(new FieldSchema("c_int", serdeConstants.INT_TYPE_NAME, "int comment"));
    fields.add(new FieldSchema("c_long", serdeConstants.BIGINT_TYPE_NAME, "long comment"));
    fields.add(new FieldSchema("c_binary", serdeConstants.BINARY_TYPE_NAME, null));
    fields.add(new FieldSchema("c_string", serdeConstants.STRING_TYPE_NAME, null));
    fields.add(new FieldSchema("c_timestamp", serdeConstants.TIMESTAMP_TYPE_NAME, null));
    fields.add(new FieldSchema("c_date", serdeConstants.DATE_TYPE_NAME, null));
    fields.add(new FieldSchema("c_decimal", serdeConstants.DECIMAL_TYPE_NAME + "(38,10)", null));
    return fields;
  }

  protected List<FieldSchema> getNotSupportedFieldSchemas() {
    List<FieldSchema> fields = Lists.newArrayListWithCapacity(6);
    fields.add(new FieldSchema("c_byte", serdeConstants.TINYINT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_short", serdeConstants.SMALLINT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_char", serdeConstants.CHAR_TYPE_NAME + "(5)", ""));
    fields.add(new FieldSchema("c_varchar", serdeConstants.VARCHAR_TYPE_NAME + "(5)", ""));
    fields.add(
        new FieldSchema("c_interval_date", serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_interval_time", serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME, ""));
    return fields;
  }

  protected Schema getSchemaWithSupportedTypes() {
    return new Schema(
        optional(0, "c_float", Types.FloatType.get(), "float comment"),
        optional(1, "c_double", Types.DoubleType.get(), "double comment"),
        optional(2, "c_boolean", Types.BooleanType.get(), "boolean comment"),
        optional(3, "c_int", Types.IntegerType.get(), "int comment"),
        optional(4, "c_long", Types.LongType.get(), "long comment"),
        optional(5, "c_binary", Types.BinaryType.get()),
        optional(6, "c_string", Types.StringType.get()),
        optional(7, "c_timestamp", Types.TimestampType.withoutZone()),
        optional(8, "c_date", Types.DateType.get()),
        optional(9, "c_decimal", Types.DecimalType.of(38, 10)));
  }

  /**
   * Check conversion for 1-on-1 mappings
   *
   * @param typeInfo Hive type
   * @param type Iceberg type
   */
  private void checkConvert(TypeInfo typeInfo, Type type) {
    // Convert to TypeInfo
    assertThat(HiveSchemaUtil.convert(type)).isEqualTo(typeInfo);
    // Convert to Type
    assertEquals(type, HiveSchemaUtil.convert(typeInfo));
  }

  /**
   * Compares the nested types without checking the ids.
   *
   * @param expected The expected types to compare
   * @param actual The actual types to compare
   */
  private void assertEquals(Type expected, Type actual) {
    if (actual.isPrimitiveType()) {
      assertThat(actual).isEqualTo(expected);
    } else {
      List<Types.NestedField> expectedFields = ((Type.NestedType) expected).fields();
      List<Types.NestedField> actualFields = ((Type.NestedType) actual).fields();
      for (int i = 0; i < expectedFields.size(); ++i) {
        assertEquals(expectedFields.get(i).type(), actualFields.get(i).type());
        assertThat(actualFields.get(i).name()).isEqualTo(expectedFields.get(i).name());
      }
    }
  }
}
