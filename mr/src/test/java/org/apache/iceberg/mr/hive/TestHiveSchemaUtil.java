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

package org.apache.iceberg.mr.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestHiveSchemaUtil {
  private static final Schema SIMPLE_SCHEMA = new Schema(
      optional(0, "customer_id", Types.LongType.get()),
      optional(1, "first_name", Types.StringType.get())
  );

  private static final Schema COMPLEX_SCHEMA = new Schema(
      optional(0, "id", Types.LongType.get()),
      optional(1, "name", Types.StringType.get()),
      optional(2, "employee_info", Types.StructType.of(
          optional(3, "employer", Types.StringType.get()),
          optional(4, "id", Types.LongType.get()),
          optional(5, "address", Types.StringType.get())
      )),
      optional(6, "places_lived", Types.ListType.ofOptional(10, Types.StructType.of(
          optional(7, "street", Types.StringType.get()),
          optional(8, "city", Types.StringType.get()),
          optional(9, "country", Types.StringType.get())
      ))),
      optional(11, "memorable_moments", Types.MapType.ofOptional(15, 16,
          Types.StringType.get(),
          Types.StructType.of(
              optional(12, "year", Types.IntegerType.get()),
              optional(13, "place", Types.StringType.get()),
              optional(14, "details", Types.StringType.get())
          ))),
      optional(17, "current_address", Types.StructType.of(
          optional(18, "street_address", Types.StructType.of(
              optional(19, "street_number", Types.IntegerType.get()),
              optional(20, "street_name", Types.StringType.get()),
              optional(21, "street_type", Types.StringType.get())
          )),
          optional(22, "country", Types.StringType.get()),
          optional(23, "postal_code", Types.StringType.get())
      ))
  );

  @Test
  public void testSimpleSchemaConvert() {
    Schema schema = HiveSchemaUtil.schema("customer_id,first_name", "bigint:string", ",");
    Assert.assertEquals(SIMPLE_SCHEMA.asStruct(), schema.asStruct());
  }

  @Test
  public void testSimpleSchemaConvertFromType() {
    List<FieldSchema> fields = new ArrayList<>();
    fields.add(new FieldSchema("customer_id", serdeConstants.BIGINT_TYPE_NAME, ""));
    fields.add(new FieldSchema("first_name", serdeConstants.STRING_TYPE_NAME, ""));
    Schema schema = HiveSchemaUtil.schema(fields);
    Assert.assertEquals(SIMPLE_SCHEMA.asStruct(), schema.asStruct());
  }

  @Test
  public void testComplexSchemaConvert() {
    Schema schema = HiveSchemaUtil.schema(
        "id,name,employee_info,places_lived,memorable_moments,current_address",
        "bigint:string:" +
            "struct<employer:string,id:bigint,address:string>:" +
            "array<struct<street:string,city:string,country:string>>:" +
            "map<string,struct<year:int,place:string,details:string>>:" +
            "struct<street_address:struct<street_number:int,street_name:string,street_type:string>," +
                "country:string,postal_code:string>",
        ",");
    Assert.assertEquals(COMPLEX_SCHEMA.asStruct(), schema.asStruct());
  }

  @Test
  public void testSchemaConvertForEveryPrimitiveType() {
    Schema schemaWithEveryType = HiveSchemaUtil.schema(getSupportedFieldSchemas());
    Assert.assertEquals(getSchemaWithSupportedTypes().asStruct(), schemaWithEveryType.asStruct());
  }

  @Test
  public void testNotSupportedTypes() {
    for (FieldSchema notSupportedField : getNotSupportedFieldSchemas()) {
      AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
          "Unsupported Hive type", () -> {
            HiveSchemaUtil.schema(new ArrayList<>(Arrays.asList(notSupportedField)));
          }
      );
    }
  }

  protected List<FieldSchema> getSupportedFieldSchemas() {
    List<FieldSchema> fields = new ArrayList<>();
    fields.add(new FieldSchema("c_float", serdeConstants.FLOAT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_double", serdeConstants.DOUBLE_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_boolean", serdeConstants.BOOLEAN_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_int", serdeConstants.INT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_long", serdeConstants.BIGINT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_binary", serdeConstants.BINARY_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_string", serdeConstants.STRING_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_timestamp", serdeConstants.TIMESTAMP_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_date", serdeConstants.DATE_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_decimal", serdeConstants.DECIMAL_TYPE_NAME + "(38,10)", ""));
    return fields;
  }

  protected List<FieldSchema> getNotSupportedFieldSchemas() {
    List<FieldSchema> fields = new ArrayList<>();
    fields.add(new FieldSchema("c_byte", serdeConstants.TINYINT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_short", serdeConstants.SMALLINT_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_char", serdeConstants.CHAR_TYPE_NAME + "(5)", ""));
    fields.add(new FieldSchema("c_varchar", serdeConstants.VARCHAR_TYPE_NAME + "(5)", ""));
    fields.add(new FieldSchema("c_interval_date", serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME, ""));
    fields.add(new FieldSchema("c_interval_time", serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME, ""));
    return fields;
  }

  protected Schema getSchemaWithSupportedTypes() {
    return new Schema(
        optional(0, "c_float", Types.FloatType.get()),
        optional(1, "c_double", Types.DoubleType.get()),
        optional(2, "c_boolean", Types.BooleanType.get()),
        optional(3, "c_int", Types.IntegerType.get()),
        optional(4, "c_long", Types.LongType.get()),
        optional(5, "c_binary", Types.BinaryType.get()),
        optional(6, "c_string", Types.StringType.get()),
        optional(7, "c_timestamp", Types.TimestampType.withoutZone()),
        optional(8, "c_date", Types.DateType.get()),
        optional(9, "c_decimal", Types.DecimalType.of(38, 10)));
  }
}
