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

import static org.apache.iceberg.types.Types.NestedField.required;

import com.fasterxml.jackson.databind.node.IntNode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;

/**
 * Util class to generate test data with extensive coverage different field types: from primitives
 * to complex nested types.
 */
public class DataGenerators {

  public static class Primitives implements DataGenerator {
    private static final DateTime JODA_DATETIME_EPOC =
        new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
    private static final DateTime JODA_DATETIME_20220110 =
        new DateTime(2022, 1, 10, 0, 0, 0, 0, DateTimeZone.UTC);
    private static final int DAYS_BTW_EPOC_AND_20220110 =
        Days.daysBetween(JODA_DATETIME_EPOC, JODA_DATETIME_20220110).getDays();
    private static final int HOUR_8_IN_MILLI = (int) TimeUnit.HOURS.toMillis(8);

    private static final LocalDate JAVA_LOCAL_DATE_20220110 = LocalDate.of(2022, 1, 10);
    private static final LocalTime JAVA_LOCAL_TIME_HOUR8 = LocalTime.of(8, 0);
    private static final OffsetDateTime JAVA_OFFSET_DATE_TIME_20220110 =
        OffsetDateTime.of(2022, 1, 10, 0, 0, 0, 0, ZoneOffset.UTC);
    private static final LocalDateTime JAVA_LOCAL_DATE_TIME_20220110 =
        LocalDateTime.of(2022, 1, 10, 0, 0, 0);
    private static final BigDecimal BIG_DECIMAL_NEGATIVE = new BigDecimal("-1.50");
    private static final byte[] FIXED_BYTES = "012345689012345".getBytes(StandardCharsets.UTF_8);

    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            // primitive types
            Types.NestedField.optional(2, "boolean_field", Types.BooleanType.get()),
            Types.NestedField.optional(3, "int_field", Types.IntegerType.get()),
            Types.NestedField.optional(4, "long_field", Types.LongType.get()),
            Types.NestedField.optional(5, "float_field", Types.FloatType.get()),
            Types.NestedField.optional(6, "double_field", Types.DoubleType.get()),
            Types.NestedField.required(7, "string_field", Types.StringType.get()),
            Types.NestedField.required(8, "date_field", Types.DateType.get()),
            Types.NestedField.required(9, "time_field", Types.TimeType.get()),
            Types.NestedField.required(10, "ts_with_zone_field", Types.TimestampType.withZone()),
            Types.NestedField.required(
                11, "ts_without_zone_field", Types.TimestampType.withoutZone()),
            Types.NestedField.required(12, "uuid_field", Types.UUIDType.get()),
            Types.NestedField.required(13, "binary_field", Types.BinaryType.get()),
            Types.NestedField.required(14, "decimal_field", Types.DecimalType.of(9, 2)),
            Types.NestedField.required(15, "fixed_field", Types.FixedType.ofLength(16)));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    /**
     * Fix up Avro Schema that is converted from Iceberg Schema.
     *
     * @param schemaConvertedFromIceberg Avro Schema converted from Iceberg schema via {@link
     *     AvroSchemaUtil#convert(Schema, String)}
     */
    private org.apache.avro.Schema fixupAvroSchemaConvertedFromIcebergSchema(
        org.apache.avro.Schema schemaConvertedFromIceberg) {
      List<org.apache.avro.Schema.Field> fixedFields =
          schemaConvertedFromIceberg.getFields().stream()
              .map(
                  field -> {
                    org.apache.avro.Schema.Field updatedField = field;
                    if (field.name().equals("time_field")) {
                      // Iceberg's AvroSchemaUtil uses timestamp-micros with Long value for time
                      // field, while AvroToRowDataConverters#convertToTime() always looks for
                      // Integer value assuming millis. The root problem is that
                      // AvroToRowDataConverters#createConverter() uses LogicalTypeRoot to
                      // determine converter and LogicalTypeRoot lost the timestamp precision
                      // carried by LogicalType like Time(6).
                      org.apache.avro.Schema fieldSchema =
                          LogicalTypes.timeMillis()
                              .addToSchema(
                                  org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT));
                      updatedField = new org.apache.avro.Schema.Field("time_field", fieldSchema);
                    }

                    return new org.apache.avro.Schema.Field(updatedField, updatedField.schema());
                  })
              .collect(Collectors.toList());
      return org.apache.avro.Schema.createRecord(
          schemaConvertedFromIceberg.getName(),
          schemaConvertedFromIceberg.getDoc(),
          schemaConvertedFromIceberg.getNamespace(),
          schemaConvertedFromIceberg.isError(),
          fixedFields);
    }

    private final org.apache.avro.Schema avroSchema =
        fixupAvroSchemaConvertedFromIcebergSchema(AvroSchemaUtil.convert(icebergSchema, "table"));

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField("boolean_field", false);
      genericRecord.setField("int_field", Integer.MAX_VALUE);
      genericRecord.setField("long_field", Long.MAX_VALUE);
      genericRecord.setField("float_field", Float.MAX_VALUE);
      genericRecord.setField("double_field", Double.MAX_VALUE);
      genericRecord.setField("string_field", "str");

      genericRecord.setField("date_field", JAVA_LOCAL_DATE_20220110);
      genericRecord.setField("time_field", JAVA_LOCAL_TIME_HOUR8);
      genericRecord.setField("ts_with_zone_field", JAVA_OFFSET_DATE_TIME_20220110);
      genericRecord.setField("ts_without_zone_field", JAVA_LOCAL_DATE_TIME_20220110);

      byte[] uuidBytes = new byte[16];
      for (int i = 0; i < 16; ++i) {
        uuidBytes[i] = (byte) i;
      }

      genericRecord.setField("uuid_field", UUID.nameUUIDFromBytes(uuidBytes));

      byte[] binaryBytes = new byte[7];
      for (int i = 0; i < 7; ++i) {
        binaryBytes[i] = (byte) i;
      }
      genericRecord.setField("binary_field", ByteBuffer.wrap(binaryBytes));

      genericRecord.setField("decimal_field", BIG_DECIMAL_NEGATIVE);
      genericRecord.setField("fixed_field", FIXED_BYTES);

      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      byte[] uuidBytes = new byte[16];
      for (int i = 0; i < 16; ++i) {
        uuidBytes[i] = (byte) i;
      }

      byte[] binaryBytes = new byte[7];
      for (int i = 0; i < 7; ++i) {
        binaryBytes[i] = (byte) i;
      }

      return GenericRowData.of(
          StringData.fromString("row_id_value"),
          false,
          Integer.MAX_VALUE,
          Long.MAX_VALUE,
          Float.MAX_VALUE,
          Double.MAX_VALUE,
          StringData.fromString("str"),
          DAYS_BTW_EPOC_AND_20220110,
          HOUR_8_IN_MILLI,
          // Although Avro logical type for timestamp fields are in micro seconds,
          // AvroToRowDataConverters only looks for long value in milliseconds.
          TimestampData.fromEpochMillis(JODA_DATETIME_20220110.getMillis()),
          TimestampData.fromEpochMillis(JODA_DATETIME_20220110.getMillis()),
          uuidBytes,
          binaryBytes,
          DecimalData.fromBigDecimal(BIG_DECIMAL_NEGATIVE, 9, 2),
          FIXED_BYTES);
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", new Utf8("row_id_value"));
      genericRecord.put("boolean_field", false);
      genericRecord.put("int_field", Integer.MAX_VALUE);
      genericRecord.put("long_field", Long.MAX_VALUE);
      genericRecord.put("float_field", Float.MAX_VALUE);
      genericRecord.put("double_field", Double.MAX_VALUE);
      genericRecord.put("string_field", new Utf8("str"));

      genericRecord.put("date_field", DAYS_BTW_EPOC_AND_20220110);
      genericRecord.put("time_field", HOUR_8_IN_MILLI);
      // Although Avro logical type for timestamp fields are in micro seconds,
      // AvroToRowDataConverters only looks for long value in milliseconds.
      genericRecord.put("ts_with_zone_field", JODA_DATETIME_20220110.getMillis());
      genericRecord.put("ts_without_zone_field", JODA_DATETIME_20220110.getMillis());

      byte[] uuidBytes = new byte[16];
      for (int i = 0; i < 16; ++i) {
        uuidBytes[i] = (byte) i;
      }
      genericRecord.put("uuid_field", ByteBuffer.wrap(uuidBytes));

      byte[] binaryBytes = new byte[7];
      for (int i = 0; i < 7; ++i) {
        binaryBytes[i] = (byte) i;
      }
      genericRecord.put("binary_field", ByteBuffer.wrap(binaryBytes));

      BigDecimal bigDecimal = new BigDecimal("-1.50");
      // unscaledValue().toByteArray() is to match the behavior of RowDataToAvroConverters from
      // Flink for decimal type
      genericRecord.put("decimal_field", ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray()));

      genericRecord.put("fixed_field", ByteBuffer.wrap(FIXED_BYTES));

      return genericRecord;
    }
  }

  public static class StructOfPrimitive implements DataGenerator {
    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2,
                "struct_of_primitive",
                Types.StructType.of(
                    required(101, "id", Types.IntegerType.get()),
                    required(102, "name", Types.StringType.get()))));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    private final org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(icebergSchema, "table");

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      Schema structSchema =
          new Schema(icebergSchema.findField("struct_of_primitive").type().asStructType().fields());
      GenericRecord struct = GenericRecord.create(structSchema);
      struct.setField("id", 1);
      struct.setField("name", "Jane");
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField("struct_of_primitive", struct);
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      return GenericRowData.of(
          StringData.fromString("row_id_value"),
          GenericRowData.of(1, StringData.fromString("Jane")));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.Schema structSchema = avroSchema.getField("struct_of_primitive").schema();
      org.apache.avro.generic.GenericRecord struct = new GenericData.Record(structSchema);
      struct.put("id", 1);
      struct.put("name", "Jane");
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", "row_id_value");
      genericRecord.put("struct_of_primitive", struct);
      return genericRecord;
    }
  }

  public static class StructOfArray implements DataGenerator {
    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2,
                "struct_of_array",
                Types.StructType.of(
                    required(101, "id", Types.IntegerType.get()),
                    required(
                        102, "names", Types.ListType.ofRequired(201, Types.StringType.get())))));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    private final org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(icebergSchema, "table");

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      Schema structSchema =
          new Schema(icebergSchema.findField("struct_of_array").type().asStructType().fields());
      GenericRecord struct = GenericRecord.create(structSchema);
      struct.setField("id", 1);
      struct.setField("names", Arrays.asList("Jane", "Joe"));
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField("struct_of_array", struct);
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      StringData[] names = {StringData.fromString("Jane"), StringData.fromString("Joe")};
      return GenericRowData.of(
          StringData.fromString("row_id_value"), GenericRowData.of(1, new GenericArrayData(names)));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.Schema structSchema = avroSchema.getField("struct_of_array").schema();
      org.apache.avro.generic.GenericRecord struct = new GenericData.Record(structSchema);
      struct.put("id", 1);
      struct.put("names", Arrays.asList("Jane", "Joe"));
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", "row_id_value");
      genericRecord.put("struct_of_array", struct);
      return genericRecord;
    }
  }

  public static class StructOfMap implements DataGenerator {
    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2,
                "struct_of_map",
                Types.StructType.of(
                    required(101, "id", Types.IntegerType.get()),
                    required(
                        102,
                        "names",
                        Types.MapType.ofRequired(
                            201, 202, Types.StringType.get(), Types.StringType.get())))));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    private final org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(icebergSchema, "table");

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      Schema structSchema =
          new Schema(icebergSchema.findField("struct_of_map").type().asStructType().fields());
      GenericRecord struct = GenericRecord.create(structSchema);
      struct.setField("id", 1);
      struct.setField("names", ImmutableMap.of("Jane", "female", "Joe", "male"));
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField("struct_of_map", struct);
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      return GenericRowData.of(
          StringData.fromString("row_id_value"),
          GenericRowData.of(
              1,
              new GenericMapData(
                  ImmutableMap.of(
                      StringData.fromString("Jane"),
                      StringData.fromString("female"),
                      StringData.fromString("Joe"),
                      StringData.fromString("male")))));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.Schema structSchema = avroSchema.getField("struct_of_map").schema();
      org.apache.avro.generic.GenericRecord struct = new GenericData.Record(structSchema);
      struct.put("id", 1);
      struct.put("names", ImmutableMap.of("Jane", new Utf8("female"), "Joe", new Utf8("male")));
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", "row_id_value");
      genericRecord.put("struct_of_map", struct);
      return genericRecord;
    }
  }

  public static class StructOfStruct implements DataGenerator {
    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2,
                "struct_of_struct",
                Types.StructType.of(
                    required(101, "id", Types.IntegerType.get()),
                    required(
                        102,
                        "person_struct",
                        Types.StructType.of(
                            Types.NestedField.required(201, "name", Types.StringType.get()),
                            Types.NestedField.required(202, "address", Types.StringType.get()))))));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    private final org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(icebergSchema, "table");

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      Schema structSchema =
          new Schema(icebergSchema.findField("struct_of_struct").type().asStructType().fields());
      Schema personSchema =
          new Schema(structSchema.findField("person_struct").type().asStructType().fields());
      GenericRecord person = GenericRecord.create(personSchema);
      person.setField("name", "Jane");
      person.setField("address", "Apple Park");
      GenericRecord struct = GenericRecord.create(structSchema);
      struct.setField("id", 1);
      struct.setField("person_struct", person);
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField("struct_of_struct", struct);
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      return GenericRowData.of(
          StringData.fromString("row_id_value"),
          GenericRowData.of(
              1,
              GenericRowData.of(
                  StringData.fromString("Jane"), StringData.fromString("Apple Park"))));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.Schema structSchema = avroSchema.getField("struct_of_struct").schema();
      org.apache.avro.Schema personSchema = structSchema.getField("person_struct").schema();
      org.apache.avro.generic.GenericRecord person = new GenericData.Record(personSchema);
      person.put("name", "Jane");
      person.put("address", "Apple Park");
      org.apache.avro.generic.GenericRecord struct = new GenericData.Record(structSchema);
      struct.put("id", 1);
      struct.put("person_struct", person);
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", "row_id_value");
      genericRecord.put("struct_of_struct", struct);
      return genericRecord;
    }
  }

  public static class ArrayOfPrimitive implements DataGenerator {
    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2, "array_of_int", Types.ListType.ofOptional(101, Types.IntegerType.get())));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    private final org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(icebergSchema, "table");

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField("array_of_int", Arrays.asList(1, 2, 3));
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      Integer[] arr = {1, 2, 3};
      return GenericRowData.of(StringData.fromString("row_id_value"), new GenericArrayData(arr));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", "row_id_value");
      genericRecord.put("array_of_int", Arrays.asList(1, 2, 3));
      return genericRecord;
    }
  }

  public static class ArrayOfArray implements DataGenerator {
    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2,
                "array_of_array",
                Types.ListType.ofRequired(
                    101, Types.ListType.ofRequired(201, Types.IntegerType.get()))));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    private final org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(icebergSchema, "table");

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField(
          "array_of_array", Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6)));
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      // non-primitive
      Integer[] array1 = {1, 2, 3};
      Integer[] array2 = {4, 5, 6};
      GenericArrayData[] arrayOfArrays = {
        new GenericArrayData(array1), new GenericArrayData(array2)
      };
      return GenericRowData.of(
          StringData.fromString("row_id_value"), new GenericArrayData(arrayOfArrays));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", "row_id_value");
      genericRecord.put(
          "array_of_array", Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6)));
      return genericRecord;
    }
  }

  public static class ArrayOfMap implements DataGenerator {
    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2,
                "array_of_map",
                Types.ListType.ofRequired(
                    101,
                    Types.MapType.ofRequired(
                        201, 202, Types.StringType.get(), Types.IntegerType.get()))));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    private final org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(icebergSchema, "table");

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField(
          "array_of_map",
          Arrays.asList(
              ImmutableMap.of("Jane", 1, "Joe", 2), ImmutableMap.of("Alice", 3, "Bob", 4)));
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      GenericMapData[] array = {
        new GenericMapData(
            ImmutableMap.of(StringData.fromString("Jane"), 1, StringData.fromString("Joe"), 2)),
        new GenericMapData(
            ImmutableMap.of(StringData.fromString("Alice"), 3, StringData.fromString("Bob"), 4))
      };
      return GenericRowData.of(StringData.fromString("row_id_value"), new GenericArrayData(array));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", "row_id_value");
      genericRecord.put(
          "array_of_map",
          Arrays.asList(
              ImmutableMap.of("Jane", 1, "Joe", 2), ImmutableMap.of("Alice", 3, "Bob", 4)));
      return genericRecord;
    }
  }

  public static class ArrayOfStruct implements DataGenerator {
    private final Types.StructType structType =
        Types.StructType.of(
            required(201, "id", Types.IntegerType.get()),
            required(202, "name", Types.StringType.get()));
    private final Schema structIcebergSchema = new Schema(structType.fields());
    private final org.apache.avro.Schema structAvroSchema =
        AvroSchemaUtil.convert(structIcebergSchema, "struct");

    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.optional(
                2, "array_of_struct", Types.ListType.ofRequired(101, structType)));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    private final org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(icebergSchema, "table");

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      GenericRecord struct1 = GenericRecord.create(structIcebergSchema);
      struct1.setField("id", 1);
      struct1.setField("name", "Jane");
      GenericRecord struct2 = GenericRecord.create(structIcebergSchema);
      struct2.setField("id", 2);
      struct2.setField("name", "Joe");
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField("array_of_struct", Arrays.asList(struct1, struct2));
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      GenericRowData[] structArray = {
        GenericRowData.of(1, StringData.fromString("Jane")),
        GenericRowData.of(2, StringData.fromString("Joe"))
      };
      return GenericRowData.of(
          StringData.fromString("row_id_value"), new GenericArrayData(structArray));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.generic.GenericRecord struct1 = new GenericData.Record(structAvroSchema);
      struct1.put("id", 1);
      struct1.put("name", "Jane");
      org.apache.avro.generic.GenericRecord struct2 = new GenericData.Record(structAvroSchema);
      struct2.put("id", 2);
      struct2.put("name", "Joe");
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", "row_id_value");
      genericRecord.put("array_of_struct", Arrays.asList(struct1, struct2));
      return genericRecord;
    }
  }

  public static class MapOfPrimitives implements DataGenerator {
    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.optional(
                2,
                "map_of_primitives",
                Types.MapType.ofRequired(
                    101, 102, Types.StringType.get(), Types.IntegerType.get())));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    private final org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(icebergSchema, "table");

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField("map_of_primitives", ImmutableMap.of("Jane", 1, "Joe", 2));
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      return GenericRowData.of(
          StringData.fromString("row_id_value"),
          new GenericMapData(
              ImmutableMap.of(StringData.fromString("Jane"), 1, StringData.fromString("Joe"), 2)));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", "row_id_value");
      genericRecord.put("map_of_primitives", ImmutableMap.of("Jane", 1, "Joe", 2));
      return genericRecord;
    }
  }

  public static class MapOfArray implements DataGenerator {
    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2,
                "map_of_array",
                Types.MapType.ofRequired(
                    101,
                    102,
                    Types.StringType.get(),
                    Types.ListType.ofRequired(201, Types.IntegerType.get()))));

    private final RowType rowType = FlinkSchemaUtil.convert(icebergSchema);

    private final org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(icebergSchema, "table");

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return rowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField(
          "map_of_array",
          ImmutableMap.of(
              "Jane", Arrays.asList(1, 2, 3),
              "Joe", Arrays.asList(4, 5, 6)));
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      Integer[] janeArray = {1, 2, 3};
      Integer[] joeArray = {4, 5, 6};
      return GenericRowData.of(
          StringData.fromString("row_id_value"),
          new GenericMapData(
              ImmutableMap.of(
                  StringData.fromString("Jane"),
                  new GenericArrayData(janeArray),
                  StringData.fromString("Joe"),
                  new GenericArrayData(joeArray))));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", "row_id_value");
      genericRecord.put(
          "map_of_array",
          ImmutableMap.of(
              "Jane", Arrays.asList(1, 2, 3),
              "Joe", Arrays.asList(4, 5, 6)));
      return genericRecord;
    }
  }

  public static class MapOfMap implements DataGenerator {
    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2,
                "map_of_map",
                Types.MapType.ofRequired(
                    101,
                    102,
                    Types.StringType.get(),
                    Types.MapType.ofRequired(
                        301, 302, Types.StringType.get(), Types.IntegerType.get()))));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    private final org.apache.avro.Schema avroSchema =
        AvroSchemaUtil.convert(icebergSchema, "table");

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField(
          "map_of_map",
          ImmutableMap.of(
              "female", ImmutableMap.of("Jane", 1, "Alice", 2),
              "male", ImmutableMap.of("Joe", 3, "Bob", 4)));
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      return GenericRowData.of(
          StringData.fromString("row_id_value"),
          new GenericMapData(
              ImmutableMap.of(
                  StringData.fromString("female"),
                  new GenericMapData(
                      ImmutableMap.of(
                          StringData.fromString("Jane"), 1, StringData.fromString("Alice"), 2)),
                  StringData.fromString("male"),
                  new GenericMapData(
                      ImmutableMap.of(
                          StringData.fromString("Joe"), 3, StringData.fromString("Bob"), 4)))));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", "row_id_value");
      genericRecord.put(
          "map_of_map",
          ImmutableMap.of(
              "female", ImmutableMap.of("Jane", 1, "Alice", 2),
              "male", ImmutableMap.of("Joe", 3, "Bob", 4)));
      return genericRecord;
    }
  }

  public static class MapOfStruct implements DataGenerator {
    private org.apache.avro.Schema createAvroSchemaIdField() {
      org.apache.avro.Schema schema = SchemaBuilder.builder().intType();
      // this is needed to match the converter generated schema props
      schema.addProp("field-id", IntNode.valueOf(201));
      return schema;
    }

    private org.apache.avro.Schema createAvroSchemaNameField() {
      org.apache.avro.Schema schema = SchemaBuilder.builder().stringType();
      // this is needed to match the converter generated schema props
      schema.addProp("field-id", IntNode.valueOf(202));
      return schema;
    }

    private final Types.StructType structType =
        Types.StructType.of(
            required(201, "id", Types.IntegerType.get()),
            required(202, "name", Types.StringType.get()));
    private final Schema structIcebergSchema = new Schema(structType.fields());

    private final org.apache.avro.Schema structAvroSchema =
        SchemaBuilder.builder()
            .record("struct")
            .fields()
            .name("id")
            .type(createAvroSchemaIdField())
            .noDefault()
            .name("name")
            .type(createAvroSchemaNameField())
            .noDefault()
            .endRecord();

    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2,
                "map_of_struct",
                Types.MapType.ofRequired(101, 102, Types.StringType.get(), structType)));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    // Can't use AvroSchemaUtil.convert otherwise the nested schema will have generated name like
    // `r102` not the specified name like `struct`.
    org.apache.avro.Schema avroSchema =
        SchemaBuilder.builder()
            .record("table")
            .fields()
            .requiredString("row_id")
            .name("map_of_struct")
            .type(SchemaBuilder.builder().map().values(structAvroSchema))
            .noDefault()
            .endRecord();

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      return avroSchema;
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      GenericRecord struct1 = GenericRecord.create(structIcebergSchema);
      struct1.setField("id", 1);
      struct1.setField("name", "Jane");
      GenericRecord struct2 = GenericRecord.create(structIcebergSchema);
      struct2.setField("id", 2);
      struct2.setField("name", "Joe");
      GenericRecord genericRecord = GenericRecord.create(icebergSchema);
      genericRecord.setField("row_id", "row_id_value");
      genericRecord.setField(
          "map_of_struct", ImmutableMap.of("struct1", struct1, "struct2", struct2));
      return genericRecord;
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      return GenericRowData.of(
          StringData.fromString("row_id_value"),
          new GenericMapData(
              ImmutableMap.of(
                  StringData.fromString("struct1"),
                  GenericRowData.of(1, StringData.fromString("Jane")),
                  StringData.fromString("struct2"),
                  GenericRowData.of(2, StringData.fromString("Joe")))));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      org.apache.avro.generic.GenericRecord struct1 = new GenericData.Record(structAvroSchema);
      struct1.put("id", 1);
      struct1.put("name", new Utf8("Jane"));
      org.apache.avro.generic.GenericRecord struct2 = new GenericData.Record(structAvroSchema);
      struct2.put("id", 2);
      struct2.put("name", new Utf8("Joe"));
      org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(avroSchema);
      genericRecord.put("row_id", new Utf8("row_id_value"));
      genericRecord.put("map_of_struct", ImmutableMap.of("struct1", struct1, "struct2", struct2));
      return genericRecord;
    }
  }

  public static class MapOfStructStruct implements DataGenerator {
    private final Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.optional(
                2,
                "map",
                Types.MapType.ofOptional(
                    101,
                    102,
                    Types.StructType.of(
                        Types.NestedField.required(201, "key", Types.LongType.get()),
                        Types.NestedField.optional(202, "keyData", Types.StringType.get())),
                    Types.StructType.of(
                        Types.NestedField.required(203, "value", Types.LongType.get()),
                        Types.NestedField.optional(204, "valueData", Types.StringType.get())))));

    private final RowType flinkRowType = FlinkSchemaUtil.convert(icebergSchema);

    @Override
    public Schema icebergSchema() {
      return icebergSchema;
    }

    @Override
    public RowType flinkRowType() {
      return flinkRowType;
    }

    @Override
    public org.apache.avro.Schema avroSchema() {
      throw new UnsupportedOperationException(
          "Not applicable as Avro Map only support string key type");
    }

    @Override
    public GenericRecord generateIcebergGenericRecord() {
      throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public GenericRowData generateFlinkRowData() {
      return GenericRowData.of(
          StringData.fromString("row_id_value"),
          new GenericMapData(
              ImmutableMap.of(
                  GenericRowData.of(1L, StringData.fromString("key_data")),
                  GenericRowData.of(1L, StringData.fromString("value_data")))));
    }

    @Override
    public org.apache.avro.generic.GenericRecord generateAvroGenericRecord() {
      throw new UnsupportedOperationException("Avro Map only support string key type");
    }
  }
}
