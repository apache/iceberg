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
package org.apache.iceberg.connect.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TimeTypeConverterTest {
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
  private static final Calendar EPOCH;
  private static final Calendar TIME;
  private static final Calendar DATE;
  private static final Calendar DATE_PLUS_TIME;
  private static final long DATE_PLUS_TIME_UNIX;
  private static final String STRING_DATE_FMT = "yyyy MM dd HH mm ss SSS z";
  private static final String DATE_PLUS_TIME_STRING;

  private final TimeTypeConverter<SourceRecord> xformValue = new TimeTypeConverter.Value<>();

  static {
    EPOCH = GregorianCalendar.getInstance(UTC);
    EPOCH.setTimeInMillis(0L);

    TIME = GregorianCalendar.getInstance(UTC);
    TIME.setTimeInMillis(0L);
    TIME.add(Calendar.MILLISECOND, 1234);

    DATE = GregorianCalendar.getInstance(UTC);
    DATE.setTimeInMillis(0L);
    DATE.set(1970, Calendar.JANUARY, 1, 0, 0, 0);
    DATE.add(Calendar.DATE, 1);

    DATE_PLUS_TIME = GregorianCalendar.getInstance(UTC);
    DATE_PLUS_TIME.setTimeInMillis(0L);
    DATE_PLUS_TIME.add(Calendar.DATE, 1);
    DATE_PLUS_TIME.add(Calendar.MILLISECOND, 1234);
    // 86 401 234 milliseconds
    DATE_PLUS_TIME_UNIX = DATE_PLUS_TIME.getTime().getTime();
    DATE_PLUS_TIME_STRING = "1970 01 02 00 00 01 234 UTC";
  }

  @AfterEach
  public void after() {
    xformValue.close();
  }

  @Test
  public void tesWithNestedStruct() {
    Map<String, String> config = Maps.newHashMap();
    config.put(TimeTypeConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimeTypeConverter.FORMAT_CONFIG, STRING_DATE_FMT);
    config.put(TimeTypeConverter.INPUT_TYPE_CONFIG, "Timestamp");
    xformValue.configure(config);

    Schema nestedSchema =
        SchemaBuilder.struct()
            .field("nested_ts", Timestamp.SCHEMA)
            .field("other", Schema.STRING_SCHEMA)
            .build();

    Schema schema =
        SchemaBuilder.struct().field("ts", Timestamp.SCHEMA).field("nested", nestedSchema).build();

    Struct nestedValue = new Struct(nestedSchema);
    nestedValue.put("nested_ts", DATE_PLUS_TIME.getTime());
    nestedValue.put("other", "test");

    Struct value = new Struct(schema);
    value.put("ts", DATE_PLUS_TIME.getTime());
    value.put("nested", nestedValue);

    SourceRecord transformed = xformValue.apply(createRecordWithSchema(schema, value));

    Schema expectedNestedSchema =
        SchemaBuilder.struct()
            .field("nested_ts", Schema.STRING_SCHEMA)
            .field("other", Schema.STRING_SCHEMA)
            .build();

    Schema expectedSchema =
        SchemaBuilder.struct()
            .field("ts", Schema.STRING_SCHEMA)
            .field("nested", expectedNestedSchema)
            .build();

    assertThat(expectedSchema).isEqualTo(transformed.valueSchema());
    Struct transformedValue = (Struct) transformed.value();
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedValue.get("ts"));

    Struct transformedNested = (Struct) transformedValue.get("nested");
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedNested.get("nested_ts"));
    assertThat("test").isEqualTo(transformedNested.get("other"));
  }

  @Test
  public void testWithArrayOfTimestamps() {
    Map<String, String> config = Maps.newHashMap();
    config.put(TimeTypeConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimeTypeConverter.FORMAT_CONFIG, STRING_DATE_FMT);
    config.put(TimeTypeConverter.INPUT_TYPE_CONFIG, "Timestamp");
    xformValue.configure(config);

    Schema schema =
        SchemaBuilder.struct()
            .field("ts_array", SchemaBuilder.array(Timestamp.SCHEMA).build())
            .build();

    Struct value = new Struct(schema);
    value.put("ts_array", Arrays.asList(DATE_PLUS_TIME.getTime(), DATE_PLUS_TIME.getTime()));

    SourceRecord transformed = xformValue.apply(createRecordWithSchema(schema, value));

    Schema expectedSchema =
        SchemaBuilder.struct()
            .field("ts_array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build();

    assertThat(expectedSchema).isEqualTo(transformed.valueSchema());
    Struct transformedValue = (Struct) transformed.value();
    List<?> tsArray = (List<?>) transformedValue.get("ts_array");
    assertThat(2).isEqualTo(tsArray.size());
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(tsArray.get(0));
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(tsArray.get(1));
  }

  @Test
  public void testWithMapOfUnixTimestamps() {
    Map<String, String> config = Maps.newHashMap();
    config.put(TimeTypeConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimeTypeConverter.FORMAT_CONFIG, STRING_DATE_FMT);
    config.put(TimeTypeConverter.INPUT_TYPE_CONFIG, "unix");
    xformValue.configure(config);

    Schema schema =
        SchemaBuilder.struct()
            .field("ts_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).build())
            .build();

    Map<String, Long> tsMap = Maps.newHashMap();
    tsMap.put("first", DATE_PLUS_TIME_UNIX);
    tsMap.put("second", DATE_PLUS_TIME_UNIX);

    Struct value = new Struct(schema);
    value.put("ts_map", tsMap);

    SourceRecord transformed = xformValue.apply(createRecordWithSchema(schema, value));

    Schema expectedSchema =
        SchemaBuilder.struct()
            .field("ts_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
            .build();

    assertThat(expectedSchema).isEqualTo(transformed.valueSchema());
    Struct transformedValue = (Struct) transformed.value();
    Map<?, ?> transformedMap = (Map<?, ?>) transformedValue.get("ts_map");
    assertThat(2).isEqualTo(transformedMap.size());
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedMap.get("first"));
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedMap.get("second"));
  }

  @Test
  public void testWithPrimitiveTimestampField() {
    Map<String, String> config = Maps.newHashMap();
    config.put(TimeTypeConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimeTypeConverter.FORMAT_CONFIG, STRING_DATE_FMT);
    config.put(TimeTypeConverter.INPUT_TYPE_CONFIG, "Timestamp");
    xformValue.configure(config);

    Schema schema =
        SchemaBuilder.struct()
            .field("primitive_ts", Timestamp.SCHEMA)
            .field("primitive_int", Schema.INT32_SCHEMA)
            .field("primitive_str", Schema.STRING_SCHEMA)
            .build();

    Struct value = new Struct(schema);
    value.put("primitive_ts", DATE_PLUS_TIME.getTime());
    value.put("primitive_int", 42);
    value.put("primitive_str", "test");

    SourceRecord transformed = xformValue.apply(createRecordWithSchema(schema, value));

    Schema expectedSchema =
        SchemaBuilder.struct()
            .field("primitive_ts", Schema.STRING_SCHEMA)
            .field("primitive_int", Schema.INT32_SCHEMA)
            .field("primitive_str", Schema.STRING_SCHEMA)
            .build();

    assertThat(expectedSchema).isEqualTo(transformed.valueSchema());
    Struct transformedValue = (Struct) transformed.value();
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedValue.get("primitive_ts"));
    assertThat(42).isEqualTo(transformedValue.get("primitive_int"));
    assertThat("test").isEqualTo(transformedValue.get("primitive_str"));
  }

  @Test
  public void testWithMixedTypes() {
    Map<String, String> config = Maps.newHashMap();
    config.put(TimeTypeConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimeTypeConverter.FORMAT_CONFIG, STRING_DATE_FMT);
    config.put(TimeTypeConverter.INPUT_TYPE_CONFIG, "Timestamp");
    xformValue.configure(config);

    Schema nestedSchema =
        SchemaBuilder.struct()
            .field("nested_ts", Timestamp.SCHEMA)
            .field("nested_int", Schema.INT32_SCHEMA)
            .build();

    Schema schema =
        SchemaBuilder.struct()
            .field("ts", Timestamp.SCHEMA)
            .field("array_ts", SchemaBuilder.array(Timestamp.SCHEMA).build())
            .field("map_ts", SchemaBuilder.map(Schema.STRING_SCHEMA, Timestamp.SCHEMA).build())
            .field("nested", nestedSchema)
            .field("primitive", Schema.INT64_SCHEMA)
            .build();

    Struct nestedValue = new Struct(nestedSchema);
    nestedValue.put("nested_ts", DATE_PLUS_TIME.getTime());
    nestedValue.put("nested_int", 42);

    Struct value = new Struct(schema);
    value.put("ts", DATE_PLUS_TIME.getTime());
    value.put("array_ts", List.of(DATE_PLUS_TIME.getTime()));
    value.put("map_ts", Collections.singletonMap("key", DATE_PLUS_TIME.getTime()));
    value.put("nested", nestedValue);
    value.put("primitive", 123L);

    SourceRecord transformed = xformValue.apply(createRecordWithSchema(schema, value));

    // Verify all timestamp fields were converted to strings
    Struct transformedValue = (Struct) transformed.value();
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedValue.get("ts"));

    List<?> arrayTs = (List<?>) transformedValue.get("array_ts");
    assertThat(1).isEqualTo(arrayTs.size());
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(arrayTs.get(0));

    Map<?, ?> mapTs = (Map<?, ?>) transformedValue.get("map_ts");
    assertThat(1).isEqualTo(mapTs.size());
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(mapTs.get("key"));

    Struct transformedNested = (Struct) transformedValue.get("nested");
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedNested.get("nested_ts"));
    assertThat(42).isEqualTo(transformedNested.get("nested_int"));
  }

  @Test
  public void testWithNonTimestampFields() {
    Map<String, String> config = Maps.newHashMap();
    config.put(TimeTypeConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimeTypeConverter.FORMAT_CONFIG, STRING_DATE_FMT);
    config.put(TimeTypeConverter.INPUT_TYPE_CONFIG, "Timestamp");
    xformValue.configure(config);

    Schema schema =
        SchemaBuilder.struct()
            .field("int_field", Schema.INT32_SCHEMA)
            .field("str_field", Schema.STRING_SCHEMA)
            .field("bool_field", Schema.BOOLEAN_SCHEMA)
            .build();

    Struct value = new Struct(schema);
    value.put("int_field", 42);
    value.put("str_field", "test");
    value.put("bool_field", true);

    SourceRecord transformed = xformValue.apply(createRecordWithSchema(schema, value));

    // Schema should remain unchanged since no timestamp fields were found
    assertThat(schema).isEqualTo(transformed.valueSchema());
    Struct transformedValue = (Struct) transformed.value();
    assertThat(42).isEqualTo(transformedValue.get("int_field"));
    assertThat("test").isEqualTo(transformedValue.get("str_field"));
    assertThat((Boolean) transformedValue.get("bool_field")).isTrue();
  }

  @Test
  public void testWithDifferentInputTypes() {
    // Test with Unix timestamp input type
    Map<String, String> config = Maps.newHashMap();
    config.put(TimeTypeConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimeTypeConverter.FORMAT_CONFIG, STRING_DATE_FMT);
    config.put(TimeTypeConverter.INPUT_TYPE_CONFIG, "unix");
    xformValue.configure(config);

    Schema schema =
        SchemaBuilder.struct()
            .field("unix_ts", Schema.INT64_SCHEMA)
            .field("regular_ts", Timestamp.SCHEMA)
            .build();

    Struct value = new Struct(schema);
    value.put("unix_ts", DATE_PLUS_TIME_UNIX);
    value.put("regular_ts", DATE_PLUS_TIME.getTime());

    SourceRecord transformed = xformValue.apply(createRecordWithSchema(schema, value));

    // Only the unix timestamp field should be converted
    Struct transformedValue = (Struct) transformed.value();
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedValue.get("unix_ts"));
    assertThat(DATE_PLUS_TIME.getTime()).isEqualTo(transformedValue.get("regular_ts"));
  }

  @Test
  public void testWithOptionalFields() {
    Map<String, String> config = Maps.newHashMap();
    config.put(TimeTypeConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimeTypeConverter.FORMAT_CONFIG, STRING_DATE_FMT);
    config.put(TimeTypeConverter.INPUT_TYPE_CONFIG, "Timestamp");
    xformValue.configure(config);

    Schema schema =
        SchemaBuilder.struct()
            .field("optional_ts", Timestamp.builder().optional().build())
            .field("required_ts", Timestamp.SCHEMA)
            .build();

    Struct value = new Struct(schema);
    value.put("optional_ts", null);
    value.put("required_ts", DATE_PLUS_TIME.getTime());

    SourceRecord transformed = xformValue.apply(createRecordWithSchema(schema, value));

    Schema expectedSchema =
        SchemaBuilder.struct()
            .field("optional_ts", Schema.OPTIONAL_STRING_SCHEMA)
            .field("required_ts", Schema.STRING_SCHEMA)
            .build();

    assertThat(expectedSchema).isEqualTo(transformed.valueSchema());
    Struct transformedValue = (Struct) transformed.value();
    assertThat(transformedValue.get("optional_ts")).isNull();
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedValue.get("required_ts"));
  }

  @Test
  public void testWithDeeplyNestedStructures() {
    Map<String, String> config = Maps.newHashMap();
    config.put(TimeTypeConverter.TARGET_TYPE_CONFIG, "string");
    config.put(TimeTypeConverter.FORMAT_CONFIG, STRING_DATE_FMT);
    config.put(TimeTypeConverter.INPUT_TYPE_CONFIG, "Timestamp");
    xformValue.configure(config);

    // Create a deeply nested schema
    Schema innerMostSchema = SchemaBuilder.struct().field("ts", Timestamp.SCHEMA).build();

    Schema middleSchema =
        SchemaBuilder.struct()
            .field("inner", innerMostSchema)
            .field("ts_array", SchemaBuilder.array(Timestamp.SCHEMA).build())
            .build();

    Schema outerSchema =
        SchemaBuilder.struct()
            .field("middle", middleSchema)
            .field("ts_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Timestamp.SCHEMA).build())
            .build();

    // Create test data
    Struct innerMost = new Struct(innerMostSchema);
    innerMost.put("ts", DATE_PLUS_TIME.getTime());

    Struct middle = new Struct(middleSchema);
    middle.put("inner", innerMost);
    middle.put("ts_array", List.of(DATE_PLUS_TIME.getTime()));

    Struct outer = new Struct(outerSchema);
    outer.put("middle", middle);
    outer.put("ts_map", Collections.singletonMap("key", DATE_PLUS_TIME.getTime()));

    SourceRecord transformed = xformValue.apply(createRecordWithSchema(outerSchema, outer));

    // Verify all timestamp fields were converted at all nesting levels
    Struct transformedValue = (Struct) transformed.value();

    // Check ts_map
    Map<?, ?> transformedMap = (Map<?, ?>) transformedValue.get("ts_map");
    assertThat(1).isEqualTo(transformedMap.size());
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedMap.get("key"));

    // Check middle struct
    Struct transformedMiddle = (Struct) transformedValue.get("middle");

    // Check ts_array
    List<?> transformedArray = (List<?>) transformedMiddle.get("ts_array");
    assertThat(1).isEqualTo(transformedArray.size());
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedArray.get(0));

    // Check inner struct
    Struct transformedInner = (Struct) transformedMiddle.get("inner");
    assertThat(DATE_PLUS_TIME_STRING).isEqualTo(transformedInner.get("ts"));
  }

  private SourceRecord createRecordWithSchema(Schema schema, Object value) {
    return new SourceRecord(null, null, "topic", 0, schema, value);
  }
}
