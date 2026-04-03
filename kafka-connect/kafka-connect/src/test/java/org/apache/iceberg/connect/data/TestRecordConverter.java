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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SchemaUpdate.AddColumn;
import org.apache.iceberg.connect.data.SchemaUpdate.UpdateType;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.types.Types.UUIDType;
import org.apache.iceberg.types.Types.VariantType;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantValue;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestRecordConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final org.apache.iceberg.Schema SCHEMA =
      new org.apache.iceberg.Schema(
          NestedField.required(20, "i", IntegerType.get()),
          NestedField.required(21, "l", LongType.get()),
          NestedField.required(22, "d", DateType.get()),
          NestedField.required(23, "t", TimeType.get()),
          NestedField.required(24, "ts", TimestampType.withoutZone()),
          NestedField.required(25, "tsz", TimestampType.withZone()),
          NestedField.required(26, "fl", FloatType.get()),
          NestedField.required(27, "do", DoubleType.get()),
          NestedField.required(28, "dec", DecimalType.of(9, 2)),
          NestedField.required(29, "s", StringType.get()),
          NestedField.required(30, "b", BooleanType.get()),
          NestedField.required(31, "u", UUIDType.get()),
          NestedField.required(32, "f", FixedType.ofLength(3)),
          NestedField.required(33, "bi", BinaryType.get()),
          NestedField.required(34, "li", ListType.ofRequired(35, StringType.get())),
          NestedField.required(
              36, "ma", MapType.ofRequired(37, 38, StringType.get(), StringType.get())),
          NestedField.optional(39, "extra", StringType.get()));

  // we have 1 unmapped column so exclude that from the count
  private static final int MAPPED_CNT = SCHEMA.columns().size() - 1;

  private static final org.apache.iceberg.Schema NESTED_SCHEMA =
      new org.apache.iceberg.Schema(
          NestedField.required(1, "ii", IntegerType.get()),
          NestedField.required(2, "st", SCHEMA.asStruct()));

  private static final org.apache.iceberg.Schema SIMPLE_SCHEMA =
      new org.apache.iceberg.Schema(
          NestedField.required(1, "ii", IntegerType.get()),
          NestedField.required(2, "st", StringType.get()));

  private static final org.apache.iceberg.Schema ID_SCHEMA =
      new org.apache.iceberg.Schema(NestedField.required(1, "ii", IntegerType.get()));

  private static final org.apache.iceberg.Schema STRUCT_IN_LIST_SCHEMA =
      new org.apache.iceberg.Schema(
          NestedField.required(100, "stli", ListType.ofRequired(101, NESTED_SCHEMA.asStruct())));

  private static final org.apache.iceberg.Schema STRUCT_IN_LIST_BASIC_SCHEMA =
      new org.apache.iceberg.Schema(
          NestedField.required(100, "stli", ListType.ofRequired(101, ID_SCHEMA.asStruct())));

  private static final org.apache.iceberg.Schema STRUCT_IN_MAP_SCHEMA =
      new org.apache.iceberg.Schema(
          NestedField.required(
              100,
              "stma",
              MapType.ofRequired(101, 102, StringType.get(), NESTED_SCHEMA.asStruct())));

  private static final org.apache.iceberg.Schema STRUCT_IN_MAP_BASIC_SCHEMA =
      new org.apache.iceberg.Schema(
          NestedField.required(
              100, "stma", MapType.ofRequired(101, 102, StringType.get(), ID_SCHEMA.asStruct())));

  private static final org.apache.iceberg.Schema VARIANT_SCHEMA =
      new org.apache.iceberg.Schema(NestedField.required(1, "v", VariantType.get()));

  private static final Schema CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .field("i", Schema.INT32_SCHEMA)
          .field("l", Schema.INT64_SCHEMA)
          .field("d", org.apache.kafka.connect.data.Date.SCHEMA)
          .field("t", Time.SCHEMA)
          .field("ts", Timestamp.SCHEMA)
          .field("tsz", Timestamp.SCHEMA)
          .field("fl", Schema.FLOAT32_SCHEMA)
          .field("do", Schema.FLOAT64_SCHEMA)
          .field("dec", Decimal.schema(2))
          .field("s", Schema.STRING_SCHEMA)
          .field("b", Schema.BOOLEAN_SCHEMA)
          .field("u", Schema.STRING_SCHEMA)
          .field("f", Schema.BYTES_SCHEMA)
          .field("bi", Schema.BYTES_SCHEMA)
          .field("li", SchemaBuilder.array(Schema.STRING_SCHEMA))
          .field("ma", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA));

  private static final Schema CONNECT_NESTED_SCHEMA =
      SchemaBuilder.struct().field("ii", Schema.INT32_SCHEMA).field("st", CONNECT_SCHEMA);

  private static final Schema CONNECT_STRUCT_IN_LIST_SCHEMA =
      SchemaBuilder.struct().field("stli", SchemaBuilder.array(CONNECT_NESTED_SCHEMA)).build();

  private static final Schema CONNECT_STRUCT_IN_MAP_SCHEMA =
      SchemaBuilder.struct()
          .field("stma", SchemaBuilder.map(Schema.STRING_SCHEMA, CONNECT_NESTED_SCHEMA))
          .build();

  private static final LocalDate DATE_VAL = LocalDate.parse("2023-05-18");
  private static final LocalTime TIME_VAL = LocalTime.parse("07:14:21");
  private static final LocalDateTime TS_VAL = LocalDateTime.parse("2023-05-18T07:14:21");
  private static final OffsetDateTime TSZ_VAL = OffsetDateTime.parse("2023-05-18T07:14:21Z");
  private static final BigDecimal DEC_VAL = new BigDecimal("12.34");
  private static final String STR_VAL = "foobar";
  private static final UUID UUID_VAL = UUID.randomUUID();
  private static final ByteBuffer BYTES_VAL = ByteBuffer.wrap(new byte[] {1, 2, 3});
  private static final List<String> LIST_VAL = ImmutableList.of("hello", "world");
  private static final Map<String, String> MAP_VAL = ImmutableMap.of("one", "1", "two", "2");

  private static final JsonConverter JSON_CONVERTER = new JsonConverter();

  private IcebergSinkConfig config;

  @BeforeAll
  public static void beforeAll() {
    JSON_CONVERTER.configure(
        ImmutableMap.of(
            JsonConverterConfig.SCHEMAS_ENABLE_CONFIG,
            false,
            ConverterConfig.TYPE_CONFIG,
            ConverterType.VALUE.getName()));
  }

  @BeforeEach
  public void before() {
    this.config = mock(IcebergSinkConfig.class);
    when(config.jsonConverter()).thenReturn(JSON_CONVERTER);
  }

  @Test
  public void testMapConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Map<String, Object> data = createMapData();
    Record record = converter.convert(data);
    assertRecordValues(record);
  }

  @Test
  public void testUUIDConversionWithParquet() {
    Table table = mock(Table.class);
    when(table.schema())
        .thenReturn(new org.apache.iceberg.Schema(NestedField.required(1, "uuid", UUIDType.get())));
    when(config.writeProps())
        .thenReturn(
            ImmutableMap.of(
                TableProperties.DEFAULT_FILE_FORMAT,
                FileFormat.PARQUET.name().toLowerCase(Locale.ROOT)));

    RecordConverter converter = new RecordConverter(table, config);
    Map<String, Object> data =
        ImmutableMap.<String, Object>builder().put("uuid", UUID_VAL.toString()).build();

    Record record = converter.convert(data);
    assertThat(record.getField("uuid")).isEqualTo(UUIDUtil.convert(UUID_VAL));
  }

  @Test
  public void testNestedMapConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(NESTED_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Map<String, Object> nestedData = createNestedMapData();
    Record record = converter.convert(nestedData);
    assertNestedRecordValues(record);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapToString() throws Exception {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Map<String, Object> nestedData = createNestedMapData();
    Record record = converter.convert(nestedData);

    String str = (String) record.getField("st");
    Map<String, Object> map = (Map<String, Object>) MAPPER.readValue(str, Map.class);
    assertThat(map).hasSize(MAPPED_CNT);
  }

  @Test
  public void testMapValueInListConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(STRUCT_IN_LIST_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Map<String, Object> data = createNestedMapData();
    Record record = converter.convert(ImmutableMap.of("stli", ImmutableList.of(data, data)));
    List<?> fieldVal = (List<?>) record.getField("stli");

    Record elementVal = (Record) fieldVal.get(0);
    assertNestedRecordValues(elementVal);
  }

  @Test
  public void testMapValueInMapConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(STRUCT_IN_MAP_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Map<String, Object> data = createNestedMapData();
    Record record =
        converter.convert(ImmutableMap.of("stma", ImmutableMap.of("key1", data, "key2", data)));

    Map<?, ?> fieldVal = (Map<?, ?>) record.getField("stma");
    Record mapVal = (Record) fieldVal.get("key1");
    assertNestedRecordValues(mapVal);
  }

  @Test
  public void testStructConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Struct data = createStructData();
    Record record = converter.convert(data);
    assertRecordValues(record);
  }

  @Test
  public void testNestedStructConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(NESTED_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Struct nestedData = createNestedStructData();
    Record record = converter.convert(nestedData);
    assertNestedRecordValues(record);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStructToString() throws Exception {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Struct nestedData = createNestedStructData();
    Record record = converter.convert(nestedData);

    String str = (String) record.getField("st");
    Map<String, Object> map = (Map<String, Object>) MAPPER.readValue(str, Map.class);
    assertThat(map).hasSize(MAPPED_CNT);
  }

  @Test
  public void testStructValueInListConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(STRUCT_IN_LIST_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Struct data = createNestedStructData();
    Struct struct =
        new Struct(CONNECT_STRUCT_IN_LIST_SCHEMA).put("stli", ImmutableList.of(data, data));
    Record record = converter.convert(struct);

    List<?> fieldVal = (List<?>) record.getField("stli");
    Record elementVal = (Record) fieldVal.get(0);
    assertNestedRecordValues(elementVal);
  }

  @Test
  public void testStructValueInMapConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(STRUCT_IN_MAP_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Struct data = createNestedStructData();
    Struct struct =
        new Struct(CONNECT_STRUCT_IN_MAP_SCHEMA)
            .put("stma", ImmutableMap.of("key1", data, "key2", data));
    Record record = converter.convert(struct);

    Map<?, ?> fieldVal = (Map<?, ?>) record.getField("stma");
    Record mapVal = (Record) fieldVal.get("key1");
    assertNestedRecordValues(mapVal);
  }

  @Test
  public void testNameMapping() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);

    NameMapping nameMapping = NameMapping.of(MappedField.of(1, ImmutableList.of("renamed_ii")));
    when(table.properties())
        .thenReturn(
            ImmutableMap.of(
                TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping)));

    RecordConverter converter = new RecordConverter(table, config);

    Map<String, Object> data = ImmutableMap.of("renamed_ii", 123);
    Record record = converter.convert(data);
    assertThat(record.getField("ii")).isEqualTo(123);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCaseSensitivity(boolean caseInsensitive) {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);

    when(config.schemaCaseInsensitive()).thenReturn(caseInsensitive);

    RecordConverter converter = new RecordConverter(table, config);

    Map<String, Object> mapData = ImmutableMap.of("II", 123);
    Record record1 = converter.convert(mapData);

    Struct structData =
        new Struct(SchemaBuilder.struct().field("II", Schema.INT32_SCHEMA).build()).put("II", 123);
    Record record2 = converter.convert(structData);

    if (caseInsensitive) {
      assertThat(record1.getField("ii")).isEqualTo(123);
      assertThat(record2.getField("ii")).isEqualTo(123);
    } else {
      assertThat(record1.getField("ii")).isEqualTo(null);
      assertThat(record2.getField("ii")).isEqualTo(null);
    }
  }

  @Test
  public void testIntConversion() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);

    RecordConverter converter = new RecordConverter(table, config);

    int expectedInt = 123;

    ImmutableList.of("123", 123.0f, 123.0d, 123L, expectedInt)
        .forEach(
            input -> {
              int val = converter.convertInt(input);
              assertThat(val).isEqualTo(expectedInt);
            });
  }

  @Test
  public void testLongConversion() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);

    RecordConverter converter = new RecordConverter(table, config);

    long expectedLong = 123L;

    ImmutableList.of("123", 123.0f, 123.0d, 123, expectedLong)
        .forEach(
            input -> {
              long val = converter.convertLong(input);
              assertThat(val).isEqualTo(expectedLong);
            });
  }

  @Test
  public void testFloatConversion() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);

    RecordConverter converter = new RecordConverter(table, config);

    float expectedFloat = 123f;

    ImmutableList.of("123", 123, 123L, 123d, expectedFloat)
        .forEach(
            input -> {
              float val = converter.convertFloat(input);
              assertThat(val).isEqualTo(expectedFloat);
            });
  }

  @Test
  public void testDoubleConversion() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);

    RecordConverter converter = new RecordConverter(table, config);

    double expectedDouble = 123d;

    ImmutableList.of("123", 123, 123L, 123f, expectedDouble)
        .forEach(
            input -> {
              double val = converter.convertDouble(input);
              assertThat(val).isEqualTo(expectedDouble);
            });
  }

  @Test
  public void testDecimalConversion() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);

    RecordConverter converter = new RecordConverter(table, config);

    BigDecimal expected = new BigDecimal("123.45");

    ImmutableList.of("123.45", 123.45d, expected)
        .forEach(
            input -> {
              BigDecimal decimal = converter.convertDecimal(input, DecimalType.of(10, 2));
              assertThat(decimal).isEqualTo(expected);
            });

    BigDecimal expected2 = new BigDecimal(123);

    ImmutableList.of("123", 123, expected2)
        .forEach(
            input -> {
              BigDecimal decimal = converter.convertDecimal(input, DecimalType.of(10, 0));
              assertThat(decimal).isEqualTo(expected2);
            });
  }

  @Test
  public void testDateConversion() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    LocalDate expected = LocalDate.of(2023, 11, 15);

    List<Object> inputList =
        ImmutableList.of(
            "2023-11-15",
            expected.toEpochDay(),
            expected,
            new Date(Duration.ofDays(expected.toEpochDay()).toMillis()));

    inputList.forEach(
        input -> {
          Temporal ts = converter.convertDateValue(input);
          assertThat(ts).isEqualTo(expected);
        });
  }

  @Test
  public void testTimeConversion() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    LocalTime expected = LocalTime.of(7, 51, 30, 888_000_000);

    List<Object> inputList =
        ImmutableList.of(
            "07:51:30.888",
            expected.toNanoOfDay() / 1000 / 1000,
            expected,
            new Date(expected.toNanoOfDay() / 1000 / 1000));

    inputList.forEach(
        input -> {
          Temporal ts = converter.convertTimeValue(input);
          assertThat(ts).isEqualTo(expected);
        });
  }

  @Test
  public void testTimestampWithZoneConversion() {
    OffsetDateTime expected = OffsetDateTime.parse("2023-05-18T11:22:33Z");
    long expectedMillis = expected.toInstant().toEpochMilli();
    assertTimestampConvert(expected, expectedMillis, TimestampType.withZone());

    // zone should be respected
    expected = OffsetDateTime.parse("2023-05-18T03:22:33-08:00");
    List<Object> additionalInput =
        ImmutableList.of(
            "2023-05-18T03:22:33-08",
            "2023-05-18 03:22:33-08",
            "2023-05-18T03:22:33-08:00",
            "2023-05-18 03:22:33-08:00",
            "2023-05-18T03:22:33-0800",
            "2023-05-18 03:22:33-0800");
    assertTimestampConvert(expected, additionalInput, TimestampType.withZone());
  }

  @Test
  public void testTimestampWithoutZoneConversion() {
    LocalDateTime expected = LocalDateTime.parse("2023-05-18T11:22:33");
    long expectedMillis = expected.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
    assertTimestampConvert(expected, expectedMillis, TimestampType.withoutZone());

    // zone should be ignored
    List<Object> additionalInput =
        ImmutableList.of(
            "2023-05-18T11:22:33-08",
            "2023-05-18 11:22:33-08",
            "2023-05-18T11:22:33-08:00",
            "2023-05-18 11:22:33-08:00",
            "2023-05-18T11:22:33-0800",
            "2023-05-18 11:22:33-0800");
    assertTimestampConvert(expected, additionalInput, TimestampType.withoutZone());
  }

  private void assertTimestampConvert(Temporal expected, long expectedMillis, TimestampType type) {
    List<Object> inputList =
        Lists.newArrayList(
            "2023-05-18T11:22:33Z",
            "2023-05-18 11:22:33Z",
            "2023-05-18T11:22:33+00",
            "2023-05-18 11:22:33+00",
            "2023-05-18T11:22:33+00:00",
            "2023-05-18 11:22:33+00:00",
            "2023-05-18T11:22:33+0000",
            "2023-05-18 11:22:33+0000",
            "2023-05-18T11:22:33",
            "2023-05-18 11:22:33",
            expectedMillis,
            new Date(expectedMillis),
            OffsetDateTime.ofInstant(Instant.ofEpochMilli(expectedMillis), ZoneOffset.UTC),
            LocalDateTime.ofInstant(Instant.ofEpochMilli(expectedMillis), ZoneOffset.UTC));

    assertTimestampConvert(expected, inputList, type);
  }

  private void assertTimestampConvert(
      Temporal expected, List<Object> inputList, TimestampType type) {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    inputList.forEach(
        input -> {
          Temporal ts = converter.convertTimestampValue(input, type);
          assertThat(ts).isEqualTo(expected);
        });
  }

  @Test
  public void testMissingColumnDetectionMap() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(ID_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Map<String, Object> data = Maps.newHashMap(createMapData());
    data.put("null", null);

    SchemaUpdate.Consumer consumer = new SchemaUpdate.Consumer();
    converter.convert(data, consumer);
    Collection<AddColumn> addCols = consumer.addColumns();

    assertThat(addCols).hasSize(MAPPED_CNT);

    Map<String, AddColumn> newColMap = Maps.newHashMap();
    addCols.forEach(addCol -> newColMap.put(addCol.name(), addCol));

    assertTypesAddedFromMap(col -> newColMap.get(col).type());

    // null values should be ignored
    assertThat(newColMap).doesNotContainKey("null");
  }

  @Test
  public void testMissingColumnDetectionMapNested() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(ID_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Map<String, Object> nestedData = createNestedMapData();
    SchemaUpdate.Consumer consumer = new SchemaUpdate.Consumer();
    converter.convert(nestedData, consumer);
    Collection<AddColumn> addCols = consumer.addColumns();

    assertThat(addCols).hasSize(1);

    AddColumn addCol = addCols.iterator().next();
    assertThat(addCol.name()).isEqualTo("st");

    StructType addedType = addCol.type().asStructType();
    assertThat(addedType.fields()).hasSize(MAPPED_CNT);
    assertTypesAddedFromMap(col -> addedType.field(col).type());
  }

  @Test
  public void testMissingColumnDetectionMapListValue() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(STRUCT_IN_LIST_BASIC_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Map<String, Object> nestedData = createNestedMapData();
    Map<String, Object> map = ImmutableMap.of("stli", ImmutableList.of(nestedData, nestedData));
    SchemaUpdate.Consumer consumer = new SchemaUpdate.Consumer();
    converter.convert(map, consumer);
    Collection<AddColumn> addCols = consumer.addColumns();

    assertThat(addCols).hasSize(1);

    AddColumn addCol = addCols.iterator().next();
    assertThat(addCol.parentName()).isEqualTo("stli.element");
    assertThat(addCol.name()).isEqualTo("st");

    StructType nestedElementType = addCol.type().asStructType();
    assertThat(nestedElementType.fields()).hasSize(MAPPED_CNT);
    assertTypesAddedFromMap(col -> nestedElementType.field(col).type());
  }

  private void assertTypesAddedFromMap(Function<String, Type> fn) {
    assertThat(fn.apply("i")).isInstanceOf(LongType.class);
    assertThat(fn.apply("l")).isInstanceOf(LongType.class);
    assertThat(fn.apply("d")).isInstanceOf(StringType.class);
    assertThat(fn.apply("t")).isInstanceOf(StringType.class);
    assertThat(fn.apply("ts")).isInstanceOf(StringType.class);
    assertThat(fn.apply("tsz")).isInstanceOf(StringType.class);
    assertThat(fn.apply("fl")).isInstanceOf(DoubleType.class);
    assertThat(fn.apply("do")).isInstanceOf(DoubleType.class);
    assertThat(fn.apply("dec")).isInstanceOf(StringType.class);
    assertThat(fn.apply("s")).isInstanceOf(StringType.class);
    assertThat(fn.apply("b")).isInstanceOf(BooleanType.class);
    assertThat(fn.apply("u")).isInstanceOf(StringType.class);
    assertThat(fn.apply("f")).isInstanceOf(StringType.class);
    assertThat(fn.apply("bi")).isInstanceOf(StringType.class);
    assertThat(fn.apply("li")).isInstanceOf(ListType.class);
    assertThat(fn.apply("ma")).isInstanceOf(StructType.class);
  }

  @Test
  public void testMissingColumnDetectionStruct() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(ID_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Struct data = createStructData();
    SchemaUpdate.Consumer consumer = new SchemaUpdate.Consumer();
    converter.convert(data, consumer);
    Collection<AddColumn> addCols = consumer.addColumns();

    assertThat(addCols).hasSize(MAPPED_CNT);

    Map<String, AddColumn> newColMap = Maps.newHashMap();
    addCols.forEach(addCol -> newColMap.put(addCol.name(), addCol));

    assertTypesAddedFromStruct(col -> newColMap.get(col).type());
  }

  @Test
  public void testMissingColumnDetectionStructNested() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(ID_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Struct nestedData = createNestedStructData();
    SchemaUpdate.Consumer consumer = new SchemaUpdate.Consumer();
    converter.convert(nestedData, consumer);
    Collection<AddColumn> addCols = consumer.addColumns();

    assertThat(addCols).hasSize(1);

    AddColumn addCol = addCols.iterator().next();
    assertThat(addCol.name()).isEqualTo("st");

    StructType addedType = addCol.type().asStructType();
    assertThat(addedType.fields()).hasSize(MAPPED_CNT);
    assertTypesAddedFromStruct(col -> addedType.field(col).type());
  }

  @Test
  public void testMissingColumnDetectionStructListValue() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(STRUCT_IN_LIST_BASIC_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Struct nestedData = createNestedStructData();
    Struct struct =
        new Struct(CONNECT_STRUCT_IN_LIST_SCHEMA)
            .put("stli", ImmutableList.of(nestedData, nestedData));
    SchemaUpdate.Consumer consumer = new SchemaUpdate.Consumer();
    converter.convert(struct, consumer);
    Collection<AddColumn> addCols = consumer.addColumns();

    assertThat(addCols).hasSize(1);

    AddColumn addCol = addCols.iterator().next();
    assertThat(addCol.parentName()).isEqualTo("stli.element");
    assertThat(addCol.name()).isEqualTo("st");

    StructType nestedElementType = addCol.type().asStructType();
    assertThat(nestedElementType.fields()).hasSize(MAPPED_CNT);
    assertTypesAddedFromStruct(col -> nestedElementType.field(col).type());
  }

  @Test
  public void testMissingColumnDetectionStructMapValue() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(STRUCT_IN_MAP_BASIC_SCHEMA);
    RecordConverter converter = new RecordConverter(table, config);

    Struct nestedData = createNestedStructData();
    Struct struct =
        new Struct(CONNECT_STRUCT_IN_MAP_SCHEMA)
            .put("stma", ImmutableMap.of("key1", nestedData, "key2", nestedData));
    SchemaUpdate.Consumer consumer = new SchemaUpdate.Consumer();
    converter.convert(struct, consumer);
    Collection<AddColumn> addCols = consumer.addColumns();

    assertThat(addCols).hasSize(1);

    AddColumn addCol = addCols.iterator().next();
    assertThat(addCol.parentName()).isEqualTo("stma.value");
    assertThat(addCol.name()).isEqualTo("st");

    StructType nestedValueType = addCol.type().asStructType();
    assertThat(nestedValueType.fields()).hasSize(MAPPED_CNT);
    assertTypesAddedFromStruct(col -> nestedValueType.field(col).type());
  }

  private void assertTypesAddedFromStruct(Function<String, Type> fn) {
    assertThat(fn.apply("i")).isInstanceOf(IntegerType.class);
    assertThat(fn.apply("l")).isInstanceOf(LongType.class);
    assertThat(fn.apply("d")).isInstanceOf(DateType.class);
    assertThat(fn.apply("t")).isInstanceOf(TimeType.class);
    assertThat(fn.apply("ts")).isInstanceOf(TimestampType.class);
    assertThat(fn.apply("tsz")).isInstanceOf(TimestampType.class);
    assertThat(fn.apply("fl")).isInstanceOf(FloatType.class);
    assertThat(fn.apply("do")).isInstanceOf(DoubleType.class);
    assertThat(fn.apply("dec")).isInstanceOf(DecimalType.class);
    assertThat(fn.apply("s")).isInstanceOf(StringType.class);
    assertThat(fn.apply("b")).isInstanceOf(BooleanType.class);
    assertThat(fn.apply("u")).isInstanceOf(StringType.class);
    assertThat(fn.apply("f")).isInstanceOf(BinaryType.class);
    assertThat(fn.apply("bi")).isInstanceOf(BinaryType.class);
    assertThat(fn.apply("li")).isInstanceOf(ListType.class);
    assertThat(fn.apply("ma")).isInstanceOf(MapType.class);
  }

  @Test
  public void testEvolveTypeDetectionStruct() {
    org.apache.iceberg.Schema tableSchema =
        new org.apache.iceberg.Schema(
            NestedField.required(1, "ii", IntegerType.get()),
            NestedField.required(2, "ff", FloatType.get()));

    Table table = mock(Table.class);
    when(table.schema()).thenReturn(tableSchema);
    RecordConverter converter = new RecordConverter(table, config);

    Schema valueSchema =
        SchemaBuilder.struct().field("ii", Schema.INT64_SCHEMA).field("ff", Schema.FLOAT64_SCHEMA);
    Struct data = new Struct(valueSchema).put("ii", 11L).put("ff", 22d);

    SchemaUpdate.Consumer consumer = new SchemaUpdate.Consumer();
    converter.convert(data, consumer);
    Collection<UpdateType> updates = consumer.updateTypes();

    assertThat(updates).hasSize(2);

    Map<String, UpdateType> updateMap = Maps.newHashMap();
    updates.forEach(update -> updateMap.put(update.name(), update));

    assertThat(updateMap.get("ii").type()).isInstanceOf(LongType.class);
    assertThat(updateMap.get("ff").type()).isInstanceOf(DoubleType.class);
  }

  @Test
  public void testEvolveTypeDetectionStructNested() {
    org.apache.iceberg.Schema structColSchema =
        new org.apache.iceberg.Schema(
            NestedField.required(1, "ii", IntegerType.get()),
            NestedField.required(2, "ff", FloatType.get()));

    org.apache.iceberg.Schema tableSchema =
        new org.apache.iceberg.Schema(
            NestedField.required(3, "i", IntegerType.get()),
            NestedField.required(4, "st", structColSchema.asStruct()));

    Table table = mock(Table.class);
    when(table.schema()).thenReturn(tableSchema);
    RecordConverter converter = new RecordConverter(table, config);

    Schema structSchema =
        SchemaBuilder.struct().field("ii", Schema.INT64_SCHEMA).field("ff", Schema.FLOAT64_SCHEMA);
    Schema schema =
        SchemaBuilder.struct().field("i", Schema.INT32_SCHEMA).field("st", structSchema);
    Struct structValue = new Struct(structSchema).put("ii", 11L).put("ff", 22d);
    Struct data = new Struct(schema).put("i", 1).put("st", structValue);

    SchemaUpdate.Consumer consumer = new SchemaUpdate.Consumer();
    converter.convert(data, consumer);
    Collection<UpdateType> updates = consumer.updateTypes();

    assertThat(updates).hasSize(2);

    Map<String, UpdateType> updateMap = Maps.newHashMap();
    updates.forEach(update -> updateMap.put(update.name(), update));

    assertThat(updateMap.get("st.ii").type()).isInstanceOf(LongType.class);
    assertThat(updateMap.get("st.ff").type()).isInstanceOf(DoubleType.class);
  }

  private RecordConverter variantConverter() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(VARIANT_SCHEMA);
    return new RecordConverter(table, config);
  }

  @Test
  public void testConvertVariantValueFromNull() {
    Variant variant = variantConverter().convertVariantValue(null);
    assertThat(variant).isNotNull();
    assertThat(variant.value().type()).isEqualTo(PhysicalType.NULL);
  }

  @Test
  public void testConvertVariantValueFromPrimitiveString() {
    Variant variant = variantConverter().convertVariantValue("hello");
    assertThat(variant).isNotNull();
    assertThat(variant.metadata()).isNotNull();
    assertThat(variant.metadata().dictionarySize()).isEqualTo(0);
    assertThat(variant.value().type()).isEqualTo(PhysicalType.STRING);
    assertThat(variant.value().asPrimitive().get()).isEqualTo("hello");
  }

  @Test
  public void testConvertVariantValueFromPrimitiveNumber() {
    Variant variant = variantConverter().convertVariantValue(123);
    assertThat(variant).isNotNull();
    assertThat(variant.metadata().dictionarySize()).isEqualTo(0);
    assertThat(variant.value().type()).isEqualTo(PhysicalType.INT32);
    assertThat(variant.value().asPrimitive().get()).isEqualTo(123);
  }

  @Test
  public void testConvertVariantValueFromBoolean() {
    Variant variant = variantConverter().convertVariantValue(true);
    assertThat(variant).isNotNull();
    assertThat(variant.value().type()).isEqualTo(PhysicalType.BOOLEAN_TRUE);
    assertThat(variant.value().asPrimitive().get()).isEqualTo(true);
  }

  @Test
  public void testConvertVariantValueFromListWithMixedTypes() {
    // array with heterogeneous element types (string, int, boolean, double, null)
    // Note: java.util.Date is not supported in variant conversion; use supported types only.
    List<Object> input = Lists.newArrayList("a", 1, true, 2.5, null);
    Variant variant = variantConverter().convertVariantValue(input);

    assertThat(variant).isNotNull();
    assertThat(variant.value().type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(variant.value().asArray().numElements()).isEqualTo(5);

    assertThat(variant.value().asArray().get(0).type()).isEqualTo(PhysicalType.STRING);
    assertThat(variant.value().asArray().get(0).asPrimitive().get()).isEqualTo("a");

    assertThat(variant.value().asArray().get(1).type()).isEqualTo(PhysicalType.INT32);
    assertThat(variant.value().asArray().get(1).asPrimitive().get()).isEqualTo(1);

    assertThat(variant.value().asArray().get(2).type()).isEqualTo(PhysicalType.BOOLEAN_TRUE);
    assertThat(variant.value().asArray().get(2).asPrimitive().get()).isEqualTo(true);

    assertThat(variant.value().asArray().get(3).type()).isEqualTo(PhysicalType.DOUBLE);
    assertThat(variant.value().asArray().get(3).asPrimitive().get()).isEqualTo(2.5);

    assertThat(variant.value().asArray().get(4).type()).isEqualTo(PhysicalType.NULL);
  }

  @Test
  public void testConvertVariantValueFromMap() {
    Variant variant = variantConverter().convertVariantValue(ImmutableMap.of("hello", 1));
    assertThat(variant).isNotNull();
    assertThat(variant.metadata().dictionarySize()).isEqualTo(1);
    assertThat(variant.metadata().get(0)).isEqualTo("hello");
    assertThat(variant.value().type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(variant.value().asObject().numFields()).isEqualTo(1);
    assertThat(variant.value().asObject().get("hello").asPrimitive().get()).isEqualTo(1);
  }

  @Test
  public void testConvertVariantValueFromStructNested() {
    Schema innerSchema =
        SchemaBuilder.struct().field("x", Schema.INT32_SCHEMA).field("y", Schema.STRING_SCHEMA);
    Schema outerSchema =
        SchemaBuilder.struct().field("inner", innerSchema).field("id", Schema.INT64_SCHEMA);
    Struct inner = new Struct(innerSchema).put("x", 1).put("y", "world");
    Struct outer = new Struct(outerSchema).put("inner", inner).put("id", 100L);

    Variant variant = variantConverter().convertVariantValue(outer);

    assertThat(variant).isNotNull();
    assertThat(variant.metadata().dictionarySize()).isEqualTo(4);
    assertThat(variant.value().type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(variant.value().asObject().get("id").asPrimitive().get()).isEqualTo(100L);
    VariantValue innerVal = variant.value().asObject().get("inner");
    assertThat(innerVal.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(innerVal.asObject().get("x").asPrimitive().get()).isEqualTo(1);
    assertThat(innerVal.asObject().get("y").asPrimitive().get()).isEqualTo("world");
  }

  @Test
  public void testConvertVariantValueFromMapNested() {
    Variant variant =
        variantConverter()
            .convertVariantValue(ImmutableMap.of("hello", ImmutableMap.of("world", 1)));
    assertThat(variant).isNotNull();
    assertThat(variant.metadata().dictionarySize()).isEqualTo(2);
    assertThat(variant.value().type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(variant.value().asObject().get("hello").type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(variant.value().asObject().get("hello").asObject().get("world").asPrimitive().get())
        .isEqualTo(1);
  }

  @Test
  public void testConvertVariantValueFromMapWithList() {
    // map with list values: { "tags": ["a", "b"], "counts": [1, 2] }
    Map<String, Object> input =
        ImmutableMap.<String, Object>builder()
            .put("tags", ImmutableList.of("a", "b"))
            .put("counts", ImmutableList.of(1, 2))
            .build();
    Variant variant = variantConverter().convertVariantValue(input);

    assertThat(variant).isNotNull();
    assertThat(variant.value().type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(variant.value().asObject().numFields()).isEqualTo(2);

    VariantValue tags = variant.value().asObject().get("tags");
    assertThat(tags.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(tags.asArray().numElements()).isEqualTo(2);
    assertThat(tags.asArray().get(0).asPrimitive().get()).isEqualTo("a");
    assertThat(tags.asArray().get(1).asPrimitive().get()).isEqualTo("b");

    VariantValue counts = variant.value().asObject().get("counts");
    assertThat(counts.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(counts.asArray().get(0).asPrimitive().get()).isEqualTo(1);
    assertThat(counts.asArray().get(1).asPrimitive().get()).isEqualTo(2);
  }

  @Test
  public void testConvertVariantValueFromListOfMaps() {
    // list of maps: [ {"id": 1, "name": "x"}, {"id": 2} ]
    List<Map<String, Object>> input =
        ImmutableList.of(
            ImmutableMap.<String, Object>builder().put("id", 1).put("name", "x").build(),
            ImmutableMap.of("id", 2));
    Variant variant = variantConverter().convertVariantValue(input);

    assertThat(variant).isNotNull();
    assertThat(variant.value().type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(variant.value().asArray().numElements()).isEqualTo(2);

    VariantValue first = variant.value().asArray().get(0);
    assertThat(first.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(first.asObject().get("id").asPrimitive().get()).isEqualTo(1);
    assertThat(first.asObject().get("name").asPrimitive().get()).isEqualTo("x");

    VariantValue second = variant.value().asArray().get(1);
    assertThat(second.type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(second.asObject().get("id").asPrimitive().get()).isEqualTo(2);
  }

  @Test
  public void testConvertVariantValueFromMixedNested() {
    // map with list of maps, and list contains mixed types: { "items": [ {"k": "v"}, {"nested": [1,
    // 2]} ] }
    Map<String, Object> item0 = ImmutableMap.of("k", "v");
    Map<String, Object> item1 = ImmutableMap.of("nested", ImmutableList.of(1, 2));
    Map<String, Object> input = ImmutableMap.of("items", ImmutableList.of(item0, item1));

    Variant variant = variantConverter().convertVariantValue(input);

    assertThat(variant).isNotNull();
    assertThat(variant.value().type()).isEqualTo(PhysicalType.OBJECT);
    VariantValue items = variant.value().asObject().get("items");
    assertThat(items.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(items.asArray().numElements()).isEqualTo(2);

    assertThat(items.asArray().get(0).type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(items.asArray().get(0).asObject().get("k").asPrimitive().get()).isEqualTo("v");

    assertThat(items.asArray().get(1).type()).isEqualTo(PhysicalType.OBJECT);
    VariantValue nested = items.asArray().get(1).asObject().get("nested");
    assertThat(nested.type()).isEqualTo(PhysicalType.ARRAY);
    assertThat(nested.asArray().get(0).asPrimitive().get()).isEqualTo(1);
    assertThat(nested.asArray().get(1).asPrimitive().get()).isEqualTo(2);
  }

  @Test
  public void testConvertVariantValueFromStructWithTimestamp() {
    Schema structSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("created_at", Timestamp.SCHEMA)
            .build();
    // 2025-12-09T14:30:45.123Z
    long epochMillis = 1733751045123L;
    Struct struct = new Struct(structSchema).put("id", 42).put("created_at", new Date(epochMillis));

    Variant variant = variantConverter().convertVariantValue(struct);

    assertThat(variant).isNotNull();
    assertThat(variant.value().type()).isEqualTo(PhysicalType.OBJECT);
    assertThat(variant.value().asObject().get("id").asPrimitive().get()).isEqualTo(42);

    VariantValue createdAt = variant.value().asObject().get("created_at");
    assertThat(createdAt.type()).isEqualTo(PhysicalType.TIMESTAMPTZ);
    assertThat(createdAt.asPrimitive().get()).isEqualTo(epochMillis * 1000);
  }

  @Test
  public void testConvertVariantValueFromStructWithTime() {
    Schema structSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("start_time", Time.SCHEMA)
            .build();
    // 14:30:45 = 52245000 millis since midnight
    long millisSinceMidnight = 52245000L;
    Struct struct =
        new Struct(structSchema).put("id", 1).put("start_time", new Date(millisSinceMidnight));

    Variant variant = variantConverter().convertVariantValue(struct);

    assertThat(variant).isNotNull();
    assertThat(variant.value().type()).isEqualTo(PhysicalType.OBJECT);

    VariantValue startTime = variant.value().asObject().get("start_time");
    assertThat(startTime.type()).isEqualTo(PhysicalType.TIME);
    assertThat(startTime.asPrimitive().get()).isEqualTo(millisSinceMidnight * 1000);
  }

  @Test
  public void testConvertVariantValueFromStructWithDate() {
    Schema structSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("birth_date", org.apache.kafka.connect.data.Date.SCHEMA)
            .build();
    // 2025-12-09 = 20431 days since epoch
    long epochMillis = 20431L * 86_400_000;
    Struct struct = new Struct(structSchema).put("id", 1).put("birth_date", new Date(epochMillis));

    Variant variant = variantConverter().convertVariantValue(struct);

    assertThat(variant).isNotNull();
    assertThat(variant.value().type()).isEqualTo(PhysicalType.OBJECT);

    VariantValue birthDate = variant.value().asObject().get("birth_date");
    assertThat(birthDate.type()).isEqualTo(PhysicalType.DATE);
    assertThat(birthDate.asPrimitive().get()).isEqualTo(20431);
  }

  @Test
  public void testConvertVariantValueFromStructWithAllDateTypes() {
    Schema structSchema =
        SchemaBuilder.struct()
            .field("ts", Timestamp.SCHEMA)
            .field("t", Time.SCHEMA)
            .field("d", org.apache.kafka.connect.data.Date.SCHEMA)
            .build();
    long tsMillis = 1733751045123L;
    long timeMillis = 52245000L;
    long dateMillis = 20431L * 86_400_000;
    Struct struct =
        new Struct(structSchema)
            .put("ts", new Date(tsMillis))
            .put("t", new Date(timeMillis))
            .put("d", new Date(dateMillis));

    Variant variant = variantConverter().convertVariantValue(struct);

    assertThat(variant).isNotNull();
    assertThat(variant.value().asObject().get("ts").type()).isEqualTo(PhysicalType.TIMESTAMPTZ);
    assertThat(variant.value().asObject().get("ts").asPrimitive().get()).isEqualTo(tsMillis * 1000);

    assertThat(variant.value().asObject().get("t").type()).isEqualTo(PhysicalType.TIME);
    assertThat(variant.value().asObject().get("t").asPrimitive().get())
        .isEqualTo(timeMillis * 1000);

    assertThat(variant.value().asObject().get("d").type()).isEqualTo(PhysicalType.DATE);
    assertThat(variant.value().asObject().get("d").asPrimitive().get()).isEqualTo(20431);
  }

  public static Map<String, Object> createMapData() {
    return ImmutableMap.<String, Object>builder()
        .put("i", 1)
        .put("l", 2L)
        .put("d", DATE_VAL.toString())
        .put("t", TIME_VAL.toString())
        .put("ts", TS_VAL.toString())
        .put("tsz", TSZ_VAL.toString())
        .put("fl", 1.1f)
        .put("do", 2.2d)
        .put("dec", DEC_VAL.toString())
        .put("s", STR_VAL)
        .put("b", true)
        .put("u", UUID_VAL.toString())
        .put("f", Base64.getEncoder().encodeToString(BYTES_VAL.array()))
        .put("bi", Base64.getEncoder().encodeToString(BYTES_VAL.array()))
        .put("li", LIST_VAL)
        .put("ma", MAP_VAL)
        .build();
  }

  private Map<String, Object> createNestedMapData() {
    return ImmutableMap.<String, Object>builder().put("ii", 11).put("st", createMapData()).build();
  }

  private Struct createStructData() {
    return new Struct(CONNECT_SCHEMA)
        .put("i", 1)
        .put("l", 2L)
        .put("d", new Date(DATE_VAL.toEpochDay() * 24 * 60 * 60 * 1000L))
        .put("t", new Date(TIME_VAL.toNanoOfDay() / 1_000_000))
        .put("ts", Date.from(TS_VAL.atZone(ZoneOffset.UTC).toInstant()))
        .put("tsz", Date.from(TSZ_VAL.toInstant()))
        .put("fl", 1.1f)
        .put("do", 2.2d)
        .put("dec", DEC_VAL)
        .put("s", STR_VAL)
        .put("b", true)
        .put("u", UUID_VAL.toString())
        .put("f", BYTES_VAL)
        .put("bi", BYTES_VAL)
        .put("li", LIST_VAL)
        .put("ma", MAP_VAL);
  }

  private Struct createNestedStructData() {
    return new Struct(CONNECT_NESTED_SCHEMA).put("ii", 11).put("st", createStructData());
  }

  private void assertRecordValues(Record record) {
    GenericRecord rec = (GenericRecord) record;
    assertThat(rec.getField("i")).isEqualTo(1);
    assertThat(rec.getField("l")).isEqualTo(2L);
    assertThat(rec.getField("d")).isEqualTo(DATE_VAL);
    assertThat(rec.getField("t")).isEqualTo(TIME_VAL);
    assertThat(rec.getField("ts")).isEqualTo(TS_VAL);
    assertThat(rec.getField("tsz")).isEqualTo(TSZ_VAL);
    assertThat(rec.getField("fl")).isEqualTo(1.1f);
    assertThat(rec.getField("do")).isEqualTo(2.2d);
    assertThat(rec.getField("dec")).isEqualTo(DEC_VAL);
    assertThat(rec.getField("s")).isEqualTo(STR_VAL);
    assertThat(rec.getField("b")).isEqualTo(true);
    assertThat(rec.getField("f")).isEqualTo(BYTES_VAL.array());
    assertThat(rec.getField("bi")).isEqualTo(BYTES_VAL);
    assertThat(rec.getField("li")).isEqualTo(LIST_VAL);
    assertThat(rec.getField("ma")).isEqualTo(MAP_VAL);
    assertThat(rec.getField("u")).isEqualTo(UUID_VAL);
  }

  private void assertNestedRecordValues(Record record) {
    GenericRecord rec = (GenericRecord) record;
    assertThat(rec.getField("ii")).isEqualTo(11);
    assertRecordValues((GenericRecord) rec.getField("st"));
  }
}
