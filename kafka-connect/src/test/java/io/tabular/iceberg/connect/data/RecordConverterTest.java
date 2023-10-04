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
package io.tabular.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.data.SchemaUpdate.AddColumn;
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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.junit.jupiter.api.Test;

public class RecordConverterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final org.apache.iceberg.Schema SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(21, "i", Types.IntegerType.get()),
          Types.NestedField.required(22, "l", Types.LongType.get()),
          Types.NestedField.required(23, "d", Types.DateType.get()),
          Types.NestedField.required(24, "t", Types.TimeType.get()),
          Types.NestedField.required(25, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.required(26, "tsz", Types.TimestampType.withZone()),
          Types.NestedField.required(27, "fl", Types.FloatType.get()),
          Types.NestedField.required(28, "do", Types.DoubleType.get()),
          Types.NestedField.required(29, "dec", Types.DecimalType.of(9, 2)),
          Types.NestedField.required(30, "s", Types.StringType.get()),
          Types.NestedField.required(31, "u", Types.UUIDType.get()),
          Types.NestedField.required(32, "f", Types.FixedType.ofLength(3)),
          Types.NestedField.required(33, "b", Types.BinaryType.get()),
          Types.NestedField.required(
              34, "li", Types.ListType.ofRequired(35, Types.StringType.get())),
          Types.NestedField.required(
              36,
              "ma",
              Types.MapType.ofRequired(37, 38, Types.StringType.get(), Types.StringType.get())),
          Types.NestedField.optional(39, "extra", Types.StringType.get()));

  // we have 1 unmapped column so exclude that from the count
  private static final int MAPPED_CNT = SCHEMA.columns().size() - 1;

  private static final org.apache.iceberg.Schema NESTED_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "ii", Types.IntegerType.get()),
          Types.NestedField.required(2, "st", SCHEMA.asStruct()));

  private static final org.apache.iceberg.Schema SIMPLE_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "ii", Types.IntegerType.get()),
          Types.NestedField.required(2, "st", Types.StringType.get()));

  private static final org.apache.iceberg.Schema ID_SCHEMA =
      new org.apache.iceberg.Schema(Types.NestedField.required(1, "ii", Types.IntegerType.get()));

  private static final Schema CONNECT_SCHEMA =
      SchemaBuilder.struct()
          .field("i", Schema.INT32_SCHEMA)
          .field("l", Schema.INT64_SCHEMA)
          .field("d", Schema.STRING_SCHEMA)
          .field("t", Schema.STRING_SCHEMA)
          .field("ts", Schema.STRING_SCHEMA)
          .field("tsz", Schema.STRING_SCHEMA)
          .field("fl", Schema.FLOAT32_SCHEMA)
          .field("do", Schema.FLOAT64_SCHEMA)
          .field("dec", Schema.STRING_SCHEMA)
          .field("s", Schema.STRING_SCHEMA)
          .field("u", Schema.STRING_SCHEMA)
          .field("f", Schema.BYTES_SCHEMA)
          .field("b", Schema.BYTES_SCHEMA)
          .field("li", SchemaBuilder.array(Schema.STRING_SCHEMA))
          .field("ma", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA));

  private static final Schema CONNECT_NESTED_SCHEMA =
      SchemaBuilder.struct().field("ii", Schema.INT32_SCHEMA).field("st", CONNECT_SCHEMA);

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

  static {
    JSON_CONVERTER.configure(
        ImmutableMap.of(
            JsonConverterConfig.SCHEMAS_ENABLE_CONFIG,
            false,
            ConverterConfig.TYPE_CONFIG,
            ConverterType.VALUE.getName()));
  }

  @Test
  public void testMapConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    Map<String, Object> data = createMapData();
    Record record = converter.convert(data);
    assertRecordValues(record);
  }

  @Test
  public void testNestedMapConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(NESTED_SCHEMA);
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    Map<String, Object> nestedData = createNestedMapData();
    Record record = converter.convert(nestedData);
    assertNestedRecordValues(record);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapToString() throws Exception {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    Map<String, Object> nestedData = createNestedMapData();
    Record record = converter.convert(nestedData);

    String str = (String) record.getField("st");
    Map<String, Object> map = (Map<String, Object>) MAPPER.readValue(str, Map.class);
    assertThat(map).hasSize(MAPPED_CNT);
  }

  @Test
  public void testStructConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SCHEMA);
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    Struct data = createStructData();
    Record record = converter.convert(data);
    assertRecordValues(record);
  }

  @Test
  public void testNestedStructConvert() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(NESTED_SCHEMA);
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    Struct nestedData = createNestedStructData();
    Record record = converter.convert(nestedData);
    assertNestedRecordValues(record);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStructToString() throws Exception {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    Struct nestedData = createNestedStructData();
    Record record = converter.convert(nestedData);

    String str = (String) record.getField("st");
    Map<String, Object> map = (Map<String, Object>) MAPPER.readValue(str, Map.class);
    assertThat(map).hasSize(MAPPED_CNT);
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

    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    Map<String, Object> data = ImmutableMap.of("renamed_ii", 123);
    Record record = converter.convert(data);
    assertThat(record.getField("ii")).isEqualTo(123);
  }

  @Test
  public void testDecimalConversion() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

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
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

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
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

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
    convertToTimestamps(expected, expectedMillis, TimestampType.withZone());
  }

  @Test
  public void testTimestampWithoutZoneConversion() {
    LocalDateTime expected = LocalDateTime.parse("2023-05-18T11:22:33");
    long expectedMillis = expected.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
    convertToTimestamps(expected, expectedMillis, TimestampType.withoutZone());
  }

  private void convertToTimestamps(Temporal expected, long expectedMillis, TimestampType type) {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    List<Object> inputList =
        ImmutableList.of(
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
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    Map<String, Object> data = Maps.newHashMap(createMapData());
    data.put("null", null);

    List<AddColumn> addCols = Lists.newArrayList();
    converter.convert(data, addCols::add);

    assertThat(addCols).hasSize(15);

    Map<String, AddColumn> newColMap = Maps.newHashMap();
    addCols.forEach(addCol -> newColMap.put(addCol.name(), addCol));

    assertThat(newColMap.get("i").type()).isInstanceOf(LongType.class);
    assertThat(newColMap.get("l").type()).isInstanceOf(LongType.class);
    assertThat(newColMap.get("d").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("t").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("ts").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("tsz").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("fl").type()).isInstanceOf(DoubleType.class);
    assertThat(newColMap.get("do").type()).isInstanceOf(DoubleType.class);
    assertThat(newColMap.get("dec").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("s").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("u").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("f").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("b").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("li").type()).isInstanceOf(ListType.class);
    assertThat(newColMap.get("ma").type()).isInstanceOf(StructType.class);

    // null values should be ignored
    assertThat(newColMap).doesNotContainKey("null");
  }

  @Test
  public void testMissingColumnDetectionMapNested() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(ID_SCHEMA);
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    Map<String, Object> nestedData = createNestedMapData();
    List<AddColumn> addCols = Lists.newArrayList();
    converter.convert(nestedData, addCols::add);

    assertThat(addCols).hasSize(1);

    assertThat(addCols).hasSize(1);

    AddColumn addCol = addCols.get(0);
    assertThat(addCol.name()).isEqualTo("st");

    StructType addedType = addCol.type().asStructType();
    assertThat(addedType.fields()).hasSize(15);
    assertThat(addedType.field("i").type()).isInstanceOf(LongType.class);
    assertThat(addedType.field("l").type()).isInstanceOf(LongType.class);
    assertThat(addedType.field("d").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("t").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("ts").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("tsz").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("fl").type()).isInstanceOf(DoubleType.class);
    assertThat(addedType.field("do").type()).isInstanceOf(DoubleType.class);
    assertThat(addedType.field("dec").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("s").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("u").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("f").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("b").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("li").type()).isInstanceOf(ListType.class);
    assertThat(addedType.field("ma").type()).isInstanceOf(StructType.class);
  }

  @Test
  public void testMissingColumnDetectionStruct() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(ID_SCHEMA);
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    Struct data = createStructData();
    List<AddColumn> addCols = Lists.newArrayList();
    converter.convert(data, addCols::add);

    assertThat(addCols).hasSize(15);

    Map<String, AddColumn> newColMap = Maps.newHashMap();
    addCols.forEach(addCol -> newColMap.put(addCol.name(), addCol));

    assertThat(newColMap.get("i").type()).isInstanceOf(IntegerType.class);
    assertThat(newColMap.get("l").type()).isInstanceOf(LongType.class);
    assertThat(newColMap.get("d").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("t").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("ts").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("tsz").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("fl").type()).isInstanceOf(FloatType.class);
    assertThat(newColMap.get("do").type()).isInstanceOf(DoubleType.class);
    assertThat(newColMap.get("dec").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("s").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("u").type()).isInstanceOf(StringType.class);
    assertThat(newColMap.get("f").type()).isInstanceOf(BinaryType.class);
    assertThat(newColMap.get("b").type()).isInstanceOf(BinaryType.class);
    assertThat(newColMap.get("li").type()).isInstanceOf(ListType.class);
    assertThat(newColMap.get("ma").type()).isInstanceOf(MapType.class);
  }

  @Test
  public void testMissingColumnDetectionStructNested() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(ID_SCHEMA);
    RecordConverter converter = new RecordConverter(table, JSON_CONVERTER);

    Struct nestedData = createNestedStructData();
    List<AddColumn> addCols = Lists.newArrayList();
    converter.convert(nestedData, addCols::add);

    assertThat(addCols).hasSize(1);

    AddColumn addCol = addCols.get(0);
    assertThat(addCol.name()).isEqualTo("st");

    StructType addedType = addCol.type().asStructType();
    assertThat(addedType.fields()).hasSize(15);
    assertThat(addedType.field("i").type()).isInstanceOf(IntegerType.class);
    assertThat(addedType.field("l").type()).isInstanceOf(LongType.class);
    assertThat(addedType.field("d").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("t").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("ts").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("tsz").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("fl").type()).isInstanceOf(FloatType.class);
    assertThat(addedType.field("do").type()).isInstanceOf(DoubleType.class);
    assertThat(addedType.field("dec").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("s").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("u").type()).isInstanceOf(StringType.class);
    assertThat(addedType.field("f").type()).isInstanceOf(BinaryType.class);
    assertThat(addedType.field("b").type()).isInstanceOf(BinaryType.class);
    assertThat(addedType.field("li").type()).isInstanceOf(ListType.class);
    assertThat(addedType.field("ma").type()).isInstanceOf(MapType.class);
  }

  private Map<String, Object> createMapData() {
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
        .put("u", UUID_VAL.toString())
        .put("f", Base64.getEncoder().encodeToString(BYTES_VAL.array()))
        .put("b", Base64.getEncoder().encodeToString(BYTES_VAL.array()))
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
        .put("d", DATE_VAL.toString())
        .put("t", TIME_VAL.toString())
        .put("ts", TS_VAL.toString())
        .put("tsz", TSZ_VAL.toString())
        .put("fl", 1.1f)
        .put("do", 2.2d)
        .put("dec", DEC_VAL.toString())
        .put("s", STR_VAL)
        .put("u", UUID_VAL.toString())
        .put("f", BYTES_VAL.array())
        .put("b", BYTES_VAL.array())
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
    assertThat(rec.getField("u")).isEqualTo(UUID_VAL);
    assertThat(rec.getField("f")).isEqualTo(BYTES_VAL);
    assertThat(rec.getField("b")).isEqualTo(BYTES_VAL);
    assertThat(rec.getField("li")).isEqualTo(LIST_VAL);
    assertThat(rec.getField("ma")).isEqualTo(MAP_VAL);
  }

  private void assertNestedRecordValues(Record record) {
    GenericRecord rec = (GenericRecord) record;
    assertThat(rec.getField("ii")).isEqualTo(11);
    assertRecordValues((GenericRecord) rec.getField("st"));
  }
}
