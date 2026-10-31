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
package org.apache.iceberg.flink.maintenance.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestRecordSerializer {

  private static final Types.StructType KEY_TYPE =
      Types.StructType.of(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final LocalDate DATE = LocalDate.of(2026, 10, 31);
  private static final LocalTime TIME = LocalTime.of(12, 34, 56, 789_012_000);
  private static final LocalDateTime TIMESTAMP = LocalDateTime.of(DATE, TIME);
  private static final LocalDateTime TIMESTAMP_NANO =
      LocalDateTime.of(DATE, LocalTime.of(1, 2, 3, 456_789_123));
  private static final OffsetDateTime TIMESTAMPTZ =
      OffsetDateTime.of(TIMESTAMP, ZoneOffset.ofHours(2));
  private static final OffsetDateTime TIMESTAMPTZ_NANO =
      OffsetDateTime.of(TIMESTAMP_NANO, ZoneOffset.ofHours(-5));
  private static final UUID UUID_VALUE = UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7");
  private static final byte[] FIXED = new byte[] {1, 2, 3, 4};
  private static final byte[] BINARY = new byte[] {5, 6, 7};

  /** Iceberg type, generic {@link Record} value, and the equivalent internal value. */
  private static Stream<Arguments> primitiveTypes() {
    return Stream.of(
        Arguments.of(Types.BooleanType.get(), true, true),
        Arguments.of(Types.IntegerType.get(), 42, 42),
        Arguments.of(Types.LongType.get(), 42L, 42L),
        Arguments.of(Types.FloatType.get(), 1.5f, 1.5f),
        Arguments.of(Types.DoubleType.get(), 2.5d, 2.5d),
        Arguments.of(Types.DateType.get(), DATE, (int) DATE.toEpochDay()),
        Arguments.of(Types.TimeType.get(), TIME, TIME.toNanoOfDay() / 1000),
        Arguments.of(
            Types.TimestampType.withoutZone(),
            TIMESTAMP,
            DateTimeUtil.microsFromTimestamp(TIMESTAMP)),
        Arguments.of(
            Types.TimestampType.withZone(),
            TIMESTAMPTZ,
            DateTimeUtil.microsFromTimestamptz(TIMESTAMPTZ)),
        Arguments.of(
            Types.TimestampNanoType.withoutZone(),
            TIMESTAMP_NANO,
            DateTimeUtil.nanosFromTimestamp(TIMESTAMP_NANO)),
        Arguments.of(
            Types.TimestampNanoType.withZone(),
            TIMESTAMPTZ_NANO,
            DateTimeUtil.nanosFromTimestamptz(TIMESTAMPTZ_NANO)),
        Arguments.of(Types.StringType.get(), "hello", "hello"),
        Arguments.of(Types.UUIDType.get(), UUID_VALUE, UUID_VALUE),
        Arguments.of(Types.FixedType.ofLength(FIXED.length), FIXED, ByteBuffer.wrap(FIXED)),
        Arguments.of(Types.BinaryType.get(), ByteBuffer.wrap(BINARY), ByteBuffer.wrap(BINARY)),
        Arguments.of(
            Types.DecimalType.of(10, 2), new BigDecimal("123.45"), new BigDecimal("123.45")));
  }

  @ParameterizedTest
  @MethodSource("primitiveTypes")
  void serializeKeyMatchesInternalEncoding(Type type, Object value, Object internalValue) {
    Types.StructType keyType = Types.StructType.of(Types.NestedField.optional(7, "key", type));
    Record key = GenericRecord.create(keyType);
    key.set(0, value);

    assertThat(new RecordSerializer(keyType).serializeKey(key).data())
        .isEqualTo(expectedBytes(keyType, internalValue));
  }

  @ParameterizedTest
  @MethodSource("primitiveTypes")
  void serializeKeyIsEqualForEquivalentRecords(Type type, Object value, Object internalValue) {
    Types.StructType keyType = Types.StructType.of(Types.NestedField.required(7, "key", type));
    RecordSerializer serializer = new RecordSerializer(keyType);
    Record key1 = GenericRecord.create(keyType);
    key1.set(0, value);
    Record key2 = key1.copy();

    assertThat(serializer.serializeKey(key1)).isEqualTo(serializer.serializeKey(key2));
  }

  @ParameterizedTest
  @MethodSource("primitiveTypes")
  void serializeKeyHandlesNulls(Type type, Object value, Object internalValue) {
    Types.StructType keyType = Types.StructType.of(Types.NestedField.optional(7, "key", type));
    RecordSerializer serializer = new RecordSerializer(keyType);
    Record withNull = GenericRecord.create(keyType);
    Record withValue = GenericRecord.create(keyType);
    withValue.set(0, value);

    assertThat(serializer.serializeKey(withNull).data())
        .isEqualTo(expectedBytes(keyType, (Object) null));
    assertThat(serializer.serializeKey(withNull)).isNotEqualTo(serializer.serializeKey(withValue));
  }

  @Test
  void serializeKeyMatchesInternalEncodingForAllTypesInOneKey() {
    List<Arguments> types = primitiveTypes().toList();
    List<Types.NestedField> fields = Lists.newArrayList();
    for (int i = 0; i < types.size(); i++) {
      fields.add(Types.NestedField.optional(100 + i, "f" + i, (Type) types.get(i).get()[0]));
    }

    Types.StructType keyType = Types.StructType.of(fields);
    Record key = GenericRecord.create(keyType);
    Object[] internalValues = new Object[types.size()];
    for (int i = 0; i < types.size(); i++) {
      key.set(i, types.get(i).get()[1]);
      internalValues[i] = types.get(i).get()[2];
    }

    RecordSerializer serializer = new RecordSerializer(keyType);
    byte[] expected = expectedBytes(keyType, internalValues);
    assertThat(serializer.serializeKey(key).data()).isEqualTo(expected);
    // The serializer reuses its buffer and wrapper across calls.
    assertThat(serializer.serializeKey(key).data()).isEqualTo(expected);
  }

  @Test
  void serializeKeyIgnoresTrailingFields() {
    Types.StructType dataType =
        Types.StructType.of(
            KEY_TYPE.fields().get(0), KEY_TYPE.fields().get(1), MetadataColumns.ROW_POSITION);
    Record dataRecord = GenericRecord.create(dataType);
    dataRecord.set(0, 1);
    dataRecord.set(1, "a");
    dataRecord.set(2, 99L);

    Record deleteRecord = GenericRecord.create(KEY_TYPE);
    deleteRecord.set(0, 1);
    deleteRecord.set(1, "a");

    RecordSerializer serializer = new RecordSerializer(KEY_TYPE);
    assertThat(serializer.serializeKey(dataRecord))
        .isEqualTo(serializer.serializeKey(deleteRecord));
  }

  @Test
  void serializeKeyDiffersForDifferentKeys() {
    Record record1 = GenericRecord.create(KEY_TYPE);
    record1.set(0, 1);
    record1.set(1, "a");

    Record record2 = GenericRecord.create(KEY_TYPE);
    record2.set(0, 2);
    record2.set(1, "b");

    RecordSerializer serializer = new RecordSerializer(KEY_TYPE);
    assertThat(serializer.serializeKey(record1)).isNotEqualTo(serializer.serializeKey(record2));
  }

  @Test
  void nullDiffersFromEmptyString() {
    Record withNull = GenericRecord.create(KEY_TYPE);
    withNull.set(0, 1);
    withNull.set(1, null);

    Record withValue = GenericRecord.create(KEY_TYPE);
    withValue.set(0, 1);
    withValue.set(1, "");

    RecordSerializer serializer = new RecordSerializer(KEY_TYPE);
    assertThat(serializer.serializeKey(withNull)).isNotEqualTo(serializer.serializeKey(withValue));
  }

  @Test
  void sameValuesDifferentFieldSetsAreNotEqual() {
    Types.StructType typeWithField1 =
        Types.StructType.of(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    Types.StructType typeWithField3 =
        Types.StructType.of(Types.NestedField.required(3, "other", Types.IntegerType.get()));

    Record record1 = GenericRecord.create(typeWithField1);
    record1.set(0, 42);

    Record record3 = GenericRecord.create(typeWithField3);
    record3.set(0, 42);

    assertThat(new RecordSerializer(typeWithField1).serializeKey(record1))
        .isNotEqualTo(new RecordSerializer(typeWithField3).serializeKey(record3));
  }

  @Test
  void serializeKeyFlattensNestedFields() {
    Types.StructType userType =
        Types.StructType.of(
            Types.NestedField.required(3, "id", Types.IntegerType.get()),
            Types.NestedField.optional(4, "birthday", Types.DateType.get()));
    Types.StructType keyType =
        Types.StructType.of(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", userType));
    Record user = GenericRecord.create(userType);
    user.set(0, 10);
    user.set(1, DATE);
    Record key = GenericRecord.create(keyType);
    key.set(0, 1);
    key.set(1, user);

    // Leaves id, user.id, user.birthday in depth-first order; nested values are internal too.
    Types.StructType flatType =
        Types.StructType.of(keyType.fields().get(0), userType.fields().get(0), userType.field(4));
    assertThat(new RecordSerializer(keyType).serializeKey(key).data())
        .isEqualTo(expectedBytes(flatType, 1, 10, (int) DATE.toEpochDay()));
  }

  @Test
  void serializeKeyWritesNullLeavesForNullParentStruct() {
    Types.StructType userType =
        Types.StructType.of(Types.NestedField.required(3, "id", Types.IntegerType.get()));
    Types.StructType keyType =
        Types.StructType.of(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "user", userType));
    Record key = GenericRecord.create(keyType);
    key.set(0, 1);

    Types.StructType flatType = Types.StructType.of(keyType.fields().get(0), userType.field(3));
    assertThat(new RecordSerializer(keyType).serializeKey(key).data())
        .isEqualTo(expectedBytes(flatType, 1, null));
  }

  /**
   * Reference implementation of the keyed-state key format: field count, field ids, then per field
   * a null marker and the length-prefixed {@link Conversions} bytes of the internal value. Changing
   * the format breaks restored Flink state.
   */
  private static byte[] expectedBytes(Types.StructType keyType, Object... internalValues) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      List<Types.NestedField> fields = keyType.fields();
      dos.writeInt(fields.size());
      for (Types.NestedField field : fields) {
        dos.writeInt(field.fieldId());
      }

      for (int i = 0; i < fields.size(); i++) {
        Object value = internalValues[i];
        if (value == null) {
          dos.writeBoolean(true);
        } else {
          dos.writeBoolean(false);
          ByteBuffer buf = Conversions.toByteBuffer(fields.get(i).type(), value);
          byte[] bytes = new byte[buf.remaining()];
          buf.duplicate().get(bytes);
          dos.writeInt(bytes.length);
          dos.write(bytes);
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    return baos.toByteArray();
  }
}
