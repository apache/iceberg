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
package org.apache.iceberg.spark.data;

import static org.apache.iceberg.spark.SparkSchemaUtil.convert;
import static scala.collection.JavaConverters.mapAsJavaMapConverter;
import static scala.collection.JavaConverters.seqAsJavaListConverter;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import scala.collection.Seq;

public class GenericsHelpers {
  private GenericsHelpers() {}

  private static final OffsetDateTime EPOCH = Instant.ofEpochMilli(0L).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  public static void assertEqualsSafe(Types.StructType struct, Record expected, Row actual) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Type fieldType = fields.get(i).type();

      Object expectedValue = expected.get(i);
      Object actualValue = actual.get(i);

      assertEqualsSafe(fieldType, expectedValue, actualValue);
    }
  }

  private static void assertEqualsSafe(
      Types.ListType list, Collection<?> expected, List<?> actual) {
    Type elementType = list.elementType();
    List<?> expectedElements = Lists.newArrayList(expected);
    for (int i = 0; i < expectedElements.size(); i += 1) {
      Object expectedValue = expectedElements.get(i);
      Object actualValue = actual.get(i);

      assertEqualsSafe(elementType, expectedValue, actualValue);
    }
  }

  private static void assertEqualsSafe(Types.MapType map, Map<?, ?> expected, Map<?, ?> actual) {
    Type keyType = map.keyType();
    Type valueType = map.valueType();
    Assert.assertEquals(
        "Should have the same number of keys", expected.keySet().size(), actual.keySet().size());

    for (Object expectedKey : expected.keySet()) {
      Object matchingKey = null;
      for (Object actualKey : actual.keySet()) {
        try {
          assertEqualsSafe(keyType, expectedKey, actualKey);
          matchingKey = actualKey;
          break;
        } catch (AssertionError e) {
          // failed
        }
      }

      Assert.assertNotNull("Should have a matching key", matchingKey);
      assertEqualsSafe(valueType, expected.get(expectedKey), actual.get(matchingKey));
    }
  }

  @SuppressWarnings("unchecked")
  private static void assertEqualsSafe(Type type, Object expected, Object actual) {
    if (expected == null && actual == null) {
      return;
    }

    switch (type.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
        Assert.assertEquals("Primitive value should be equal to expected", expected, actual);
        break;
      case DATE:
        Assertions.assertThat(expected)
            .as("Should expect a LocalDate")
            .isInstanceOf(LocalDate.class);
        Assertions.assertThat(actual).as("Should be a Date").isInstanceOf(Date.class);
        Assert.assertEquals(
            "ISO-8601 date should be equal", expected.toString(), actual.toString());
        break;
      case TIMESTAMP:
        Assertions.assertThat(actual).as("Should be a Timestamp").isInstanceOf(Timestamp.class);
        Timestamp ts = (Timestamp) actual;
        // milliseconds from nanos has already been added by getTime
        OffsetDateTime actualTs =
            EPOCH.plusNanos((ts.getTime() * 1_000_000) + (ts.getNanos() % 1_000_000));
        Types.TimestampType timestampType = (Types.TimestampType) type;
        if (timestampType.shouldAdjustToUTC()) {
          Assertions.assertThat(expected)
              .as("Should expect an OffsetDateTime")
              .isInstanceOf(OffsetDateTime.class);
          Assert.assertEquals("Timestamp should be equal", expected, actualTs);
        } else {
          Assertions.assertThat(expected)
              .as("Should expect an LocalDateTime")
              .isInstanceOf(LocalDateTime.class);
          Assert.assertEquals("Timestamp should be equal", expected, actualTs.toLocalDateTime());
        }
        break;
      case STRING:
        Assertions.assertThat(actual).as("Should be a String").isInstanceOf(String.class);
        Assert.assertEquals("Strings should be equal", String.valueOf(expected), actual);
        break;
      case UUID:
        Assertions.assertThat(expected).as("Should expect a UUID").isInstanceOf(UUID.class);
        Assertions.assertThat(actual).as("Should be a String").isInstanceOf(String.class);
        Assert.assertEquals("UUID string representation should match", expected.toString(), actual);
        break;
      case FIXED:
        Assertions.assertThat(expected).as("Should expect a byte[]").isInstanceOf(byte[].class);
        Assertions.assertThat(actual).as("Should be a byte[]").isInstanceOf(byte[].class);
        Assert.assertArrayEquals("Bytes should match", (byte[]) expected, (byte[]) actual);
        break;
      case BINARY:
        Assertions.assertThat(expected)
            .as("Should expect a ByteBuffer")
            .isInstanceOf(ByteBuffer.class);
        Assertions.assertThat(actual).as("Should be a byte[]").isInstanceOf(byte[].class);
        Assert.assertArrayEquals(
            "Bytes should match", ((ByteBuffer) expected).array(), (byte[]) actual);
        break;
      case DECIMAL:
        Assertions.assertThat(expected)
            .as("Should expect a BigDecimal")
            .isInstanceOf(BigDecimal.class);
        Assertions.assertThat(actual).as("Should be a BigDecimal").isInstanceOf(BigDecimal.class);
        Assert.assertEquals("BigDecimals should be equal", expected, actual);
        break;
      case STRUCT:
        Assertions.assertThat(expected).as("Should expect a Record").isInstanceOf(Record.class);
        Assertions.assertThat(actual).as("Should be a Row").isInstanceOf(Row.class);
        assertEqualsSafe(type.asNestedType().asStructType(), (Record) expected, (Row) actual);
        break;
      case LIST:
        Assertions.assertThat(expected)
            .as("Should expect a Collection")
            .isInstanceOf(Collection.class);
        Assertions.assertThat(actual).as("Should be a Seq").isInstanceOf(Seq.class);
        List<?> asList = seqAsJavaListConverter((Seq<?>) actual).asJava();
        assertEqualsSafe(type.asNestedType().asListType(), (Collection<?>) expected, asList);
        break;
      case MAP:
        Assertions.assertThat(expected).as("Should expect a Collection").isInstanceOf(Map.class);
        Assertions.assertThat(actual)
            .as("Should be a Map")
            .isInstanceOf(scala.collection.Map.class);
        Map<String, ?> asMap =
            mapAsJavaMapConverter((scala.collection.Map<String, ?>) actual).asJava();
        assertEqualsSafe(type.asNestedType().asMapType(), (Map<?, ?>) expected, asMap);
        break;
      case TIME:
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }

  public static void assertEqualsUnsafe(
      Types.StructType struct, Record expected, InternalRow actual) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Type fieldType = fields.get(i).type();

      Object expectedValue = expected.get(i);
      Object actualValue = actual.get(i, convert(fieldType));

      assertEqualsUnsafe(fieldType, expectedValue, actualValue);
    }
  }

  private static void assertEqualsUnsafe(
      Types.ListType list, Collection<?> expected, ArrayData actual) {
    Type elementType = list.elementType();
    List<?> expectedElements = Lists.newArrayList(expected);
    for (int i = 0; i < expectedElements.size(); i += 1) {
      Object expectedValue = expectedElements.get(i);
      Object actualValue = actual.get(i, convert(elementType));

      assertEqualsUnsafe(elementType, expectedValue, actualValue);
    }
  }

  private static void assertEqualsUnsafe(Types.MapType map, Map<?, ?> expected, MapData actual) {
    Type keyType = map.keyType();
    Type valueType = map.valueType();

    List<Map.Entry<?, ?>> expectedElements = Lists.newArrayList(expected.entrySet());
    ArrayData actualKeys = actual.keyArray();
    ArrayData actualValues = actual.valueArray();

    for (int i = 0; i < expectedElements.size(); i += 1) {
      Map.Entry<?, ?> expectedPair = expectedElements.get(i);
      Object actualKey = actualKeys.get(i, convert(keyType));
      Object actualValue = actualValues.get(i, convert(keyType));

      assertEqualsUnsafe(keyType, expectedPair.getKey(), actualKey);
      assertEqualsUnsafe(valueType, expectedPair.getValue(), actualValue);
    }
  }

  private static void assertEqualsUnsafe(Type type, Object expected, Object actual) {
    if (expected == null && actual == null) {
      return;
    }

    switch (type.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
        Assert.assertEquals("Primitive value should be equal to expected", expected, actual);
        break;
      case DATE:
        Assertions.assertThat(expected)
            .as("Should expect a LocalDate")
            .isInstanceOf(LocalDate.class);
        int expectedDays = (int) ChronoUnit.DAYS.between(EPOCH_DAY, (LocalDate) expected);
        Assert.assertEquals("Primitive value should be equal to expected", expectedDays, actual);
        break;
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) type;
        if (timestampType.shouldAdjustToUTC()) {
          Assertions.assertThat(expected)
              .as("Should expect an OffsetDateTime")
              .isInstanceOf(OffsetDateTime.class);
          long expectedMicros = ChronoUnit.MICROS.between(EPOCH, (OffsetDateTime) expected);
          Assert.assertEquals(
              "Primitive value should be equal to expected", expectedMicros, actual);
        } else {
          Assertions.assertThat(expected)
              .as("Should expect an LocalDateTime")
              .isInstanceOf(LocalDateTime.class);
          long expectedMicros =
              ChronoUnit.MICROS.between(EPOCH, ((LocalDateTime) expected).atZone(ZoneId.of("UTC")));
          Assert.assertEquals(
              "Primitive value should be equal to expected", expectedMicros, actual);
        }
        break;
      case STRING:
        Assertions.assertThat(actual).as("Should be a UTF8String").isInstanceOf(UTF8String.class);
        Assert.assertEquals("Strings should be equal", expected, actual.toString());
        break;
      case UUID:
        Assertions.assertThat(expected).as("Should expect a UUID").isInstanceOf(UUID.class);
        Assertions.assertThat(actual).as("Should be a UTF8String").isInstanceOf(UTF8String.class);
        Assert.assertEquals(
            "UUID string representation should match", expected.toString(), actual.toString());
        break;
      case FIXED:
        Assertions.assertThat(expected).as("Should expect a byte[]").isInstanceOf(byte[].class);
        Assertions.assertThat(actual).as("Should be a byte[]").isInstanceOf(byte[].class);
        Assert.assertArrayEquals("Bytes should match", (byte[]) expected, (byte[]) actual);
        break;
      case BINARY:
        Assertions.assertThat(expected)
            .as("Should expect a ByteBuffer")
            .isInstanceOf(ByteBuffer.class);
        Assertions.assertThat(actual).as("Should be a byte[]").isInstanceOf(byte[].class);
        Assert.assertArrayEquals(
            "Bytes should match", ((ByteBuffer) expected).array(), (byte[]) actual);
        break;
      case DECIMAL:
        Assertions.assertThat(expected)
            .as("Should expect a BigDecimal")
            .isInstanceOf(BigDecimal.class);
        Assertions.assertThat(actual).as("Should be a Decimal").isInstanceOf(Decimal.class);
        Assert.assertEquals(
            "BigDecimals should be equal", expected, ((Decimal) actual).toJavaBigDecimal());
        break;
      case STRUCT:
        Assertions.assertThat(expected).as("Should expect a Record").isInstanceOf(Record.class);
        Assertions.assertThat(actual)
            .as("Should be an InternalRow")
            .isInstanceOf(InternalRow.class);
        assertEqualsUnsafe(
            type.asNestedType().asStructType(), (Record) expected, (InternalRow) actual);
        break;
      case LIST:
        Assertions.assertThat(expected)
            .as("Should expect a Collection")
            .isInstanceOf(Collection.class);
        Assertions.assertThat(actual).as("Should be an ArrayData").isInstanceOf(ArrayData.class);
        assertEqualsUnsafe(
            type.asNestedType().asListType(), (Collection<?>) expected, (ArrayData) actual);
        break;
      case MAP:
        Assertions.assertThat(expected).as("Should expect a Map").isInstanceOf(Map.class);
        Assertions.assertThat(actual)
            .as("Should be an ArrayBasedMapData")
            .isInstanceOf(MapData.class);
        assertEqualsUnsafe(type.asNestedType().asMapType(), (Map<?, ?>) expected, (MapData) actual);
        break;
      case TIME:
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }
}
