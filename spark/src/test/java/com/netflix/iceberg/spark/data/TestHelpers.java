/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.spark.data;

import com.google.common.collect.Lists;
import com.netflix.iceberg.spark.SparkSchemaUtil;
import com.netflix.iceberg.types.Comparators;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import scala.collection.Seq;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;
import static scala.collection.JavaConverters.seqAsJavaListConverter;

public class TestHelpers {

  public static void assertEqualsSafe(Types.StructType struct, Record rec, Row row) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Type fieldType = fields.get(i).type();

      Object expectedValue = rec.get(i);
      Object actualValue = row.get(i);

      assertEqualsSafe(fieldType, expectedValue, actualValue);
    }
  }

  private static void assertEqualsSafe(Types.ListType list, Collection<?> expected, List actual) {
    Type elementType = list.elementType();
    List<?> expectedElements = Lists.newArrayList(expected);
    for (int i = 0; i < expectedElements.size(); i += 1) {
      Object expectedValue = expectedElements.get(i);
      Object actualValue = actual.get(i);

      assertEqualsSafe(elementType, expectedValue, actualValue);
    }
  }

  private static void assertEqualsSafe(Types.MapType map,
                                       Map<String, ?> expected, Map<String, ?> actual) {
    Type keyType = map.keyType();
    Type valueType = map.valueType();

    List<String> expectedKeys = Lists.newArrayList(expected.keySet());
    List<String> actualKeys = Lists.newArrayList(actual.keySet());

    expectedKeys.sort(Comparators.charSequences());
    actualKeys.sort(Comparators.charSequences());

    for (int i = 0; i < expectedKeys.size(); i += 1) {
      String expectedKey = expectedKeys.get(i);
      String actualKey = actualKeys.get(i);

      assertEqualsSafe(keyType, expectedKey, actualKey);
      assertEqualsSafe(valueType, expected.get(expectedKey), actual.get(actualKey));
    }
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochMilli(0L).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

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
        Assert.assertTrue("Should be an int", expected instanceof Integer);
        Assert.assertTrue("Should be a Date", actual instanceof Date);
        int daysFrom1970_01_01 = (Integer) expected;
        LocalDate date = ChronoUnit.DAYS.addTo(EPOCH_DAY, daysFrom1970_01_01);
        Assert.assertEquals("ISO-8601 date should be equal", date.toString(), actual.toString());
        break;
      case TIMESTAMP:
        Assert.assertTrue("Should be a long", expected instanceof Long);
        Assert.assertTrue("Should be a Timestamp", actual instanceof Timestamp);
        Timestamp ts = (Timestamp) actual;
        // milliseconds from nanos has already been added by getTime
        long tsMicros = (ts.getTime() * 1000) + ((ts.getNanos() / 1000) % 1000);
        Assert.assertEquals("Timestamp micros should be equal", expected, tsMicros);
        break;
      case STRING:
        Assert.assertTrue("Should be a String", actual instanceof String);
        Assert.assertEquals("Strings should be equal", expected, actual);
        break;
      case UUID:
        Assert.assertTrue("Should expect a UUID", expected instanceof UUID);
        Assert.assertTrue("Should be a String", actual instanceof String);
        Assert.assertEquals("UUID string representation should match",
            expected.toString(), actual);
        break;
      case FIXED:
        Assert.assertTrue("Should expect a Fixed", expected instanceof GenericData.Fixed);
        Assert.assertTrue("Should be a byte[]", actual instanceof byte[]);
        Assert.assertArrayEquals("Bytes should match",
            ((GenericData.Fixed) expected).bytes(), (byte[]) actual);
        break;
      case BINARY:
        Assert.assertTrue("Should expect a ByteBuffer", expected instanceof ByteBuffer);
        Assert.assertTrue("Should be a byte[]", actual instanceof byte[]);
        Assert.assertArrayEquals("Bytes should match",
            ((ByteBuffer) expected).array(), (byte[]) actual);
        break;
      case DECIMAL:
        Assert.assertTrue("Should expect a BigDecimal", expected instanceof BigDecimal);
        Assert.assertTrue("Should be a BigDecimal", actual instanceof BigDecimal);
        Assert.assertEquals("BigDecimals should be equal", expected, actual);
        break;
      case STRUCT:
        Assert.assertTrue("Should expect a Record", expected instanceof Record);
        Assert.assertTrue("Should be a Row", actual instanceof Row);
        assertEqualsSafe(type.asNestedType().asStructType(), (Record) expected, (Row) actual);
        break;
      case LIST:
        Assert.assertTrue("Should expect a Collection", expected instanceof Collection);
        Assert.assertTrue("Should be a Seq", actual instanceof Seq);
        List<?> asList = seqAsJavaListConverter((Seq<?>) actual).asJava();
        assertEqualsSafe(type.asNestedType().asListType(), (Collection) expected, asList);
        break;
      case MAP:
        Assert.assertTrue("Should expect a Collection", expected instanceof Map);
        Assert.assertTrue("Should be a Map", actual instanceof scala.collection.Map);
        Map<String, ?> asMap = mapAsJavaMapConverter(
            (scala.collection.Map<String, ?>) actual).asJava();
        assertEqualsSafe(type.asNestedType().asMapType(), (Map<String, ?>) expected, asMap);
        break;
      case TIME:
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }

  public static void assertEqualsUnsafe(Types.StructType struct, Record rec, InternalRow row) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Type fieldType = fields.get(i).type();

      Object expectedValue = rec.get(i);
      Object actualValue = row.get(i, SparkSchemaUtil.convert(fieldType));

      assertEqualsUnsafe(fieldType, expectedValue, actualValue);
    }
  }

  private static void assertEqualsUnsafe(Types.ListType list, Collection<?> expected, ArrayData actual) {
    Type elementType = list.elementType();
    List<?> expectedElements = Lists.newArrayList(expected);
    for (int i = 0; i < expectedElements.size(); i += 1) {
      Object expectedValue = expectedElements.get(i);
      Object actualValue = actual.get(i, SparkSchemaUtil.convert(elementType));

      assertEqualsUnsafe(elementType, expectedValue, actualValue);
    }
  }

  private static void assertEqualsUnsafe(Types.MapType map, Map<?, ?> expected, ArrayBasedMapData actual) {
    Type keyType = map.keyType();
    Type valueType = map.valueType();

    List<Map.Entry<?, ?>> expectedElements = Lists.newArrayList(expected.entrySet());
    ArrayData actualKeys = actual.keyArray();
    ArrayData actualValues = actual.valueArray();

    for (int i = 0; i < expectedElements.size(); i += 1) {
      Map.Entry<?, ?> expectedPair = expectedElements.get(i);
      Object actualKey = actualKeys.get(i, SparkSchemaUtil.convert(keyType));
      Object actualValue = actualValues.get(i, SparkSchemaUtil.convert(keyType));

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
      case DATE:
      case TIMESTAMP:
        Assert.assertEquals("Primitive value should be equal to expected", expected, actual);
        break;
      case STRING:
        Assert.assertTrue("Should be a UTF8String", actual instanceof UTF8String);
        Assert.assertEquals("Strings should be equal", expected, actual.toString());
        break;
      case UUID:
        Assert.assertTrue("Should expect a UUID", expected instanceof UUID);
        Assert.assertTrue("Should be a UTF8String", actual instanceof UTF8String);
        Assert.assertEquals("UUID string representation should match",
            expected.toString(), actual.toString());
        break;
      case FIXED:
        Assert.assertTrue("Should expect a Fixed", expected instanceof GenericData.Fixed);
        Assert.assertTrue("Should be a byte[]", actual instanceof byte[]);
        Assert.assertArrayEquals("Bytes should match",
            ((GenericData.Fixed) expected).bytes(), (byte[]) actual);
        break;
      case BINARY:
        Assert.assertTrue("Should expect a ByteBuffer", expected instanceof ByteBuffer);
        Assert.assertTrue("Should be a byte[]", actual instanceof byte[]);
        Assert.assertArrayEquals("Bytes should match",
            ((ByteBuffer) expected).array(), (byte[]) actual);
        break;
      case DECIMAL:
        Assert.assertTrue("Should expect a BigDecimal", expected instanceof BigDecimal);
        Assert.assertTrue("Should be a Decimal", actual instanceof Decimal);
        Assert.assertEquals("BigDecimals should be equal",
            expected, ((Decimal) actual).toJavaBigDecimal());
        break;
      case STRUCT:
        Assert.assertTrue("Should expect a Record", expected instanceof Record);
        Assert.assertTrue("Should be an InternalRow", actual instanceof InternalRow);
        assertEqualsUnsafe(type.asNestedType().asStructType(), (Record) expected, (InternalRow) actual);
        break;
      case LIST:
        Assert.assertTrue("Should expect a Collection", expected instanceof Collection);
        Assert.assertTrue("Should be an InternalRow", actual instanceof ArrayData);
        assertEqualsUnsafe(type.asNestedType().asListType(), (Collection) expected, (ArrayData) actual);
        break;
      case MAP:
        Assert.assertTrue("Should expect a Collection", expected instanceof Map);
        Assert.assertTrue("Should be an InternalRow", actual instanceof ArrayBasedMapData);
        assertEqualsUnsafe(type.asNestedType().asMapType(), (Map) expected, (ArrayBasedMapData) actual);
        break;
      case TIME:
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }
}
