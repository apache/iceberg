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

package org.apache.iceberg.flink.data;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;

public class TestHelpers {
  private TestHelpers() {}

  private static final OffsetDateTime EPOCH = Instant.ofEpochMilli(0L).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  public static void assertRowData(Type type, Record expected, RowData actual) {
    if (expected == null && actual == null) {
      return;
    }

    List<Type> types = new ArrayList<>();
    for (Types.NestedField field : type.asStructType().fields()) {
      types.add(field.type());
    }

    for (int i = 0; i < types.size(); i += 1) {
      if (expected.get(i) == null) {
        Assert.assertTrue(actual.isNullAt(i));
        continue;
      }
      Type.TypeID typeId = types.get(i).typeId();
      Object value = expected.get(i);
      switch (typeId) {
        case BOOLEAN:
          Assert.assertEquals("boolean value should be equal", value, actual.getBoolean(i));
          break;
        case INTEGER:
          Assert.assertEquals("int value should be equal", value, actual.getInt(i));
          break;
        case LONG:
          Assert.assertEquals("long value should be equal", value, actual.getLong(i));
          break;
        case FLOAT:
          Assert.assertEquals("float value should be equal", value, actual.getFloat(i));
          break;
        case DOUBLE:
          Assert.assertEquals("double should be equal", value, actual.getDouble(i));
          break;
        case STRING:
          Assert.assertTrue("Should expect a CharSequence", value instanceof CharSequence);
          Assert.assertEquals("string should be equal", String.valueOf(value), actual.getString(i).toString());
          break;
        case DATE:
          Assert.assertTrue("Should expect a Date", value instanceof LocalDate);
          LocalDate date = ChronoUnit.DAYS.addTo(EPOCH_DAY, actual.getInt(i));
          Assert.assertEquals("date should be equal", value, date);
          break;
        case TIME:
          Assert.assertTrue("Should expect a LocalTime", value instanceof LocalTime);
          int milliseconds = (int) (((LocalTime) value).toNanoOfDay() / 1000_000);
          Assert.assertEquals("time millis should be equal", milliseconds, actual.getInt(i));
          break;
        case TIMESTAMP:
          if (((Types.TimestampType) type.asPrimitiveType()).shouldAdjustToUTC()) {
            Assert.assertTrue("Should expect a OffsetDataTime", value instanceof OffsetDateTime);
            OffsetDateTime ts = (OffsetDateTime) value;
            Assert.assertEquals("OffsetDataTime should be equal", ts.toLocalDateTime(),
                actual.getTimestamp(i, 6).toLocalDateTime());
          } else {
            Assert.assertTrue("Should expect a LocalDataTime", value instanceof LocalDateTime);
            LocalDateTime ts = (LocalDateTime) value;
            Assert.assertEquals("LocalDataTime should be equal", ts,
                actual.getTimestamp(i, 6).toLocalDateTime());
          }
          break;
        case FIXED:
          Assert.assertTrue("Should expect byte[]", value instanceof byte[]);
          Assert.assertArrayEquals("binary should be equal", (byte[]) value, actual.getBinary(i));
          break;
        case BINARY:
          Assert.assertTrue("Should expect a ByteBuffer", value instanceof ByteBuffer);
          Assert.assertArrayEquals("binary should be equal", ((ByteBuffer) value).array(), actual.getBinary(i));
          break;
        case DECIMAL:
          Assert.assertTrue("Should expect a BigDecimal", value instanceof BigDecimal);
          BigDecimal bd = (BigDecimal) value;
          Assert.assertEquals("decimal value should be equal", bd,
              actual.getDecimal(i, bd.precision(), bd.scale()).toBigDecimal());
          break;
        case LIST:
          Assert.assertTrue("Should expect a Collection", value instanceof Collection);
          Collection<?> arrayData1 = (Collection<?>) value;
          ArrayData arrayData2 = actual.getArray(i);
          Assert.assertEquals("array length should be equal", arrayData1.size(), arrayData2.size());
          for (int j = 0; j < arrayData1.size(); j += 1) {
            assertArrayValues(types.get(i).asListType().elementType(), arrayData1, arrayData2);
          }
          break;
        case MAP:
          Assert.assertTrue("Should expect a Map", value instanceof Map);
          Collection<?> keyArrayData1 = ((Map) value).keySet();
          Collection<?> valueArrayData1 = ((Map) value).values();
          ArrayData keyArrayData2 = actual.getMap(i).keyArray();
          ArrayData valueArrayData2 = actual.getMap(i).valueArray();
          Type keyType = types.get(i).asMapType().keyType();
          Type valueType = types.get(i).asMapType().valueType();
          Assert.assertEquals("map size should be equal", ((Map) value).size(), actual.getMap(i).size());

          for (int j = 0; j < keyArrayData1.size(); j += 1) {
            assertArrayValues(keyType, keyArrayData1, keyArrayData2);
            assertArrayValues(valueType, valueArrayData1, valueArrayData2);
          }
          break;
        case STRUCT:
          Assert.assertTrue("Should expect a Record", value instanceof Record);
          int numFields = types.get(i).asStructType().fields().size();
          assertRowData(types.get(i).asStructType(), (Record) value, actual.getRow(i, numFields));
          break;
        case UUID:
          Assert.assertTrue("Should expect a UUID", value instanceof UUID);
          Assert.assertEquals("UUID should be equal", value.toString(),
              UUID.nameUUIDFromBytes(actual.getBinary(i)).toString());
          break;
        default:
          throw new IllegalArgumentException("Not a supported type: " + type);
      }
    }
  }

  private static void assertArrayValues(Type type, Collection<?> expectedArray, ArrayData actualArray) {
    List<?> expectedElements = Lists.newArrayList(expectedArray);
    for (int i = 0; i < expectedArray.size(); i += 1) {
      if (expectedElements.get(i) == null) {
        Assert.assertTrue(actualArray.isNullAt(i));
        continue;
      }

      Object value = expectedElements.get(i);

      switch (type.typeId()) {
        case BOOLEAN:
          Assert.assertEquals("boolean value should be equal", value, actualArray.getBoolean(i));
          break;
        case INTEGER:
          Assert.assertEquals("int value should be equal", value, actualArray.getInt(i));
          break;
        case LONG:
          Assert.assertEquals("long value should be equal", value, actualArray.getLong(i));
          break;
        case FLOAT:
          Assert.assertEquals("float value should be equal", value, actualArray.getFloat(i));
          break;
        case DOUBLE:
          Assert.assertEquals("double value should be equal", value, actualArray.getDouble(i));
          break;
        case STRING:
          Assert.assertTrue("Should expect a CharSequence", value instanceof CharSequence);
          Assert.assertEquals("string should be equal", String.valueOf(value), actualArray.getString(i).toString());
          break;
        case DATE:
          Assert.assertTrue("Should expect a Date", value instanceof LocalDate);
          LocalDate date = ChronoUnit.DAYS.addTo(EPOCH_DAY, actualArray.getInt(i));
          Assert.assertEquals("date should be equal", value, date);
          break;
        case TIME:
          Assert.assertTrue("Should expect a LocalTime", value instanceof LocalTime);
          int milliseconds = (int) (((LocalTime) value).toNanoOfDay() / 1000_000);
          Assert.assertEquals("time millis should be equal", milliseconds, actualArray.getInt(i));
          break;
        case TIMESTAMP:
          if (((Types.TimestampType) type.asPrimitiveType()).shouldAdjustToUTC()) {
            Assert.assertTrue("Should expect a OffsetDataTime", value instanceof OffsetDateTime);
            OffsetDateTime ts = (OffsetDateTime) value;
            Assert.assertEquals("OffsetDataTime should be equal", ts.toLocalDateTime(),
                actualArray.getTimestamp(i, 6).toLocalDateTime());
          } else {
            Assert.assertTrue("Should expect a LocalDataTime", value instanceof LocalDateTime);
            LocalDateTime ts = (LocalDateTime) value;
            Assert.assertEquals("LocalDataTime should be equal", ts,
                actualArray.getTimestamp(i, 6).toLocalDateTime());
          }
          break;
        case BINARY:
          Assert.assertTrue("Should expect a ByteBuffer", value instanceof ByteBuffer);
          Assert.assertArrayEquals("binary should be equal", ((ByteBuffer) value).array(), actualArray.getBinary(i));
          break;
        case DECIMAL:
          Assert.assertTrue("Should expect a BigDecimal", value instanceof BigDecimal);
          BigDecimal bd = (BigDecimal) value;
          Assert.assertEquals("decimal value should be equal", bd,
              actualArray.getDecimal(i, bd.precision(), bd.scale()).toBigDecimal());
          break;
        case LIST:
          Assert.assertTrue("Should expect a Collection", value instanceof Collection);
          Collection<?> arrayData1 = (Collection<?>) value;
          ArrayData arrayData2 = actualArray.getArray(i);
          Assert.assertEquals("array length should be equal", arrayData1.size(), arrayData2.size());
          for (int j = 0; j < arrayData1.size(); j += 1) {
            assertArrayValues(type.asListType().elementType(), arrayData1, arrayData2);
          }
          break;
        case MAP:
          Assert.assertTrue("Should expect a Map", value instanceof Map);
          Collection<?> keyArrayData1 = ((Map) value).keySet();
          Collection<?> valueArrayData1 = ((Map) value).values();
          ArrayData keyArrayData2 = actualArray.getMap(i).keyArray();
          ArrayData valueArrayData2 = actualArray.getMap(i).valueArray();
          Type keyType = type.asMapType().keyType();
          Type valueType = type.asMapType().valueType();

          Assert.assertEquals("map size should be equal", ((Map) value).size(), actualArray.getMap(i).size());

          for (int j = 0; j < keyArrayData1.size(); j += 1) {
            assertArrayValues(keyType, keyArrayData1, keyArrayData2);
            assertArrayValues(valueType, valueArrayData1, valueArrayData2);
          }
          break;
        case STRUCT:
          Assert.assertTrue("Should expect a Record", value instanceof Record);
          int numFields = type.asStructType().fields().size();
          assertRowData(type.asStructType(), (Record) value, actualArray.getRow(i, numFields));
          break;
        case UUID:
          Assert.assertTrue("Should expect a UUID", value instanceof UUID);
          Assert.assertEquals("UUID should be equal", value.toString(),
              UUID.nameUUIDFromBytes(actualArray.getBinary(i)).toString());
          break;
        case FIXED:
          Assert.assertTrue("Should expect byte[]", value instanceof byte[]);
          Assert.assertArrayEquals("binary should be equal", (byte[]) value, actualArray.getBinary(i));
          break;
        default:
          throw new IllegalArgumentException("Not a supported type: " + type);
      }
    }
  }
}
