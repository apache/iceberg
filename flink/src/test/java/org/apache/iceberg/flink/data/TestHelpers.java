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

  public static void assertRowData(Type type, Record expectedRecord, RowData actualRowData) {
    if (expectedRecord == null && actualRowData == null) {
      return;
    }

    Assert.assertTrue("expected Record and actual RowData should be both null or not null",
        expectedRecord != null && actualRowData != null);

    List<Type> types = new ArrayList<>();
    for (Types.NestedField field : type.asStructType().fields()) {
      types.add(field.type());
    }

    for (int i = 0; i < types.size(); i += 1) {
      if (expectedRecord.get(i) == null) {
        Assert.assertTrue(actualRowData.isNullAt(i));
        continue;
      }
      Type.TypeID typeId = types.get(i).typeId();
      Object expected = expectedRecord.get(i);
      switch (typeId) {
        case BOOLEAN:
          Assert.assertEquals("boolean value should be equal", expected, actualRowData.getBoolean(i));
          break;
        case INTEGER:
          Assert.assertEquals("int value should be equal", expected, actualRowData.getInt(i));
          break;
        case LONG:
          Assert.assertEquals("long value should be equal", expected, actualRowData.getLong(i));
          break;
        case FLOAT:
          Assert.assertEquals("float value should be equal", expected, actualRowData.getFloat(i));
          break;
        case DOUBLE:
          Assert.assertEquals("double should be equal", expected, actualRowData.getDouble(i));
          break;
        case STRING:
          Assert.assertTrue("Should expect a CharSequence", expected instanceof CharSequence);
          Assert.assertEquals("string should be equal",
              String.valueOf(expected), actualRowData.getString(i).toString());
          break;
        case DATE:
          Assert.assertTrue("Should expect a Date", expected instanceof LocalDate);
          LocalDate date = ChronoUnit.DAYS.addTo(EPOCH_DAY, actualRowData.getInt(i));
          Assert.assertEquals("date should be equal", expected, date);
          break;
        case TIME:
          Assert.assertTrue("Should expect a LocalTime", expected instanceof LocalTime);
          int milliseconds = (int) (((LocalTime) expected).toNanoOfDay() / 1000_000);
          Assert.assertEquals("time millis should be equal", milliseconds, actualRowData.getInt(i));
          break;
        case TIMESTAMP:
          if (((Types.TimestampType) types.get(i).asPrimitiveType()).shouldAdjustToUTC()) {
            Assert.assertTrue("Should expect a OffsetDataTime", expected instanceof OffsetDateTime);
            OffsetDateTime ts = (OffsetDateTime) expected;
            Assert.assertEquals("OffsetDataTime should be equal", ts.toLocalDateTime(),
                actualRowData.getTimestamp(i, 6).toLocalDateTime());
          } else {
            Assert.assertTrue("Should expect a LocalDataTime", expected instanceof LocalDateTime);
            LocalDateTime ts = (LocalDateTime) expected;
            Assert.assertEquals("LocalDataTime should be equal", ts,
                actualRowData.getTimestamp(i, 6).toLocalDateTime());
          }
          break;
        case FIXED:
          Assert.assertTrue("Should expect byte[]", expected instanceof byte[]);
          Assert.assertArrayEquals("binary should be equal", (byte[]) expected, actualRowData.getBinary(i));
          break;
        case BINARY:
          Assert.assertTrue("Should expect a ByteBuffer", expected instanceof ByteBuffer);
          Assert.assertArrayEquals("binary should be equal",
              ((ByteBuffer) expected).array(), actualRowData.getBinary(i));
          break;
        case DECIMAL:
          Assert.assertTrue("Should expect a BigDecimal", expected instanceof BigDecimal);
          BigDecimal bd = (BigDecimal) expected;
          Assert.assertEquals("decimal value should be equal", bd,
              actualRowData.getDecimal(i, bd.precision(), bd.scale()).toBigDecimal());
          break;
        case LIST:
          Assert.assertTrue("Should expect a Collection", expected instanceof Collection);
          Collection<?> expectedArray = (Collection<?>) expected;
          ArrayData actualArray = actualRowData.getArray(i);
          Assert.assertEquals("array length should be equal", expectedArray.size(), actualArray.size());
          assertArrayValues(types.get(i).asListType().elementType(), expectedArray, actualArray);
          break;
        case MAP:
          Assert.assertTrue("Should expect a Map", expected instanceof Map);
          Collection<?> expectedKeyArray = ((Map<?, ?>) expected).keySet();
          Collection<?> expectedValueArray = ((Map<?, ?>) expected).values();
          ArrayData actualKeyArray = actualRowData.getMap(i).keyArray();
          ArrayData actualValueArray = actualRowData.getMap(i).valueArray();
          Type keyType = types.get(i).asMapType().keyType();
          Type valueType = types.get(i).asMapType().valueType();
          Assert.assertEquals("map size should be equal",
              ((Map<?, ?>) expected).size(), actualRowData.getMap(i).size());
          assertArrayValues(keyType, expectedKeyArray, actualKeyArray);
          assertArrayValues(valueType, expectedValueArray, actualValueArray);
          break;
        case STRUCT:
          Assert.assertTrue("Should expect a Record", expected instanceof Record);
          int numFields = types.get(i).asStructType().fields().size();
          assertRowData(types.get(i).asStructType(), (Record) expected, actualRowData.getRow(i, numFields));
          break;
        case UUID:
          Assert.assertTrue("Should expect a UUID", expected instanceof UUID);
          Assert.assertEquals("UUID should be equal", expected.toString(),
              UUID.nameUUIDFromBytes(actualRowData.getBinary(i)).toString());
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

      Object expected = expectedElements.get(i);

      switch (type.typeId()) {
        case BOOLEAN:
          Assert.assertEquals("boolean value should be equal", expected, actualArray.getBoolean(i));
          break;
        case INTEGER:
          Assert.assertEquals("int value should be equal", expected, actualArray.getInt(i));
          break;
        case LONG:
          Assert.assertEquals("long value should be equal", expected, actualArray.getLong(i));
          break;
        case FLOAT:
          Assert.assertEquals("float value should be equal", expected, actualArray.getFloat(i));
          break;
        case DOUBLE:
          Assert.assertEquals("double value should be equal", expected, actualArray.getDouble(i));
          break;
        case STRING:
          Assert.assertTrue("Should expect a CharSequence", expected instanceof CharSequence);
          Assert.assertEquals("string should be equal",
              String.valueOf(expected), actualArray.getString(i).toString());
          break;
        case DATE:
          Assert.assertTrue("Should expect a Date", expected instanceof LocalDate);
          LocalDate date = ChronoUnit.DAYS.addTo(EPOCH_DAY, actualArray.getInt(i));
          Assert.assertEquals("date should be equal", expected, date);
          break;
        case TIME:
          Assert.assertTrue("Should expect a LocalTime", expected instanceof LocalTime);
          int milliseconds = (int) (((LocalTime) expected).toNanoOfDay() / 1000_000);
          Assert.assertEquals("time millis should be equal", milliseconds, actualArray.getInt(i));
          break;
        case TIMESTAMP:
          if (((Types.TimestampType) type.asPrimitiveType()).shouldAdjustToUTC()) {
            Assert.assertTrue("Should expect a OffsetDataTime", expected instanceof OffsetDateTime);
            OffsetDateTime ts = (OffsetDateTime) expected;
            Assert.assertEquals("OffsetDataTime should be equal", ts.toLocalDateTime(),
                actualArray.getTimestamp(i, 6).toLocalDateTime());
          } else {
            Assert.assertTrue("Should expect a LocalDataTime", expected instanceof LocalDateTime);
            LocalDateTime ts = (LocalDateTime) expected;
            Assert.assertEquals("LocalDataTime should be equal", ts,
                actualArray.getTimestamp(i, 6).toLocalDateTime());
          }
          break;
        case BINARY:
          Assert.assertTrue("Should expect a ByteBuffer", expected instanceof ByteBuffer);
          Assert.assertArrayEquals("binary should be equal",
              ((ByteBuffer) expected).array(), actualArray.getBinary(i));
          break;
        case DECIMAL:
          Assert.assertTrue("Should expect a BigDecimal", expected instanceof BigDecimal);
          BigDecimal bd = (BigDecimal) expected;
          Assert.assertEquals("decimal value should be equal", bd,
              actualArray.getDecimal(i, bd.precision(), bd.scale()).toBigDecimal());
          break;
        case LIST:
          Assert.assertTrue("Should expect a Collection", expected instanceof Collection);
          Collection<?> expectedArrayData = (Collection<?>) expected;
          ArrayData actualArrayData = actualArray.getArray(i);
          Assert.assertEquals("array length should be equal", expectedArrayData.size(), actualArrayData.size());
          assertArrayValues(type.asListType().elementType(), expectedArrayData, actualArrayData);
          break;
        case MAP:
          Assert.assertTrue("Should expect a Map", expected instanceof Map);
          Assert.assertEquals("map size should be equal",
              ((Map<?, ?>) expected).size(), actualArray.getMap(i).size());
          Collection<?> expectedKeyArrayData = ((Map<?, ?>) expected).keySet();
          Collection<?> expectedValueArrayData = ((Map<?, ?>) expected).values();
          ArrayData actualKeyArrayData = actualArray.getMap(i).keyArray();
          ArrayData actualValueArrayData = actualArray.getMap(i).valueArray();
          Type keyType = type.asMapType().keyType();
          Type valueType = type.asMapType().valueType();
          assertArrayValues(keyType, expectedKeyArrayData, actualKeyArrayData);
          assertArrayValues(valueType, expectedValueArrayData, actualValueArrayData);
          break;
        case STRUCT:
          Assert.assertTrue("Should expect a Record", expected instanceof Record);
          int numFields = type.asStructType().fields().size();
          assertRowData(type.asStructType(), (Record) expected, actualArray.getRow(i, numFields));
          break;
        case UUID:
          Assert.assertTrue("Should expect a UUID", expected instanceof UUID);
          Assert.assertEquals("UUID should be equal", expected.toString(),
              UUID.nameUUIDFromBytes(actualArray.getBinary(i)).toString());
          break;
        case FIXED:
          Assert.assertTrue("Should expect byte[]", expected instanceof byte[]);
          Assert.assertArrayEquals("binary should be equal", (byte[]) expected, actualArray.getBinary(i));
          break;
        default:
          throw new IllegalArgumentException("Not a supported type: " + type);
      }
    }
  }
}
