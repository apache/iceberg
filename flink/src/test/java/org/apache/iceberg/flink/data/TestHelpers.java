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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;

public class TestHelpers {
  private TestHelpers() {
  }

  public static void assertRowData(Types.StructType structType, LogicalType rowType, Record expectedRecord,
                                   RowData actualRowData) {
    if (expectedRecord == null && actualRowData == null) {
      return;
    }

    Assert.assertTrue("expected Record and actual RowData should be both null or not null",
        expectedRecord != null && actualRowData != null);

    List<Type> types = Lists.newArrayList();
    for (Types.NestedField field : structType.fields()) {
      types.add(field.type());
    }

    for (int i = 0; i < types.size(); i += 1) {
      Object expected = expectedRecord.get(i);
      LogicalType logicalType = ((RowType) rowType).getTypeAt(i);
      assertEquals(types.get(i), logicalType, expected,
          RowData.createFieldGetter(logicalType, i).getFieldOrNull(actualRowData));
    }
  }

  private static void assertEquals(Type type, LogicalType logicalType, Object expected, Object actual) {

    if (expected == null && actual == null) {
      return;
    }

    Assert.assertTrue("expected and actual should be both null or not null",
        expected != null && actual != null);

    switch (type.typeId()) {
      case BOOLEAN:
        Assert.assertEquals("boolean value should be equal", expected, actual);
        break;
      case INTEGER:
        Assert.assertEquals("int value should be equal", expected, actual);
        break;
      case LONG:
        Assert.assertEquals("long value should be equal", expected, actual);
        break;
      case FLOAT:
        Assert.assertEquals("float value should be equal", expected, actual);
        break;
      case DOUBLE:
        Assert.assertEquals("double value should be equal", expected, actual);
        break;
      case STRING:
        Assert.assertTrue("Should expect a CharSequence", expected instanceof CharSequence);
        Assert.assertEquals("string should be equal", String.valueOf(expected), actual.toString());
        break;
      case DATE:
        Assert.assertTrue("Should expect a Date", expected instanceof LocalDate);
        LocalDate date = DateTimeUtil.dateFromDays((int) actual);
        Assert.assertEquals("date should be equal", expected, date);
        break;
      case TIME:
        Assert.assertTrue("Should expect a LocalTime", expected instanceof LocalTime);
        int milliseconds = (int) (((LocalTime) expected).toNanoOfDay() / 1000_000);
        Assert.assertEquals("time millis should be equal", milliseconds, actual);
        break;
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          Assert.assertTrue("Should expect a OffsetDataTime", expected instanceof OffsetDateTime);
          OffsetDateTime ts = (OffsetDateTime) expected;
          Assert.assertEquals("OffsetDataTime should be equal", ts.toLocalDateTime(),
              ((TimestampData) actual).toLocalDateTime());
        } else {
          Assert.assertTrue("Should expect a LocalDataTime", expected instanceof LocalDateTime);
          LocalDateTime ts = (LocalDateTime) expected;
          Assert.assertEquals("LocalDataTime should be equal", ts,
              ((TimestampData) actual).toLocalDateTime());
        }
        break;
      case BINARY:
        Assert.assertTrue("Should expect a ByteBuffer", expected instanceof ByteBuffer);
        Assert.assertEquals("binary should be equal", expected, ByteBuffer.wrap((byte[]) actual));
        break;
      case DECIMAL:
        Assert.assertTrue("Should expect a BigDecimal", expected instanceof BigDecimal);
        BigDecimal bd = (BigDecimal) expected;
        Assert.assertEquals("decimal value should be equal", bd,
            ((DecimalData) actual).toBigDecimal());
        break;
      case LIST:
        Assert.assertTrue("Should expect a Collection", expected instanceof Collection);
        Collection<?> expectedArrayData = (Collection<?>) expected;
        ArrayData actualArrayData = (ArrayData) actual;
        LogicalType elementType = ((ArrayType) logicalType).getElementType();
        Assert.assertEquals("array length should be equal", expectedArrayData.size(), actualArrayData.size());
        assertArrayValues(type.asListType().elementType(), elementType, expectedArrayData, actualArrayData);
        break;
      case MAP:
        Assert.assertTrue("Should expect a Map", expected instanceof Map);
        assertMapValues(type.asMapType(), logicalType, (Map<?, ?>) expected, (MapData) actual);
        break;
      case STRUCT:
        Assert.assertTrue("Should expect a Record", expected instanceof Record);
        assertRowData(type.asStructType(), logicalType, (Record) expected, (RowData) actual);
        break;
      case UUID:
        Assert.assertTrue("Should expect a UUID", expected instanceof UUID);
        Assert.assertEquals("UUID should be equal", expected.toString(),
            UUID.nameUUIDFromBytes((byte[]) actual).toString());
        break;
      case FIXED:
        Assert.assertTrue("Should expect byte[]", expected instanceof byte[]);
        Assert.assertArrayEquals("binary should be equal", (byte[]) expected, (byte[]) actual);
        break;
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }

  private static void assertArrayValues(Type type, LogicalType logicalType, Collection<?> expectedArray,
                                        ArrayData actualArray) {
    List<?> expectedElements = Lists.newArrayList(expectedArray);
    for (int i = 0; i < expectedArray.size(); i += 1) {
      if (expectedElements.get(i) == null) {
        Assert.assertTrue(actualArray.isNullAt(i));
        continue;
      }

      Object expected = expectedElements.get(i);

      assertEquals(type, logicalType, expected,
          ArrayData.createElementGetter(logicalType).getElementOrNull(actualArray, i));
    }
  }

  private static void assertMapValues(Types.MapType mapType, LogicalType type, Map<?, ?> expected, MapData actual) {
    Assert.assertEquals("map size should be equal", expected.size(), actual.size());

    ArrayData actualKeyArrayData = actual.keyArray();
    ArrayData actualValueArrayData = actual.valueArray();
    LogicalType actualKeyType = ((MapType) type).getKeyType();
    LogicalType actualValueType = ((MapType) type).getValueType();
    Type keyType = mapType.keyType();
    Type valueType = mapType.valueType();

    ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(actualKeyType);
    ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(actualValueType);

    for (Map.Entry<?, ?> entry : expected.entrySet()) {
      Object matchedActualKey = null;
      int matchedKeyIndex = 0;
      for (int i = 0; i < actual.size(); i += 1) {
        try {
          Object key = keyGetter.getElementOrNull(actualKeyArrayData, i);
          assertEquals(keyType, actualKeyType, entry.getKey(), key);
          matchedActualKey = key;
          matchedKeyIndex = i;
          break;
        } catch (AssertionError e) {
          // not found
        }
      }
      Assert.assertNotNull("Should have a matching key", matchedActualKey);
      final int valueIndex = matchedKeyIndex;
      assertEquals(valueType, actualValueType, entry.getValue(),
          valueGetter.getElementOrNull(actualValueArrayData, valueIndex));
    }
  }
}
