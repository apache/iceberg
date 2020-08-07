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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
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
import org.junit.Assert;

public class TestHelpers {
  private TestHelpers() {}

  private static final OffsetDateTime EPOCH = Instant.ofEpochMilli(0L).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  public static void assertRowData(Type type, LogicalType rowType, Record expectedRecord, RowData actualRowData) {
    if (expectedRecord == null && actualRowData == null) {
      return;
    }

    Assert.assertTrue("expected Record and actual RowData should be both null or not null",
        expectedRecord != null && actualRowData != null);

    List<Type> types = Lists.newArrayList();
    for (Types.NestedField field : type.asStructType().fields()) {
      types.add(field.type());
    }

    for (int i = 0; i < types.size(); i += 1) {
      if (expectedRecord.get(i) == null) {
        Assert.assertTrue(actualRowData.isNullAt(i));
        continue;
      }

      Object expected = expectedRecord.get(i);
      LogicalType logicalType = ((RowType) rowType).getTypeAt(i);

      final int fieldPos = i;
      assertEquals(types.get(i), logicalType, expected,
          () -> RowData.createFieldGetter(logicalType, fieldPos).getFieldOrNull(actualRowData));
    }

  }

  private static void assertEquals(Type type, LogicalType logicalType, Object expected, Supplier<Object> supplier) {
    switch (type.typeId()) {
      case BOOLEAN:
        Assert.assertEquals("boolean value should be equal", expected, supplier.get());
        break;
      case INTEGER:
        Assert.assertEquals("int value should be equal", expected, supplier.get());
        break;
      case LONG:
        Assert.assertEquals("long value should be equal", expected, supplier.get());
        break;
      case FLOAT:
        Assert.assertEquals("float value should be equal", expected, supplier.get());
        break;
      case DOUBLE:
        Assert.assertEquals("double value should be equal", expected, supplier.get());
        break;
      case STRING:
        Assert.assertTrue("Should expect a CharSequence", expected instanceof CharSequence);
        Assert.assertEquals("string should be equal",
            String.valueOf(expected), supplier.get().toString());
        break;
      case DATE:
        Assert.assertTrue("Should expect a Date", expected instanceof LocalDate);
        LocalDate date = ChronoUnit.DAYS.addTo(EPOCH_DAY, (int) supplier.get());
        Assert.assertEquals("date should be equal", expected, date);
        break;
      case TIME:
        Assert.assertTrue("Should expect a LocalTime", expected instanceof LocalTime);
        int milliseconds = (int) (((LocalTime) expected).toNanoOfDay() / 1000_000);
        Assert.assertEquals("time millis should be equal", milliseconds, supplier.get());
        break;
      case TIMESTAMP:
        if (((Types.TimestampType) type.asPrimitiveType()).shouldAdjustToUTC()) {
          Assert.assertTrue("Should expect a OffsetDataTime", expected instanceof OffsetDateTime);
          OffsetDateTime ts = (OffsetDateTime) expected;
          Assert.assertEquals("OffsetDataTime should be equal", ts.toLocalDateTime(),
              ((TimestampData) supplier.get()).toLocalDateTime());
        } else {
          Assert.assertTrue("Should expect a LocalDataTime", expected instanceof LocalDateTime);
          LocalDateTime ts = (LocalDateTime) expected;
          Assert.assertEquals("LocalDataTime should be equal", ts,
              ((TimestampData) supplier.get()).toLocalDateTime());
        }
        break;
      case BINARY:
        Assert.assertTrue("Should expect a ByteBuffer", expected instanceof ByteBuffer);
        Assert.assertArrayEquals("binary should be equal", ((ByteBuffer) expected).array(), (byte[]) supplier.get());
        break;
      case DECIMAL:
        Assert.assertTrue("Should expect a BigDecimal", expected instanceof BigDecimal);
        BigDecimal bd = (BigDecimal) expected;
        Assert.assertEquals("decimal value should be equal", bd,
            ((DecimalData) supplier.get()).toBigDecimal());
        break;
      case LIST:
        Assert.assertTrue("Should expect a Collection", expected instanceof Collection);
        Collection<?> expectedArrayData = (Collection<?>) expected;
        ArrayData actualArrayData = (ArrayData) supplier.get();
        LogicalType elementType = ((ArrayType) logicalType).getElementType();
        Assert.assertEquals("array length should be equal", expectedArrayData.size(), actualArrayData.size());
        assertArrayValues(type.asListType().elementType(), elementType, expectedArrayData, actualArrayData);
        break;
      case MAP:
        Assert.assertTrue("Should expect a Map", expected instanceof Map);
        MapData actual = (MapData) supplier.get();
        Assert.assertEquals("map size should be equal",
            ((Map<?, ?>) expected).size(), actual.size());
        Collection<?> expectedKeyArrayData = ((Map<?, ?>) expected).keySet();
        Collection<?> expectedValueArrayData = ((Map<?, ?>) expected).values();
        ArrayData actualKeyArrayData = actual.keyArray();
        ArrayData actualValueArrayData = actual.valueArray();
        Type keyType = type.asMapType().keyType();
        Type valueType = type.asMapType().valueType();
        LogicalType actualKeyType = ((MapType) logicalType).getKeyType();
        LogicalType actualValueType = ((MapType) logicalType).getValueType();
        assertArrayValues(keyType, actualKeyType, expectedKeyArrayData, actualKeyArrayData);
        assertArrayValues(valueType, actualValueType, expectedValueArrayData, actualValueArrayData);
        break;
      case STRUCT:
        Assert.assertTrue("Should expect a Record", expected instanceof Record);
        assertRowData(type.asStructType(), logicalType, (Record) expected, (RowData) supplier.get());
        break;
      case UUID:
        Assert.assertTrue("Should expect a UUID", expected instanceof UUID);
        Assert.assertEquals("UUID should be equal", expected.toString(),
            UUID.nameUUIDFromBytes((byte[]) supplier.get()).toString());
        break;
      case FIXED:
        Assert.assertTrue("Should expect byte[]", expected instanceof byte[]);
        Assert.assertArrayEquals("binary should be equal", (byte[]) expected, (byte[]) supplier.get());
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

      final int pos = i;
      assertEquals(type, logicalType, expected,
          () -> ArrayData.createElementGetter(logicalType).getElementOrNull(actualArray, pos));
    }
  }
}
