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
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.storage.serde2.io.DateWritable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import scala.collection.Seq;

public class TestHelpers {

  private TestHelpers() {}

  public static void assertEqualsSafe(Types.StructType struct, List<Record> recs, List<Row> rows) {
    Streams.forEachPair(
        recs.stream(), rows.stream(), (rec, row) -> assertEqualsSafe(struct, rec, row));
  }

  public static void assertEqualsSafe(Types.StructType struct, Record rec, Row row) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Type fieldType = fields.get(i).type();

      Object expectedValue = rec.get(i);
      Object actualValue = row.get(i);

      assertEqualsSafe(fieldType, expectedValue, actualValue);
    }
  }

  public static void assertEqualsBatch(
      Types.StructType struct, Iterator<Record> expected, ColumnarBatch batch) {
    for (int rowId = 0; rowId < batch.numRows(); rowId++) {
      List<Types.NestedField> fields = struct.fields();
      InternalRow row = batch.getRow(rowId);
      Record rec = expected.next();
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i).type();
        Object expectedValue = rec.get(i);
        Object actualValue = row.isNullAt(i) ? null : row.get(i, convert(fieldType));
        assertEqualsUnsafe(fieldType, expectedValue, actualValue);
      }
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

  private static void assertEqualsSafe(Types.MapType map, Map<?, ?> expected, Map<?, ?> actual) {
    Type keyType = map.keyType();
    Type valueType = map.valueType();

    for (Object expectedKey : expected.keySet()) {
      Object matchingKey = null;
      for (Object actualKey : actual.keySet()) {
        try {
          assertEqualsSafe(keyType, expectedKey, actualKey);
          matchingKey = actualKey;
        } catch (AssertionError e) {
          // failed
        }
      }

      Assert.assertNotNull("Should have a matching key", matchingKey);
      assertEqualsSafe(valueType, expected.get(expectedKey), actual.get(matchingKey));
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
        Assertions.assertThat(expected).as("Should be an int").isInstanceOf(Integer.class);
        Assertions.assertThat(actual).as("Should be a Date").isInstanceOf(Date.class);
        int daysFromEpoch = (Integer) expected;
        LocalDate date = ChronoUnit.DAYS.addTo(EPOCH_DAY, daysFromEpoch);
        Assert.assertEquals("ISO-8601 date should be equal", date.toString(), actual.toString());
        break;
      case TIMESTAMP:
        Assertions.assertThat(expected).as("Should be a long").isInstanceOf(Long.class);
        Assertions.assertThat(actual).as("Should be a Timestamp").isInstanceOf(Timestamp.class);
        Timestamp ts = (Timestamp) actual;
        // milliseconds from nanos has already been added by getTime
        long tsMicros = (ts.getTime() * 1000) + ((ts.getNanos() / 1000) % 1000);
        Assert.assertEquals("Timestamp micros should be equal", expected, tsMicros);
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
        Assertions.assertThat(expected)
            .as("Should expect a Fixed")
            .isInstanceOf(GenericData.Fixed.class);
        Assertions.assertThat(actual).as("Should be a byte[]").isInstanceOf(byte[].class);
        Assert.assertArrayEquals(
            "Bytes should match", ((GenericData.Fixed) expected).bytes(), (byte[]) actual);
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
        assertEqualsSafe(type.asNestedType().asListType(), (Collection) expected, asList);
        break;
      case MAP:
        Assertions.assertThat(expected).as("Should expect a Collection").isInstanceOf(Map.class);
        Assertions.assertThat(actual)
            .as("Should be a Map")
            .isInstanceOf(scala.collection.Map.class);
        Map<String, ?> asMap =
            mapAsJavaMapConverter((scala.collection.Map<String, ?>) actual).asJava();
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
      Object actualValue = row.isNullAt(i) ? null : row.get(i, convert(fieldType));

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
      case LONG:
        Assertions.assertThat(actual).as("Should be a long").isInstanceOf(Long.class);
        if (expected instanceof Integer) {
          Assert.assertEquals("Values didn't match", ((Number) expected).longValue(), actual);
        } else {
          Assert.assertEquals("Primitive value should be equal to expected", expected, actual);
        }
        break;
      case DOUBLE:
        Assertions.assertThat(actual).as("Should be a double").isInstanceOf(Double.class);
        if (expected instanceof Float) {
          Assert.assertEquals(
              "Values didn't match",
              Double.doubleToLongBits(((Number) expected).doubleValue()),
              Double.doubleToLongBits((double) actual));
        } else {
          Assert.assertEquals("Primitive value should be equal to expected", expected, actual);
        }
        break;
      case INTEGER:
      case FLOAT:
      case BOOLEAN:
      case DATE:
      case TIMESTAMP:
        Assert.assertEquals("Primitive value should be equal to expected", expected, actual);
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
        Assertions.assertThat(expected)
            .as("Should expect a Fixed")
            .isInstanceOf(GenericData.Fixed.class);
        Assertions.assertThat(actual).as("Should be a byte[]").isInstanceOf(byte[].class);
        Assert.assertArrayEquals(
            "Bytes should match", ((GenericData.Fixed) expected).bytes(), (byte[]) actual);
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
            type.asNestedType().asListType(), (Collection) expected, (ArrayData) actual);
        break;
      case MAP:
        Assertions.assertThat(expected).as("Should expect a Map").isInstanceOf(Map.class);
        Assertions.assertThat(actual)
            .as("Should be an ArrayBasedMapData")
            .isInstanceOf(MapData.class);
        assertEqualsUnsafe(type.asNestedType().asMapType(), (Map) expected, (MapData) actual);
        break;
      case TIME:
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }

  /**
   * Check that the given InternalRow is equivalent to the Row.
   *
   * @param prefix context for error messages
   * @param type the type of the row
   * @param expected the expected value of the row
   * @param actual the actual value of the row
   */
  public static void assertEquals(
      String prefix, Types.StructType type, InternalRow expected, Row actual) {
    if (expected == null || actual == null) {
      Assert.assertEquals(prefix, expected, actual);
    } else {
      List<Types.NestedField> fields = type.fields();
      for (int c = 0; c < fields.size(); ++c) {
        String fieldName = fields.get(c).name();
        Type childType = fields.get(c).type();
        switch (childType.typeId()) {
          case BOOLEAN:
          case INTEGER:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
          case DECIMAL:
          case DATE:
          case TIMESTAMP:
            Assert.assertEquals(
                prefix + "." + fieldName + " - " + childType,
                getValue(expected, c, childType),
                getPrimitiveValue(actual, c, childType));
            break;
          case UUID:
          case FIXED:
          case BINARY:
            assertEqualBytes(
                prefix + "." + fieldName,
                (byte[]) getValue(expected, c, childType),
                (byte[]) actual.get(c));
            break;
          case STRUCT:
            {
              Types.StructType st = (Types.StructType) childType;
              assertEquals(
                  prefix + "." + fieldName,
                  st,
                  expected.getStruct(c, st.fields().size()),
                  actual.getStruct(c));
              break;
            }
          case LIST:
            assertEqualsLists(
                prefix + "." + fieldName,
                childType.asListType(),
                expected.getArray(c),
                toList((Seq<?>) actual.get(c)));
            break;
          case MAP:
            assertEqualsMaps(
                prefix + "." + fieldName,
                childType.asMapType(),
                expected.getMap(c),
                toJavaMap((scala.collection.Map<?, ?>) actual.getMap(c)));
            break;
          default:
            throw new IllegalArgumentException("Unhandled type " + childType);
        }
      }
    }
  }

  private static void assertEqualsLists(
      String prefix, Types.ListType type, ArrayData expected, List actual) {
    if (expected == null || actual == null) {
      Assert.assertEquals(prefix, expected, actual);
    } else {
      Assert.assertEquals(prefix + " length", expected.numElements(), actual.size());
      Type childType = type.elementType();
      for (int e = 0; e < expected.numElements(); ++e) {
        switch (childType.typeId()) {
          case BOOLEAN:
          case INTEGER:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
          case DECIMAL:
          case DATE:
          case TIMESTAMP:
            Assert.assertEquals(
                prefix + ".elem " + e + " - " + childType,
                getValue(expected, e, childType),
                actual.get(e));
            break;
          case UUID:
          case FIXED:
          case BINARY:
            assertEqualBytes(
                prefix + ".elem " + e,
                (byte[]) getValue(expected, e, childType),
                (byte[]) actual.get(e));
            break;
          case STRUCT:
            {
              Types.StructType st = (Types.StructType) childType;
              assertEquals(
                  prefix + ".elem " + e,
                  st,
                  expected.getStruct(e, st.fields().size()),
                  (Row) actual.get(e));
              break;
            }
          case LIST:
            assertEqualsLists(
                prefix + ".elem " + e,
                childType.asListType(),
                expected.getArray(e),
                toList((Seq<?>) actual.get(e)));
            break;
          case MAP:
            assertEqualsMaps(
                prefix + ".elem " + e,
                childType.asMapType(),
                expected.getMap(e),
                toJavaMap((scala.collection.Map<?, ?>) actual.get(e)));
            break;
          default:
            throw new IllegalArgumentException("Unhandled type " + childType);
        }
      }
    }
  }

  private static void assertEqualsMaps(
      String prefix, Types.MapType type, MapData expected, Map<?, ?> actual) {
    if (expected == null || actual == null) {
      Assert.assertEquals(prefix, expected, actual);
    } else {
      Type keyType = type.keyType();
      Type valueType = type.valueType();
      ArrayData expectedKeyArray = expected.keyArray();
      ArrayData expectedValueArray = expected.valueArray();
      Assert.assertEquals(prefix + " length", expected.numElements(), actual.size());
      for (int e = 0; e < expected.numElements(); ++e) {
        Object expectedKey = getValue(expectedKeyArray, e, keyType);
        Object actualValue = actual.get(expectedKey);
        if (actualValue == null) {
          Assert.assertEquals(
              prefix + ".key=" + expectedKey + " has null",
              true,
              expected.valueArray().isNullAt(e));
        } else {
          switch (valueType.typeId()) {
            case BOOLEAN:
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case DECIMAL:
            case DATE:
            case TIMESTAMP:
              Assert.assertEquals(
                  prefix + ".key=" + expectedKey + " - " + valueType,
                  getValue(expectedValueArray, e, valueType),
                  actual.get(expectedKey));
              break;
            case UUID:
            case FIXED:
            case BINARY:
              assertEqualBytes(
                  prefix + ".key=" + expectedKey,
                  (byte[]) getValue(expectedValueArray, e, valueType),
                  (byte[]) actual.get(expectedKey));
              break;
            case STRUCT:
              {
                Types.StructType st = (Types.StructType) valueType;
                assertEquals(
                    prefix + ".key=" + expectedKey,
                    st,
                    expectedValueArray.getStruct(e, st.fields().size()),
                    (Row) actual.get(expectedKey));
                break;
              }
            case LIST:
              assertEqualsLists(
                  prefix + ".key=" + expectedKey,
                  valueType.asListType(),
                  expectedValueArray.getArray(e),
                  toList((Seq<?>) actual.get(expectedKey)));
              break;
            case MAP:
              assertEqualsMaps(
                  prefix + ".key=" + expectedKey,
                  valueType.asMapType(),
                  expectedValueArray.getMap(e),
                  toJavaMap((scala.collection.Map<?, ?>) actual.get(expectedKey)));
              break;
            default:
              throw new IllegalArgumentException("Unhandled type " + valueType);
          }
        }
      }
    }
  }

  private static Object getValue(SpecializedGetters container, int ord, Type type) {
    if (container.isNullAt(ord)) {
      return null;
    }
    switch (type.typeId()) {
      case BOOLEAN:
        return container.getBoolean(ord);
      case INTEGER:
        return container.getInt(ord);
      case LONG:
        return container.getLong(ord);
      case FLOAT:
        return container.getFloat(ord);
      case DOUBLE:
        return container.getDouble(ord);
      case STRING:
        return container.getUTF8String(ord).toString();
      case BINARY:
      case FIXED:
      case UUID:
        return container.getBinary(ord);
      case DATE:
        return new DateWritable(container.getInt(ord)).get();
      case TIMESTAMP:
        return DateTimeUtils.toJavaTimestamp(container.getLong(ord));
      case DECIMAL:
        {
          Types.DecimalType dt = (Types.DecimalType) type;
          return container.getDecimal(ord, dt.precision(), dt.scale()).toJavaBigDecimal();
        }
      case STRUCT:
        Types.StructType struct = type.asStructType();
        InternalRow internalRow = container.getStruct(ord, struct.fields().size());
        Object[] data = new Object[struct.fields().size()];
        for (int i = 0; i < data.length; i += 1) {
          if (internalRow.isNullAt(i)) {
            data[i] = null;
          } else {
            data[i] = getValue(internalRow, i, struct.fields().get(i).type());
          }
        }
        return new GenericRow(data);
      default:
        throw new IllegalArgumentException("Unhandled type " + type);
    }
  }

  private static Object getPrimitiveValue(Row row, int ord, Type type) {
    if (row.isNullAt(ord)) {
      return null;
    }
    switch (type.typeId()) {
      case BOOLEAN:
        return row.getBoolean(ord);
      case INTEGER:
        return row.getInt(ord);
      case LONG:
        return row.getLong(ord);
      case FLOAT:
        return row.getFloat(ord);
      case DOUBLE:
        return row.getDouble(ord);
      case STRING:
        return row.getString(ord);
      case BINARY:
      case FIXED:
      case UUID:
        return row.get(ord);
      case DATE:
        return row.getDate(ord);
      case TIMESTAMP:
        return row.getTimestamp(ord);
      case DECIMAL:
        return row.getDecimal(ord);
      default:
        throw new IllegalArgumentException("Unhandled type " + type);
    }
  }

  private static <K, V> Map<K, V> toJavaMap(scala.collection.Map<K, V> map) {
    return map == null ? null : mapAsJavaMapConverter(map).asJava();
  }

  private static List toList(Seq<?> val) {
    return val == null ? null : seqAsJavaListConverter(val).asJava();
  }

  private static void assertEqualBytes(String context, byte[] expected, byte[] actual) {
    if (expected == null || actual == null) {
      Assert.assertEquals(context, expected, actual);
    } else {
      Assert.assertArrayEquals(context, expected, actual);
    }
  }

  static void assertEquals(Schema schema, Object expected, Object actual) {
    assertEquals("schema", convert(schema), expected, actual);
  }

  private static void assertEquals(String context, DataType type, Object expected, Object actual) {
    if (expected == null && actual == null) {
      return;
    }

    if (type instanceof StructType) {
      Assertions.assertThat(expected)
          .as("Expected should be an InternalRow: " + context)
          .isInstanceOf(InternalRow.class);
      Assertions.assertThat(actual)
          .as("Actual should be an InternalRow: " + context)
          .isInstanceOf(InternalRow.class);
      assertEquals(context, (StructType) type, (InternalRow) expected, (InternalRow) actual);

    } else if (type instanceof ArrayType) {
      Assertions.assertThat(expected)
          .as("Expected should be an ArrayData: " + context)
          .isInstanceOf(ArrayData.class);
      Assertions.assertThat(actual)
          .as("Actual should be an ArrayData: " + context)
          .isInstanceOf(ArrayData.class);
      assertEquals(context, (ArrayType) type, (ArrayData) expected, (ArrayData) actual);

    } else if (type instanceof MapType) {
      Assertions.assertThat(expected)
          .as("Expected should be a MapData: " + context)
          .isInstanceOf(MapData.class);
      Assertions.assertThat(actual)
          .as("Actual should be a MapData: " + context)
          .isInstanceOf(MapData.class);
      assertEquals(context, (MapType) type, (MapData) expected, (MapData) actual);

    } else if (type instanceof BinaryType) {
      assertEqualBytes(context, (byte[]) expected, (byte[]) actual);
    } else {
      Assert.assertEquals("Value should match expected: " + context, expected, actual);
    }
  }

  private static void assertEquals(
      String context, StructType struct, InternalRow expected, InternalRow actual) {
    Assert.assertEquals("Should have correct number of fields", struct.size(), actual.numFields());
    for (int i = 0; i < actual.numFields(); i += 1) {
      StructField field = struct.fields()[i];
      DataType type = field.dataType();
      assertEquals(
          context + "." + field.name(),
          type,
          expected.isNullAt(i) ? null : expected.get(i, type),
          actual.isNullAt(i) ? null : actual.get(i, type));
    }
  }

  private static void assertEquals(
      String context, ArrayType array, ArrayData expected, ArrayData actual) {
    Assert.assertEquals(
        "Should have the same number of elements", expected.numElements(), actual.numElements());
    DataType type = array.elementType();
    for (int i = 0; i < actual.numElements(); i += 1) {
      assertEquals(
          context + ".element",
          type,
          expected.isNullAt(i) ? null : expected.get(i, type),
          actual.isNullAt(i) ? null : actual.get(i, type));
    }
  }

  private static void assertEquals(String context, MapType map, MapData expected, MapData actual) {
    Assert.assertEquals(
        "Should have the same number of elements", expected.numElements(), actual.numElements());

    DataType keyType = map.keyType();
    ArrayData expectedKeys = expected.keyArray();
    ArrayData expectedValues = expected.valueArray();

    DataType valueType = map.valueType();
    ArrayData actualKeys = actual.keyArray();
    ArrayData actualValues = actual.valueArray();

    for (int i = 0; i < actual.numElements(); i += 1) {
      assertEquals(
          context + ".key",
          keyType,
          expectedKeys.isNullAt(i) ? null : expectedKeys.get(i, keyType),
          actualKeys.isNullAt(i) ? null : actualKeys.get(i, keyType));
      assertEquals(
          context + ".value",
          valueType,
          expectedValues.isNullAt(i) ? null : expectedValues.get(i, valueType),
          actualValues.isNullAt(i) ? null : actualValues.get(i, valueType));
    }
  }

  public static List<ManifestFile> dataManifests(Table table) {
    return table.currentSnapshot().dataManifests(table.io());
  }

  public static List<ManifestFile> deleteManifests(Table table) {
    return table.currentSnapshot().deleteManifests(table.io());
  }

  public static List<DataFile> dataFiles(Table table) {
    return dataFiles(table, null);
  }

  public static List<DataFile> dataFiles(Table table, String branch) {
    TableScan scan = table.newScan();
    if (branch != null) {
      scan.useRef(branch);
    }

    CloseableIterable<FileScanTask> tasks = scan.includeColumnStats().planFiles();
    return Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
  }

  public static Set<DeleteFile> deleteFiles(Table table) {
    Set<DeleteFile> deleteFiles = Sets.newHashSet();

    for (FileScanTask task : table.newScan().planFiles()) {
      deleteFiles.addAll(task.deletes());
    }

    return deleteFiles;
  }

  public static Set<String> reachableManifestPaths(Table table) {
    return StreamSupport.stream(table.snapshots().spliterator(), false)
        .flatMap(s -> s.allManifests(table.io()).stream())
        .map(ManifestFile::path)
        .collect(Collectors.toSet());
  }

  public static void asMetadataRecord(GenericData.Record file, FileContent content) {
    file.put(0, content.id());
    file.put(3, 0); // specId
  }

  public static void asMetadataRecord(GenericData.Record file) {
    file.put(0, FileContent.DATA.id());
    file.put(3, 0); // specId
  }

  public static Dataset<Row> selectNonDerived(Dataset<Row> metadataTable) {
    StructField[] fields = metadataTable.schema().fields();
    return metadataTable.select(
        Stream.of(fields)
            .filter(f -> !f.name().equals("readable_metrics")) // derived field
            .map(f -> new Column(f.name()))
            .toArray(Column[]::new));
  }

  public static Types.StructType nonDerivedSchema(Dataset<Row> metadataTable) {
    return SparkSchemaUtil.convert(TestHelpers.selectNonDerived(metadataTable).schema()).asStruct();
  }
}
