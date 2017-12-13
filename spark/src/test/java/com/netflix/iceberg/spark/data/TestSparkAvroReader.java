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
import com.google.common.collect.Maps;
import com.netflix.iceberg.Files;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.avro.AvroIterable;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.spark.SparkSchemaUtil;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.types.Types.ListType;
import com.netflix.iceberg.types.Types.LongType;
import com.netflix.iceberg.types.Types.MapType;
import com.netflix.iceberg.types.Types.StructType;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.netflix.iceberg.avro.AvroSchemaUtil.convert;
import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;

public class TestSparkAvroReader {
  private static final StructType SUPPORTED_PRIMITIVES = StructType.of(
      required(0, "id", LongType.get()),
      optional(1, "data", Types.StringType.get()),
      required(2, "b", Types.BooleanType.get()),
      optional(3, "i", Types.IntegerType.get()),
      required(4, "l", LongType.get()),
      optional(5, "f", Types.FloatType.get()),
      required(6, "d", Types.DoubleType.get()),
      optional(7, "date", Types.DateType.get()),
      required(8, "ts", Types.TimestampType.withZone()),
      required(10, "s", Types.StringType.get()),
      required(11, "uuid", Types.UUIDType.get()),
      required(12, "fixed", Types.FixedType.ofLength(7)),
      optional(13, "bytes", Types.BinaryType.get()),
      required(14, "dec_9_0", Types.DecimalType.of(9, 0)),
      required(15, "dec_9_2", Types.DecimalType.of(9, 2)),
      required(16, "dec_38_10", Types.DecimalType.of(38, 10)) // spark's maximum precision
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testSimpleStruct() throws IOException {
    writeAndValidate(new Schema(SUPPORTED_PRIMITIVES.fields()));
  }

  @Test
  public void testArray() throws IOException {
    Schema schema = new Schema(
        required(0, "id", LongType.get()),
        optional(1, "data", ListType.ofOptional(2, Types.StringType.get())));

    writeAndValidate(schema);
  }

  @Test
  public void testArrayOfStructs() throws IOException {
    Schema schema = new Schema(
        required(0, "id", LongType.get()),
        optional(1, "data", ListType.ofOptional(2, SUPPORTED_PRIMITIVES)));

    writeAndValidate(schema);
  }

  @Test
  public void testMap() throws IOException {
    Schema schema = new Schema(
        required(0, "id", LongType.get()),
        optional(1, "data", MapType.ofOptional(2, 3, Types.StringType.get())));

    writeAndValidate(schema);
  }

  @Test
  public void testMapOfStructs() throws IOException {
    Schema schema = new Schema(
        required(0, "id", LongType.get()),
        optional(1, "data", MapType.ofOptional(2, 3, SUPPORTED_PRIMITIVES)));

    writeAndValidate(schema);
  }

  @Test
  public void testMixedTypes() throws IOException {
    Schema schema = new Schema(
        required(0, "id", LongType.get()),
        optional(1, "list_of_maps",
            ListType.ofOptional(2, MapType.ofOptional(3, 4, SUPPORTED_PRIMITIVES))),
        optional(5, "map_of_lists",
            MapType.ofOptional(6, 7, ListType.ofOptional(8, SUPPORTED_PRIMITIVES))),
        required(9, "array_of_arrays",
            ListType.ofOptional(10, ListType.ofOptional(11, SUPPORTED_PRIMITIVES))),
        required(12, "map_of_maps",
            MapType.ofOptional(13, 14, MapType.ofOptional(15, 16, SUPPORTED_PRIMITIVES))),
        required(17, "array_of_struct_of_nested_types", ListType.ofOptional(19, StructType.of(
            Types.NestedField.required(20, "m1", MapType.ofOptional(21, 22, SUPPORTED_PRIMITIVES)),
            Types.NestedField.optional(23, "a1", ListType.ofRequired(24, SUPPORTED_PRIMITIVES)),
            Types.NestedField.required(25, "a2", ListType.ofRequired(26, SUPPORTED_PRIMITIVES)),
            Types.NestedField.optional(27, "m2", MapType.ofOptional(28, 29, SUPPORTED_PRIMITIVES))
        )))
    );

    writeAndValidate(schema);
  }

  private void writeAndValidate(Schema schema) throws IOException {
    List<Record> expected = RandomData.generate(schema, 100, 0L);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer = Avro.write(Files.localOutput(testFile))
        .schema(schema)
        .named("test")
        .build()) {
      for (Record rec : expected) {
        writer.add(rec);
      }
    }

    List<InternalRow> rows;
    try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
        .createReaderFunc(SparkAvroReader::new)
        .project(schema)
        .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      assertEquals(schema.asStruct(), expected.get(i), rows.get(i));
    }
  }

  private void assertEquals(StructType struct, Record rec, InternalRow row) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Type fieldType = fields.get(i).type();

      Object expectedValue = rec.get(i);
      Object actualValue = row.get(i, SparkSchemaUtil.convert(fieldType));

      assertEquals(fieldType, expectedValue, actualValue);
    }
  }

  private void assertEquals(ListType list, Collection<?> expected, ArrayData actual) {
    Type elementType = list.elementType();
    List<?> expectedElements = Lists.newArrayList(expected);
    for (int i = 0; i < expectedElements.size(); i += 1) {
      Object expectedValue = expectedElements.get(i);
      Object actualValue = actual.get(i, SparkSchemaUtil.convert(elementType));

      assertEquals(elementType, expectedValue, actualValue);
    }
  }

  private void assertEquals(MapType map, Map<?, ?> expected, ArrayBasedMapData actual) {
    Type keyType = map.keyType();
    Type valueType = map.valueType();

    List<Map.Entry<?, ?>> expectedElements = Lists.newArrayList(expected.entrySet());
    ArrayData actualKeys = actual.keyArray();
    ArrayData actualValues = actual.valueArray();

    for (int i = 0; i < expectedElements.size(); i += 1) {
      Map.Entry<?, ?> expectedPair = expectedElements.get(i);
      Object actualKey = actualKeys.get(i, SparkSchemaUtil.convert(keyType));
      Object actualValue = actualValues.get(i, SparkSchemaUtil.convert(keyType));

      assertEquals(keyType, expectedPair.getKey(), actualKey);
      assertEquals(valueType, expectedPair.getValue(), actualValue);
    }
  }

  private void assertEquals(Type type, Object expected, Object actual) {
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
        assertEquals(type.asNestedType().asStructType(), (Record) expected, (InternalRow) actual);
        break;
      case LIST:
        Assert.assertTrue("Should expect a Collection", expected instanceof Collection);
        Assert.assertTrue("Should be an InternalRow", actual instanceof ArrayData);
        assertEquals(type.asNestedType().asListType(), (Collection) expected, (ArrayData) actual);
        break;
      case MAP:
        Assert.assertTrue("Should expect a Collection", expected instanceof Map);
        Assert.assertTrue("Should be an InternalRow", actual instanceof ArrayBasedMapData);
        assertEquals(type.asNestedType().asMapType(), (Map) expected, (ArrayBasedMapData) actual);
        break;
      case TIME:
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }

  static <K, V> Map<K, V> map(K key, V value) {
    Map<K, V> map = Maps.newHashMap();
    map.put(key, value);
    return map;
  }

  static Record record(Schema schema, Object... data) {
    Record rec = new Record(convert(schema, "test"));
    for (int i = 0; i < data.length; i += 1) {
      rec.put(i, data[i]);
    }
    return rec;
  }
}
