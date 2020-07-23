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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.flink.data.RandomData.COMPLEX_SCHEMA;

public class TestFlinkParquetReaderWriter {
  private static final int NUM_RECORDS = 20_000;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private void testCorrectness(Schema schema, int numRecords, Iterable<RowData> iterable) throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<RowData> writer = Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(FlinkSchemaUtil.convert(schema), msgType))
        .build()) {
      writer.addAll(iterable);
    }

    try (CloseableIterable<RowData> reader = Parquet.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(type -> FlinkParquetReaders.buildReader(schema, type))
        .build()) {
      Iterator<RowData> expected = iterable.iterator();
      Iterator<RowData> rows = reader.iterator();
      for (int i = 0; i < numRecords; i += 1) {
        Assert.assertTrue("Should have expected number of rows", rows.hasNext());
        assertRowData(schema.asStruct(), expected.next(), rows.next());
      }
      Assert.assertFalse("Should not have extra rows", rows.hasNext());
    }
  }

  private void assertRowData(Type type, RowData expected, RowData actual) {
    List<Type> types = new ArrayList<>();
    for (Types.NestedField field : type.asStructType().fields()) {
      types.add(field.type());
    }

    for (int i = 0; i < types.size(); i += 1) {
      if (expected.isNullAt(i)) {
        Assert.assertEquals(expected.isNullAt(i), actual.isNullAt(i));
        continue;
      }
      switch (types.get(i).typeId()) {
        case BOOLEAN:
          Assert.assertEquals("boolean value should be equal", expected.getBoolean(i), actual.getBoolean(i));
          break;
        case INTEGER:
          Assert.assertEquals("int value should be equal", expected.getInt(i), actual.getInt(i));
          break;
        case LONG:
          Assert.assertEquals("long value should be equal", expected.getLong(i), actual.getLong(i));
          break;
        case FLOAT:
          Assert.assertEquals("float value should be equal", Float.valueOf(expected.getFloat(i)),
              Float.valueOf(actual.getFloat(i)));
          break;
        case DOUBLE:
          Assert.assertEquals("double should be equal", Double.valueOf(expected.getDouble(i)),
              Double.valueOf(actual.getDouble(i)));
          break;
        case DATE:
          Assert.assertEquals("date should be equal", expected.getInt(i), expected.getInt(i));
          break;
        case TIME:
          Assert.assertEquals("time should be equal", expected.getInt(i), expected.getInt(i));
          break;
        case TIMESTAMP:
          Assert.assertEquals("timestamp should be equal", expected.getTimestamp(i, 6),
              actual.getTimestamp(i, 6));
          break;
        case UUID:
        case FIXED:
        case BINARY:
          Assert.assertArrayEquals("binary should be equal", expected.getBinary(i), actual.getBinary(i));
          break;
        case DECIMAL:
          Types.DecimalType decimal = (Types.DecimalType) types.get(i);
          int precision = decimal.precision();
          int scale = decimal.scale();
          Assert.assertEquals("uuid should be equal",
              expected.getDecimal(i, precision, scale),
              actual.getDecimal(i, precision, scale));
          break;
        case LIST:
          ArrayData arrayData1 = expected.getArray(i);
          ArrayData arrayData2 = actual.getArray(i);
          Assert.assertEquals("array length should be equal", arrayData1.size(), arrayData2.size());
          for (int j = 0; j < arrayData1.size(); j += 1) {
            assertArrayValues(types.get(i).asListType().elementType(), arrayData1, arrayData2);
          }
          break;
        case MAP:
          ArrayData keyArrayData1 = expected.getMap(i).keyArray();
          ArrayData valueArrayData1 = expected.getMap(i).valueArray();
          ArrayData keyArrayData2 = actual.getMap(i).keyArray();
          ArrayData valueArrayData2 = actual.getMap(i).valueArray();
          Type keyType = types.get(i).asMapType().keyType();
          Type valueType = types.get(i).asMapType().valueType();

          Assert.assertEquals("map size should be equal", expected.getMap(i).size(), actual.getMap(i).size());

          for (int j = 0; j < keyArrayData1.size(); j += 1) {
            assertArrayValues(keyType, keyArrayData1, keyArrayData2);
            assertArrayValues(valueType, valueArrayData1, valueArrayData2);
          }
          break;
        case STRUCT:
          int numFields = types.get(i).asStructType().fields().size();
          assertRowData(types.get(i).asStructType(), expected.getRow(i, numFields), actual.getRow(i, numFields));
          break;
      }
    }
  }

  private void assertArrayValues(Type type, ArrayData expected, ArrayData actual) {
    for (int i = 0; i < expected.size(); i += 1) {
      if (expected.isNullAt(i)) {
        Assert.assertEquals(expected.isNullAt(i), actual.isNullAt(i));
        continue;
      }
      switch (type.typeId()) {
        case BOOLEAN:
          Assert.assertEquals("boolean value should be equal", expected.getBoolean(i), actual.getBoolean(i));
          break;
        case INTEGER:
          Assert.assertEquals("int value should be equal", expected.getInt(i), actual.getInt(i));
          break;
        case LONG:
          Assert.assertEquals("long value should be equal", expected.getLong(i), actual.getLong(i));
          break;
        case FLOAT:
          Assert.assertEquals("float value should be equal", Float.valueOf(expected.getFloat(i)),
              Float.valueOf(actual.getFloat(i)));
          break;
        case DOUBLE:
          Assert.assertEquals("double should be equal", Double.valueOf(expected.getDouble(i)),
              Double.valueOf(actual.getDouble(i)));
          break;
        case DATE:
          Assert.assertEquals("date should be equal", expected.getInt(i), expected.getInt(i));
          break;
        case TIME:
          Assert.assertEquals("time should be equal", expected.getInt(i), expected.getInt(i));
          break;
        case TIMESTAMP:
          Assert.assertEquals("timestamp should be equal", expected.getTimestamp(i, 6),
              actual.getTimestamp(i, 6));
          break;
        case UUID:
        case FIXED:
        case BINARY:
          Assert.assertArrayEquals("binary should be equal", expected.getBinary(i), actual.getBinary(i));
          break;
        case DECIMAL:
          Types.DecimalType decimal = (Types.DecimalType) type;
          int precision = decimal.precision();
          int scale = decimal.scale();
          Assert.assertEquals("uuid should be equal",
              expected.getDecimal(i, precision, scale),
              actual.getDecimal(i, precision, scale));
          break;
        case LIST:
          ArrayData arrayData1 = expected.getArray(i);
          ArrayData arrayData2 = actual.getArray(i);
          Assert.assertEquals("array length should be equal", arrayData1.size(), arrayData2.size());
          for (int j = 0; j < arrayData1.size(); j += 1) {
            assertArrayValues(type.asListType().elementType(), arrayData1, arrayData2);
          }
          break;
        case MAP:
          ArrayData keyArrayData1 = expected.getMap(i).keyArray();
          ArrayData valueArrayData1 = expected.getMap(i).valueArray();
          ArrayData keyArrayData2 = actual.getMap(i).keyArray();
          ArrayData valueArrayData2 = actual.getMap(i).valueArray();

          Assert.assertEquals("map size should be equal", expected.getMap(i).size(), actual.getMap(i).size());

          for (int j = 0; j < keyArrayData1.size(); j += 1) {
            assertArrayValues(type.asMapType().keyType(), keyArrayData1, keyArrayData2);
            assertArrayValues(type.asMapType().valueType(), valueArrayData1, valueArrayData2);
          }
          break;
        case STRUCT:
          int numFields = type.asStructType().fields().size();
          assertRowData(type.asStructType(), expected.getRow(i, numFields), actual.getRow(i, numFields));
          break;
      }
    }
  }

  @Test
  public void testNormalRowData() throws IOException {
    testCorrectness(COMPLEX_SCHEMA, NUM_RECORDS, RandomData.generateRowData(COMPLEX_SCHEMA, NUM_RECORDS, 19981));
  }

  @Test
  public void testDictionaryEncodedData() throws IOException {
    testCorrectness(COMPLEX_SCHEMA, NUM_RECORDS,
        RandomData.generateDictionaryEncodableData(COMPLEX_SCHEMA, NUM_RECORDS, 21124));
  }

  @Test
  public void testFallbackData() throws IOException {
    testCorrectness(COMPLEX_SCHEMA, NUM_RECORDS,
        RandomData.generateFallbackData(COMPLEX_SCHEMA, NUM_RECORDS, 21124, NUM_RECORDS / 20));
  }
}
