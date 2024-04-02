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
package org.apache.iceberg.avro;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestReadDefaultValues {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static final Object[][] typesWithDefaults =
      new Object[][] {
        {Types.BooleanType.get(), "true"},
        {Types.IntegerType.get(), "1"},
        {Types.LongType.get(), "9999999"},
        {Types.FloatType.get(), "1.23"},
        {Types.DoubleType.get(), "123.456"},
        {Types.DateType.get(), "\"2007-12-03\""},
        {Types.TimeType.get(), "\"10:15:30\""},
        {Types.TimestampType.withoutZone(), "\"2007-12-03T10:15:30\""},
        {Types.TimestampType.withZone(), "\"2007-12-03T10:15:30+00:00\""},
        {Types.StringType.get(), "\"foo\""},
        {Types.UUIDType.get(), "\"eb26bdb1-a1d8-4aa6-990e-da940875492c\""},
        {Types.FixedType.ofLength(2), "\"111f\""},
        {Types.BinaryType.get(), "\"0000ff\""},
        {Types.DecimalType.of(9, 4), "\"123.4500\""},
        {Types.DecimalType.of(9, 0), "\"2\""},
        // Avro doesn't support negative scale
        // {Types.DecimalType.of(9, -20), "\"2E+20\""},
        {Types.ListType.ofOptional(1, Types.IntegerType.get()), "[1, 2, 3]"},
        {
          Types.MapType.ofOptional(2, 3, Types.IntegerType.get(), Types.StringType.get()),
          "{\"keys\": [1, 2], \"values\": [\"foo\", \"bar\"]}"
        },
        {
          Types.StructType.of(
              required(4, "f1", Types.IntegerType.get()),
              optional(5, "f2", Types.StringType.get())),
          "{\"4\": 1, \"5\": \"bar\"}"
        },
        // deeply nested complex types
        {
          Types.ListType.ofOptional(
              6,
              Types.StructType.of(
                  required(7, "f1", Types.IntegerType.get()),
                  optional(8, "f2", Types.StringType.get()))),
          "[{\"7\": 1, \"8\": \"bar\"}, {\"7\": 2, \"8\": " + "\"foo\"}]"
        },
        {
          Types.MapType.ofOptional(
              9,
              10,
              Types.IntegerType.get(),
              Types.StructType.of(
                  required(11, "f1", Types.IntegerType.get()),
                  optional(12, "f2", Types.StringType.get()))),
          "{\"keys\": [1, 2], \"values\": [{\"11\": 1, \"12\": \"bar\"}, {\"11\": 2, \"12\": \"foo\"}]}"
        },
        {
          Types.StructType.of(
              required(
                  13,
                  "f1",
                  Types.StructType.of(
                      optional(14, "ff1", Types.IntegerType.get()),
                      optional(15, "ff2", Types.StringType.get()))),
              optional(
                  16,
                  "f2",
                  Types.StructType.of(
                      optional(17, "ff1", Types.StringType.get()),
                      optional(18, "ff2", Types.IntegerType.get())))),
          "{\"13\": {\"14\": 1, \"15\": \"bar\"}, \"16\": {\"17\": \"bar\", \"18\": 1}}"
        },
      };

  @Test
  public void testDefaultValueApplied() throws IOException {
    for (Object[] typeWithDefault : typesWithDefaults) {
      Type type = (Type) typeWithDefault[0];
      String defaultValueJson = (String) typeWithDefault[1];
      Object defaultValue = SingleValueParser.fromJson(type, defaultValueJson);

      Schema writerSchema = new Schema(required(999, "col1", Types.IntegerType.get()));

      Schema readerSchema =
          new Schema(
              required(999, "col1", Types.IntegerType.get()),
              Types.NestedField.optional(1000, "col2", type, null, defaultValue, defaultValue));

      Record expectedRecord = new Record(AvroSchemaUtil.convert(readerSchema.asStruct()));
      expectedRecord.put(0, 1);
      expectedRecord.put(1, toGenericRecord(type, defaultValue));

      File testFile = temp.newFile();
      Assert.assertTrue("Delete should succeed", testFile.delete());

      try (FileAppender<Record> writer =
          Avro.write(Files.localOutput(testFile))
              .schema(writerSchema)
              .createWriterFunc(GenericAvroWriter::create)
              .named("test")
              .build()) {
        Record record = new Record(AvroSchemaUtil.convert(writerSchema.asStruct()));
        record.put(0, 1);
        writer.add(record);
      }

      List<Record> rows;
      try (AvroIterable<Record> reader =
          Avro.read(Files.localInput(testFile))
              .project(readerSchema)
              .createResolvingReader(schema -> GenericAvroReader.create(schema))
              .build()) {
        rows = Lists.newArrayList(reader);
      }

      AvroTestHelpers.assertEquals(readerSchema.asStruct(), expectedRecord, rows.get(0));
    }
  }

  @Test
  public void testDefaultValueNotApplied() throws IOException {
    for (Object[] typeWithDefault : typesWithDefaults) {
      Type type = (Type) typeWithDefault[0];
      String defaultValueJson = (String) typeWithDefault[1];
      Object defaultValue = SingleValueParser.fromJson(type, defaultValueJson);

      Schema readerSchema =
          new Schema(
              required(999, "col1", Types.IntegerType.get()),
              Types.NestedField.optional(1000, "col2", type, null, defaultValue, defaultValue));

      // Create a record with null value for the column with default value
      Record expectedRecord = new Record(AvroSchemaUtil.convert(readerSchema.asStruct()));
      expectedRecord.put(0, 1);
      expectedRecord.put(1, null);

      File testFile = temp.newFile();
      Assert.assertTrue("Delete should succeed", testFile.delete());

      try (FileAppender<Record> writer =
          Avro.write(Files.localOutput(testFile))
              .schema(readerSchema)
              .createWriterFunc(GenericAvroWriter::create)
              .named("test")
              .build()) {
        writer.add(expectedRecord);
      }

      List<Record> rows;
      try (AvroIterable<Record> reader =
          Avro.read(Files.localInput(testFile))
              .project(readerSchema)
              .createReaderFunc(schema -> GenericAvroReader.create(schema))
              .build()) {
        rows = Lists.newArrayList(reader);
      }

      // Existence of default value should not affect the read result
      AvroTestHelpers.assertEquals(readerSchema.asStruct(), expectedRecord, rows.get(0));
    }
  }

  // TODO: Merge the conversion mechanism with the end state of
  // ValueReaders.ConstantReader.toGenericRecord().
  private Object toGenericRecord(Type type, Object data) {
    // Recursively convert data to GenericRecord if type is a StructType.
    if (type instanceof Types.StructType) {
      Types.StructType structType = (Types.StructType) type;
      Record genericRecord = new Record(AvroSchemaUtil.convert(type));
      int index = 0;
      for (Types.NestedField field : structType.fields()) {
        genericRecord.put(
            field.name(), toGenericRecord(field.type(), ((GenericRecord) data).get(index)));
        index++;
      }
      return genericRecord;
    } else if (type instanceof Types.MapType) {
      Types.MapType mapType = (Types.MapType) type;
      Map<Object, Object> genericMap =
          Maps.newHashMapWithExpectedSize(((Map<Object, Object>) data).size());
      for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) data).entrySet()) {
        genericMap.put(
            toGenericRecord(mapType.keyType(), entry.getKey()),
            toGenericRecord(mapType.valueType(), entry.getValue()));
      }
      return genericMap;
    } else if (type instanceof Types.ListType) {
      Types.ListType listType = (Types.ListType) type;
      List<Object> genericList = Lists.newArrayListWithExpectedSize(((List<Object>) data).size());
      for (Object element : (List<Object>) data) {
        genericList.add(toGenericRecord(listType.elementType(), element));
      }
      return genericList;
    } else {
      return data;
    }
  }
}
