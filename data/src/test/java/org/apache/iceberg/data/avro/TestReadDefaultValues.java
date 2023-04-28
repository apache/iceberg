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
package org.apache.iceberg.data.avro;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
  public void writeAndValidate() throws IOException {
    for (Object[] typeWithDefault : typesWithDefaults) {
      Type type = (Type) typeWithDefault[0];
      String defaultValueJson = (String) typeWithDefault[1];
      Object defaultValue = SingleValueParser.fromJson(type, defaultValueJson);

      Schema writerSchema = new Schema(required(999, "col1", Types.IntegerType.get()));

      Schema readerSchema =
          new Schema(
              required(999, "col1", Types.IntegerType.get()),
              NestedFieldWithInitialDefault.optionalWithDefault(
                  1000, "col2", type, null, defaultValue, defaultValue));

      List<Record> generatedRecords = RandomGenericData.generate(writerSchema, 100, 0L);
      List<Record> expected = Lists.newArrayList();
      for (Record record : generatedRecords) {
        Record expectedRecord = GenericRecord.create(readerSchema);
        expectedRecord.set(0, record.get(0));
        expectedRecord.set(1, defaultValue);
        expected.add(expectedRecord);
      }

      File testFile = temp.newFile();
      Assert.assertTrue("Delete should succeed", testFile.delete());

      try (FileAppender<Record> writer =
          Avro.write(Files.localOutput(testFile))
              .schema(writerSchema)
              .createWriterFunc(DataWriter::create)
              .named("test")
              .build()) {
        for (Record rec : generatedRecords) {
          writer.add(rec);
        }
      }

      List<Record> rows;
      try (AvroIterable<Record> reader =
          Avro.read(Files.localInput(testFile))
              .project(readerSchema)
              .createReaderFunc(DataReader::create)
              .build()) {
        rows = Lists.newArrayList(reader);
      }

      for (int i = 0; i < expected.size(); i += 1) {
        DataTestHelpers.assertEquals(readerSchema.asStruct(), expected.get(i), rows.get(i));
      }
    }
  }

  // TODO: This class should be removed once NestedField.optional() that takes initialDefault and
  // writeDefault becomes public. It is intentionally package private to avoid exposing it in the
  // public API as of now.
  static class NestedFieldWithInitialDefault extends Types.NestedField {
    private final Object initialDefault;

    NestedFieldWithInitialDefault(
        boolean isOptional,
        int fieldId,
        String name,
        Type type,
        String doc,
        Object initialDefault,
        Object writeDefault) {
      super(isOptional, fieldId, name, type, doc, initialDefault, writeDefault);
      this.initialDefault = initialDefault;
    }

    static Types.NestedField optionalWithDefault(
        int id, String name, Type type, String doc, Object initialDefault, Object writeDefault) {
      return new NestedFieldWithInitialDefault(
          true, id, name, type, doc, initialDefault, writeDefault);
    }

    @Override
    public Object initialDefault() {
      return initialDefault;
    }
  }
}
