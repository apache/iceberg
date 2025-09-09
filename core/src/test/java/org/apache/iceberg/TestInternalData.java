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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestInternalData {

  @Parameter(index = 0)
  private String format;

  @Parameters(name = "format={0}")
  public static Object[][] parameters() {
    return new Object[][] {{"avro"}, {"parquet"}};
  }

  private static final Schema NESTED_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "outer_id", Types.LongType.get()),
          Types.NestedField.optional(
              2,
              "nested_struct",
              Types.StructType.of(
                  Types.NestedField.optional(3, "inner_id", Types.LongType.get()),
                  Types.NestedField.optional(4, "inner_name", Types.StringType.get()))));

  @TempDir private Path tempDir;

  private final FileIO fileIO = new TestTables.LocalFileIO();

  @TestTemplate
  public void testCustomRootType() throws IOException {
    FileFormat fileFormat = FileFormat.fromString(format);
    OutputFile outputFile = fileIO.newOutputFile(tempDir.resolve("test." + format).toString());

    List<Record> testData = createSimpleTestRecords();

    try (FileAppender<Record> appender =
        InternalData.write(fileFormat, outputFile).schema(simpleSchema()).build()) {
      appender.addAll(testData);
    }

    InputFile inputFile = fileIO.newInputFile(outputFile.location());
    List<PartitionData> readRecords = Lists.newArrayList();

    try (CloseableIterable<PartitionData> reader =
        InternalData.read(fileFormat, inputFile)
            .project(simpleSchema())
            .setRootType(PartitionData.class)
            .build()) {
      for (PartitionData record : reader) {
        readRecords.add(record);
      }
    }

    assertThat(readRecords).hasSize(testData.size());

    for (int i = 0; i < testData.size(); i++) {
      Record expected = testData.get(i);
      PartitionData actual = readRecords.get(i);

      assertThat(actual.get(0, Long.class)).isEqualTo(expected.get(0, Long.class));
      assertThat(actual.get(1, String.class)).isEqualTo(expected.get(1, String.class));
    }
  }

  @TestTemplate
  public void testCustomTypeForNestedField() throws IOException {
    FileFormat fileFormat = FileFormat.fromString(format);
    OutputFile outputFile = fileIO.newOutputFile(tempDir.resolve("test." + format).toString());

    List<Record> testData = createNestedTestRecords();

    try (FileAppender<Record> appender =
        InternalData.write(fileFormat, outputFile).schema(NESTED_SCHEMA).build()) {
      appender.addAll(testData);
    }

    InputFile inputFile = fileIO.newInputFile(outputFile.location());
    List<Record> readRecords = Lists.newArrayList();

    try (CloseableIterable<Record> reader =
        InternalData.read(fileFormat, inputFile)
            .project(NESTED_SCHEMA)
            .setCustomType(2, TestCustomRow.class)
            .build()) {
      for (Record record : reader) {
        readRecords.add(record);
      }
    }

    assertThat(readRecords).hasSize(testData.size());

    for (int i = 0; i < testData.size(); i++) {
      Record expected = testData.get(i);
      Record actual = readRecords.get(i);

      assertThat(actual.get(0, Long.class)).isEqualTo(expected.get(0, Long.class));

      Object expectedNested = expected.get(1);
      Object actualNested = actual.get(1);

      if (expectedNested == null && actualNested == null) {
        continue;
      }

      if (actualNested != null) {
        assertThat(actualNested)
            .as("Custom type should be TestCustomRow, but was: " + actualNested.getClass())
            .isInstanceOf(TestCustomRow.class);
        TestCustomRow customRow = (TestCustomRow) actualNested;
        Record expectedRecord = (Record) expectedNested;

        assertThat(customRow.get(0, Long.class))
            .isEqualTo(expectedRecord.get(0, Long.class)); // inner_id
        assertThat(customRow.get(1, String.class))
            .isEqualTo(expectedRecord.get(1, String.class)); // inner_name
      }
    }
  }


  private List<Record> createSimpleTestRecords() {
    Schema schema = simpleSchema();
    List<Record> records = Lists.newArrayList();

    Record record1 = GenericRecord.create(schema);
    record1.set(0, 1L);
    record1.set(1, "Alice");

    Record record2 = GenericRecord.create(schema);
    record2.set(0, 2L);
    record2.set(1, "Bob");

    records.add(record1);
    records.add(record2);

    return records;
  }

  private List<Record> createNestedTestRecords() {
    List<Record> records = Lists.newArrayList();

    Record record1 = GenericRecord.create(NESTED_SCHEMA);
    record1.set(0, 1L);

    Record nestedStruct1 =
        GenericRecord.create(NESTED_SCHEMA.findType("nested_struct").asStructType());
    nestedStruct1.set(0, 100L);
    nestedStruct1.set(1, "inner_alice");
    record1.set(1, nestedStruct1);

    Record record2 = GenericRecord.create(NESTED_SCHEMA);
    record2.set(0, 2L);
    record2.set(1, null); // null nested struct

    records.add(record1);
    records.add(record2);

    return records;
  }

  private Schema simpleSchema() {
    return new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()));
  }

  public static class TestCustomRow implements StructLike {
    private Object[] values;

    public TestCustomRow() {
      this.values = new Object[0];
    }

    public TestCustomRow(Types.StructType structType) {
      this.values = new Object[structType.fields().size()];
    }


    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      values[pos] = value;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other == null || getClass() != other.getClass()) {
        return false;
      }

      TestCustomRow that = (TestCustomRow) other;
      return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }
  }
}
