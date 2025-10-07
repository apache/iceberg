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
  private FileFormat format;

  @Parameters(name = " format = {0}")
  protected static List<FileFormat> parameters() {
    return Arrays.asList(FileFormat.AVRO, FileFormat.PARQUET);
  }

  private static final Schema SIMPLE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()));

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
    OutputFile outputFile = fileIO.newOutputFile(tempDir.resolve("test." + format).toString());

    List<Record> testData = RandomInternalData.generate(SIMPLE_SCHEMA, 1000, 1L);

    try (FileAppender<Record> appender =
        InternalData.write(format, outputFile).schema(SIMPLE_SCHEMA).build()) {
      appender.addAll(testData);
    }

    InputFile inputFile = fileIO.newInputFile(outputFile.location());
    List<PartitionData> readRecords = Lists.newArrayList();

    try (CloseableIterable<PartitionData> reader =
        InternalData.read(format, inputFile)
            .project(SIMPLE_SCHEMA)
            .setRootType(PartitionData.class)
            .build()) {
      for (PartitionData record : reader) {
        readRecords.add(record);
      }
    }

    assertThat(readRecords).hasSameSizeAs(testData);

    for (int i = 0; i < testData.size(); i++) {
      Record expected = testData.get(i);
      PartitionData actual = readRecords.get(i);

      assertThat(actual.get(0, Long.class)).isEqualTo(expected.get(0, Long.class));
      assertThat(actual.get(1, String.class)).isEqualTo(expected.get(1, String.class));
    }
  }

  @TestTemplate
  public void testCustomTypeForNestedField() throws IOException {
    OutputFile outputFile = fileIO.newOutputFile(tempDir.resolve("test." + format).toString());

    List<Record> testData = RandomInternalData.generate(NESTED_SCHEMA, 1000, 1L);

    try (FileAppender<Record> appender =
        InternalData.write(format, outputFile).schema(NESTED_SCHEMA).build()) {
      appender.addAll(testData);
    }

    InputFile inputFile = fileIO.newInputFile(outputFile.location());
    List<Record> readRecords = Lists.newArrayList();

    try (CloseableIterable<Record> reader =
        InternalData.read(format, inputFile)
            .project(NESTED_SCHEMA)
            .setCustomType(2, TestHelpers.CustomRow.class)
            .build()) {
      for (Record record : reader) {
        readRecords.add(record);
      }
    }

    assertThat(readRecords).hasSameSizeAs(testData);

    for (int i = 0; i < testData.size(); i++) {
      Record expected = testData.get(i);
      Record actual = readRecords.get(i);

      assertThat(actual.get(0, Long.class)).isEqualTo(expected.get(0, Long.class));

      Object expectedNested = expected.get(1);
      Object actualNested = actual.get(1);

      if (expectedNested == null) {
        // Expected nested struct is null, so actual should also be null
        assertThat(actualNested).isNull();
      } else {
        // Expected nested struct is not null, so actual should be a CustomRow
        assertThat(actualNested).isNotNull();
        assertThat(actualNested)
            .as("Custom type should be TestHelpers.CustomRow but was: " + actualNested.getClass())
            .isInstanceOf(TestHelpers.CustomRow.class);
        TestHelpers.CustomRow customRow = (TestHelpers.CustomRow) actualNested;
        Record expectedRecord = (Record) expectedNested;

        assertThat(customRow.get(0, Long.class))
            .isEqualTo(expectedRecord.get(0, Long.class)); // inner_id
        assertThat(customRow.get(1, String.class))
            .isEqualTo(expectedRecord.get(1, String.class)); // inner_name
      }
    }
  }
}
