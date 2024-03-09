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
package org.apache.iceberg.orc;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcConf;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestOrcDataReader implements WithAssertions {

  @TempDir private static File temp;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "binary", Types.BinaryType.get()),
          Types.NestedField.required(
              4, "array", Types.ListType.ofOptional(5, Types.IntegerType.get())));
  private static DataFile dataFile;
  private static OutputFile outputFile;

  @BeforeAll
  public static void createDataFile() throws IOException {
    GenericRecord bufferRecord = GenericRecord.create(SCHEMA);

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    builder.add(
        bufferRecord.copy(
            ImmutableMap.of("id", 1L, "data", "a", "array", Collections.singletonList(1))));
    builder.add(
        bufferRecord.copy(ImmutableMap.of("id", 2L, "data", "b", "array", Arrays.asList(2, 3))));
    builder.add(
        bufferRecord.copy(ImmutableMap.of("id", 3L, "data", "c", "array", Arrays.asList(3, 4, 5))));
    builder.add(
        bufferRecord.copy(
            ImmutableMap.of("id", 4L, "data", "d", "array", Arrays.asList(4, 5, 6, 7))));
    builder.add(
        bufferRecord.copy(
            ImmutableMap.of("id", 5L, "data", "e", "array", Arrays.asList(5, 6, 7, 8, 9))));

    outputFile = Files.localOutput(File.createTempFile("test", ".orc", new File(temp.getPath())));

    DataWriter<Record> dataWriter =
        ORC.writeData(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();

    try {
      for (Record record : builder.build()) {
        dataWriter.write(record);
      }
    } finally {
      dataWriter.close();
    }

    dataFile = dataWriter.toDataFile();
  }

  @Test
  public void testWrite() {
    assertThat(dataFile.format()).isEqualTo(FileFormat.ORC);
    assertThat(dataFile.content()).isEqualTo(FileContent.DATA);
    assertThat(dataFile.recordCount()).isEqualTo(5);
    assertThat(dataFile.partition().size()).isEqualTo(0);
    assertThat(dataFile.keyMetadata()).isNull();
  }

  private void validateAllRecords(List<Record> records) {
    assertThat(records).hasSize(5);
    long id = 1;
    char data = 'a';
    for (Record record : records) {
      assertThat(record.getField("id")).isEqualTo(id);
      id++;
      assertThat((String) record.getField("data")).isEqualTo(Character.toString(data));
      data++;
      assertThat(record.getField("binary")).isNull();
    }
  }

  @Test
  public void testRowReader() throws IOException {
    try (CloseableIterable<Record> reader =
        ORC.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .filter(Expressions.and(Expressions.notNull("data"), Expressions.equal("id", 3L)))
            .build()) {
      validateAllRecords(Lists.newArrayList(reader));
    }
  }

  @Test
  public void testRowReaderWithFilter() throws IOException {
    try (CloseableIterable<Record> reader =
        ORC.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .filter(Expressions.and(Expressions.notNull("data"), Expressions.equal("id", 3L)))
            .config(OrcConf.ALLOW_SARG_TO_FILTER.name(), String.valueOf(true))
            .build()) {
      validateAllRecords(Lists.newArrayList(reader));
    }
  }

  @Test
  public void testRowReaderWithFilterWithSelected() throws IOException {
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        ORC.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .filter(Expressions.and(Expressions.notNull("data"), Expressions.equal("id", 3L)))
            .config(OrcConf.ALLOW_SARG_TO_FILTER.getAttribute(), String.valueOf(true))
            .config(OrcConf.READER_USE_SELECTED.getAttribute(), String.valueOf(true))
            .build()) {
      readRecords = Lists.newArrayList(reader);
    }

    assertThat(readRecords).hasSize(1);
    assertThat(readRecords.get(0).get(0)).isEqualTo(3L);
    assertThat(readRecords.get(0).get(1)).isEqualTo("c");
    assertThat(readRecords.get(0).get(3)).isEqualTo(Arrays.asList(3, 4, 5));
  }
}
