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
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
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

  @Test
  public void testTimestampNanoFilterPushdownRespectsNanoseconds() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "ts", Types.TimestampNanoType.withoutZone()));

    // Five rows; ids 0..2 share the same microsecond, distinguished only by nanoseconds.
    GenericRecord template = GenericRecord.create(schema);
    List<Record> writeRecords =
        Lists.newArrayList(
            template.copy(
                ImmutableMap.of(
                    "id", 0L, "ts", LocalDateTime.parse("2024-01-01T00:00:00.000000000"))),
            template.copy(
                ImmutableMap.of(
                    "id", 1L, "ts", LocalDateTime.parse("2024-01-01T00:00:00.000000250"))),
            template.copy(
                ImmutableMap.of(
                    "id", 2L, "ts", LocalDateTime.parse("2024-01-01T00:00:00.000000750"))),
            template.copy(
                ImmutableMap.of(
                    "id", 3L, "ts", LocalDateTime.parse("2024-01-01T00:00:00.000001500"))),
            template.copy(
                ImmutableMap.of(
                    "id", 4L, "ts", LocalDateTime.parse("2024-01-01T00:00:00.000003000"))));

    OutputFile nanoFile =
        Files.localOutput(File.createTempFile("ts-nano", ".orc", new File(temp.getPath())));
    DataWriter<Record> nanoWriter =
        ORC.writeData(nanoFile)
            .schema(schema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try {
      for (Record record : writeRecords) {
        nanoWriter.write(record);
      }
    } finally {
      nanoWriter.close();
    }

    // Filter at a sub-microsecond boundary (>= 500 ns within the first microsecond). If ORC honors
    // nanosecond precision, only ids 2 (750 ns), 3 (1500 ns), 4 (3000 ns) remain. If it truncates
    // to micros, ids 0/1/2 are indistinguishable and the result is wrong.
    try (CloseableIterable<Record> reader =
        ORC.read(nanoFile.toInputFile())
            .project(schema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
            .filter(Expressions.greaterThanOrEqual("ts", "2024-01-01T00:00:00.000000500"))
            .config(OrcConf.ALLOW_SARG_TO_FILTER.getAttribute(), String.valueOf(true))
            .config(OrcConf.READER_USE_SELECTED.getAttribute(), String.valueOf(true))
            .build()) {
      List<Long> ids = Lists.newArrayList();
      for (Record record : reader) {
        ids.add((Long) record.getField("id"));
      }
      assertThat(ids).containsExactly(2L, 3L, 4L);
    }
  }

  @Test
  public void testTimestampTzNanoFilterAcrossTimezones() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "tsTz", Types.TimestampNanoType.withZone()));

    // Each row is written in a DIFFERENT zone offset but the instants share the same UTC second and
    // differ only by nanoseconds: id0 +0ns, id1 +500ns, id2 +750ns, id3 +1500ns, id4 +3000ns.
    GenericRecord template = GenericRecord.create(schema);
    List<Record> writeRecords =
        Lists.newArrayList(
            template.copy(
                ImmutableMap.of(
                    "id", 0L, "tsTz", OffsetDateTime.parse("2024-01-01T00:00:00.000000000+00:00"))),
            template.copy(
                ImmutableMap.of(
                    "id", 1L, "tsTz", OffsetDateTime.parse("2024-01-01T05:00:00.000000500+05:00"))),
            template.copy(
                ImmutableMap.of(
                    "id", 2L, "tsTz", OffsetDateTime.parse("2023-12-31T16:00:00.000000750-08:00"))),
            template.copy(
                ImmutableMap.of(
                    "id", 3L, "tsTz", OffsetDateTime.parse("2024-01-01T05:30:00.000001500+05:30"))),
            template.copy(
                ImmutableMap.of(
                    "id",
                    4L,
                    "tsTz",
                    OffsetDateTime.parse("2024-01-01T00:00:00.000003000+00:00"))));

    OutputFile tzFile =
        Files.localOutput(File.createTempFile("ts-tz-nano", ".orc", new File(temp.getPath())));
    DataWriter<Record> tzWriter =
        ORC.writeData(tzFile)
            .schema(schema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try {
      for (Record record : writeRecords) {
        tzWriter.write(record);
      }
    } finally {
      tzWriter.close();
    }

    // Boundary expressed in +04:00 (== 2024-01-01T00:00:00.000000500Z). Comparison is by instant at
    // nanosecond precision, so only ids 2 (750 ns), 3 (1500 ns), 4 (3000 ns) remain regardless of
    // the zone each value was written in.
    try (CloseableIterable<Record> reader =
        ORC.read(tzFile.toInputFile())
            .project(schema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
            .filter(Expressions.greaterThan("tsTz", "2024-01-01T04:00:00.000000500+04:00"))
            .config(OrcConf.ALLOW_SARG_TO_FILTER.getAttribute(), String.valueOf(true))
            .config(OrcConf.READER_USE_SELECTED.getAttribute(), String.valueOf(true))
            .build()) {
      List<Long> ids = Lists.newArrayList();
      for (Record record : reader) {
        ids.add((Long) record.getField("id"));
      }
      assertThat(ids).containsExactly(2L, 3L, 4L);
    }
  }
}
