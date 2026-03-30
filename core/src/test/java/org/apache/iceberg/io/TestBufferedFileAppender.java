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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBufferedFileAppender {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private InMemoryOutputFile outputFile;
  private GenericRecord record;

  @BeforeEach
  public void before() {
    this.outputFile = new InMemoryOutputFile();
    this.record = GenericRecord.create(SCHEMA);
  }

  private Function<List<Record>, FileAppender<Record>> avroFactory(OutputFile out) {
    return bufferedRows -> {
      try {
        return Avro.write(out)
            .createWriterFunc(DataWriter::create)
            .schema(SCHEMA)
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new org.apache.iceberg.exceptions.RuntimeIOException(e);
      }
    };
  }

  private BufferedFileAppender<Record> createAppender(int bufferSize) {
    return new BufferedFileAppender<>(bufferSize, avroFactory(outputFile), Record::copy);
  }

  private Record createRecord(long id, String data) {
    return record.copy(ImmutableMap.of("id", id, "data", data));
  }

  private List<Record> readBack() throws IOException {
    try (AvroIterable<Record> reader =
        Avro.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createResolvingReader(PlannedDataReader::create)
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  @Test
  public void testBufferFlushesOnThreshold() throws IOException {
    BufferedFileAppender<Record> appender = createAppender(3);

    appender.add(createRecord(1L, "a"));
    appender.add(createRecord(2L, "b"));

    // delegate not yet created, length should be 0
    assertThat(appender.length()).isEqualTo(0L);

    appender.add(createRecord(3L, "c"));

    // delegate created after 3rd row, length should be > 0
    assertThat(appender.length()).isGreaterThan(0L);

    appender.add(createRecord(4L, "d"));
    appender.add(createRecord(5L, "e"));
    appender.close();

    List<Record> actual = readBack();
    assertThat(actual).hasSize(5);
    assertThat(actual.get(0).getField("id")).isEqualTo(1L);
    assertThat(actual.get(4).getField("id")).isEqualTo(5L);
  }

  @Test
  public void testCloseWithPartialBuffer() throws IOException {
    BufferedFileAppender<Record> appender = createAppender(10);

    appender.add(createRecord(1L, "a"));
    appender.add(createRecord(2L, "b"));
    appender.add(createRecord(3L, "c"));

    // buffer not full yet
    assertThat(appender.length()).isEqualTo(0L);

    // close flushes partial buffer through factory
    appender.close();

    List<Record> actual = readBack();
    assertThat(actual).hasSize(3);
    assertThat(actual.get(0).getField("data")).isEqualTo("a");
    assertThat(actual.get(2).getField("data")).isEqualTo("c");
  }

  @Test
  public void testCopyFuncIsApplied() throws IOException {
    BufferedFileAppender<Record> appender = createAppender(3);

    // use a single mutable record, relying on copyFunc to snapshot it
    record.set(0, 1L);
    record.set(1, "first");
    appender.add(record);

    record.set(0, 2L);
    record.set(1, "second");
    appender.add(record);

    record.set(0, 3L);
    record.set(1, "third");
    appender.add(record);

    appender.close();

    List<Record> actual = readBack();
    assertThat(actual).hasSize(3);
    // without copyFunc, all 3 rows would have the last values (3, "third")
    assertThat(actual.get(0).getField("id")).isEqualTo(1L);
    assertThat(actual.get(0).getField("data")).isEqualTo("first");
    assertThat(actual.get(1).getField("id")).isEqualTo(2L);
    assertThat(actual.get(1).getField("data")).isEqualTo("second");
  }

  @Test
  public void testMetricsAfterClose() throws IOException {
    BufferedFileAppender<Record> appender = createAppender(2);

    appender.add(createRecord(1L, "a"));
    appender.add(createRecord(2L, "b"));
    appender.add(createRecord(3L, "c"));
    appender.close();

    assertThat(appender.metrics()).isNotNull();
    assertThat(appender.metrics().recordCount()).isEqualTo(3L);
    assertThat(appender.length()).isGreaterThan(0L);
  }

  @Test
  public void testMetricsBeforeCloseThrows() throws IOException {
    try (BufferedFileAppender<Record> appender = createAppender(10)) {
      assertThatThrownBy(appender::metrics)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Cannot return metrics for unclosed appender");
    }
  }

  @Test
  public void testAddAfterCloseThrows() throws IOException {
    try (BufferedFileAppender<Record> appender = createAppender(10)) {
      appender.add(createRecord(1L, "a"));
      appender.close();

      assertThatThrownBy(() -> appender.add(createRecord(2L, "b")))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Cannot add to a closed appender");
    }
  }

  @Test
  public void testAddAllSpanningBuffer() throws IOException {
    BufferedFileAppender<Record> appender = createAppender(2);

    List<Record> records =
        Lists.newArrayList(
            createRecord(1L, "a"),
            createRecord(2L, "b"),
            createRecord(3L, "c"),
            createRecord(4L, "d"));

    appender.addAll(records);
    appender.close();

    List<Record> actual = readBack();
    assertThat(actual).hasSize(4);
    assertThat(actual.get(0).getField("id")).isEqualTo(1L);
    assertThat(actual.get(3).getField("id")).isEqualTo(4L);
  }

  @Test
  public void testCloseWithNoData() throws IOException {
    BufferedFileAppender<Record> appender = createAppender(10);
    // close immediately with no data written
    appender.close();
    // delegate was never created
    assertThat(appender.length()).isEqualTo(0L);
    assertThat(appender.metrics()).isNotNull();
    assertThat(appender.metrics().recordCount()).isEqualTo(0L);
    assertThat(appender.splitOffsets()).isNull();
  }
}
