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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.SkippingCloseableIterator;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestParquetReaderPositions {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));
  private static final int ROWS = 4;
  private static final boolean ONE_ROW_PER_ROW_GROUP = true;

  private final InMemoryOutputFile file = new InMemoryOutputFile();

  @Test
  void positionOfRows() throws IOException {
    write(ONE_ROW_PER_ROW_GROUP);

    try (CloseableIterable<Record> reader = read(Expressions.alwaysTrue())) {
      SkippingCloseableIterator<Record> rows = iterator(reader);
      for (long pos = 0; pos < ROWS; pos += 1) {
        assertThat(rows.position()).isEqualTo(pos);
        assertThat(rows.next().get(0)).isEqualTo(pos);
      }

      assertThat(rows.hasNext()).isFalse();
    }
  }

  @Test
  void advanceOverWholeRowGroups() throws IOException {
    write(ONE_ROW_PER_ROW_GROUP);

    try (CloseableIterable<Record> reader = read(Expressions.alwaysTrue())) {
      SkippingCloseableIterator<Record> rows = iterator(reader);
      rows.advanceTo(2);

      assertThat(rows.position()).isEqualTo(2);
      assertThat(rows.next().get(0)).isEqualTo(2L);
    }
  }

  @Test
  void advancesRowsWithinRowGroup() throws IOException {
    write(!ONE_ROW_PER_ROW_GROUP);

    try (CloseableIterable<Record> reader = read(Expressions.alwaysTrue())) {
      SkippingCloseableIterator<Record> rows = iterator(reader);
      rows.advanceTo(2);

      assertThat(rows.position()).isEqualTo(2);
      assertThat(rows.next().get(0)).isEqualTo(2L);
    }
  }

  @Test
  void advancingBackwardsIsNoOp() throws IOException {
    write(ONE_ROW_PER_ROW_GROUP);

    try (CloseableIterable<Record> reader = read(Expressions.alwaysTrue())) {
      SkippingCloseableIterator<Record> rows = iterator(reader);
      rows.advanceTo(2);
      rows.advanceTo(1);

      assertThat(rows.position()).isEqualTo(2);
    }
  }

  @Test
  void advancingPastTheLastRowExhaustsTheIterator() throws IOException {
    write(ONE_ROW_PER_ROW_GROUP);

    try (CloseableIterable<Record> reader = read(Expressions.alwaysTrue())) {
      SkippingCloseableIterator<Record> rows = iterator(reader);
      rows.advanceTo(ROWS + 1);

      assertThat(rows.hasNext()).isFalse();
    }
  }

  @Test
  void rejectsPositionWhenNoRowIsAvailable() throws IOException {
    write(ONE_ROW_PER_ROW_GROUP);

    try (CloseableIterable<Record> reader = read(Expressions.greaterThan("id", (long) ROWS))) {
      SkippingCloseableIterator<Record> rows = iterator(reader);

      assertThat(rows.hasNext()).isFalse();
      assertThatThrownBy(rows::position)
          .isInstanceOf(NoSuchElementException.class)
          .hasMessage("No more rows");
    }
  }

  @Test
  void positionsSkipsFilteredLeadingRowGroups() throws IOException {
    write(ONE_ROW_PER_ROW_GROUP);

    try (CloseableIterable<Record> reader = read(Expressions.greaterThanOrEqual("id", 2L))) {
      SkippingCloseableIterator<Record> rows = iterator(reader);

      assertThat(rows.position()).isEqualTo(2);
      assertThat(rows.next().get(0)).isEqualTo(2L);
    }
  }

  @Test
  void positionsAfterFilteredRowGroup() throws IOException {
    write(ONE_ROW_PER_ROW_GROUP);

    try (CloseableIterable<Record> reader = read(Expressions.in("id", 0L, 2L))) {
      SkippingCloseableIterator<Record> rows = iterator(reader);

      assertThat(rows.position()).isEqualTo(0);
      assertThat(rows.next().get(0)).isEqualTo(0L);

      assertThat(rows.position()).isEqualTo(2);
      assertThat(rows.next().get(0)).isEqualTo(2L);

      assertThat(rows.hasNext()).isFalse();
      assertThatThrownBy(rows::position)
          .isInstanceOf(NoSuchElementException.class)
          .hasMessage("No more rows");
    }
  }

  @Test
  void advancesOverFilteredRowGroup() throws IOException {
    write(ONE_ROW_PER_ROW_GROUP);

    try (CloseableIterable<Record> reader = read(Expressions.in("id", 0L, 2L))) {
      SkippingCloseableIterator<Record> rows = iterator(reader);
      rows.advanceTo(1);

      assertThat(rows.position()).isEqualTo(2);
      assertThat(rows.next().get(0)).isEqualTo(2L);
    }
  }

  @Test
  void discardsRowGroupsAroundFilteredRowGroup() throws IOException {
    write(ONE_ROW_PER_ROW_GROUP);

    try (CloseableIterable<Record> reader = read(Expressions.in("id", 0L, 2L, 3L))) {
      SkippingCloseableIterator<Record> rows = iterator(reader);
      rows.advanceTo(3);

      assertThat(rows.position()).isEqualTo(3);
      assertThat(rows.next().get(0)).isEqualTo(3L);
      assertThat(rows.hasNext()).isFalse();
    }
  }

  @Test
  void positionWithReadingSplit() throws IOException {
    List<Long> splitOffsets = write(ONE_ROW_PER_ROW_GROUP).splitOffsets();
    long start = splitOffsets.get(2);
    long length = splitOffsets.get(3) - start;

    try (CloseableIterable<Record> reader =
        Parquet.read(file.toInputFile())
            .project(SCHEMA)
            .split(start, length)
            .createReaderFunc(fileSchema -> InternalReader.create(SCHEMA, fileSchema))
            .build()) {
      SkippingCloseableIterator<Record> rows = iterator(reader);

      assertThat(rows.position()).isEqualTo(2);
      assertThat(rows.next().get(0)).isEqualTo(2L);
      assertThat(rows.hasNext()).isFalse();
    }
  }

  private DataFile write(boolean rowGroupPerRow) throws IOException {
    Parquet.DataWriteBuilder builder =
        Parquet.writeData(file)
            .schema(SCHEMA)
            .createWriterFunc(InternalWriter::createWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned());
    if (rowGroupPerRow) {
      builder
          .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
          .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1")
          .set(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "1");
    }

    DataWriter<StructLike> writer = builder.build();
    try (writer) {
      for (long id = 0; id < ROWS; id += 1) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.set(0, id);
        writer.write(record);
      }
    }

    DataFile dataFile = writer.toDataFile();
    assertThat(dataFile.splitOffsets()).hasSize(rowGroupPerRow ? ROWS : 1);
    return dataFile;
  }

  private CloseableIterable<Record> read(Expression filter) {
    return Parquet.read(file.toInputFile())
        .project(SCHEMA)
        .filter(filter)
        .createReaderFunc(fileSchema -> InternalReader.create(SCHEMA, fileSchema))
        .build();
  }

  private static SkippingCloseableIterator<Record> iterator(CloseableIterable<Record> reader) {
    CloseableIterator<Record> iterator = reader.iterator();
    assertThat(iterator).isInstanceOf(SkippingCloseableIterator.class);
    return (SkippingCloseableIterator<Record>) iterator;
  }
}
