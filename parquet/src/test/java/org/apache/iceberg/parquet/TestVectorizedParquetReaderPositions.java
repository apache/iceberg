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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.SkippingCloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestVectorizedParquetReaderPositions {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));
  private static final int ROWS = 10;
  private static final int ROWS_PER_ROW_GROUP = 4;
  private static final int BATCH_SIZE = 3;

  private final InMemoryOutputFile file = new InMemoryOutputFile();
  private final RowCountReader model = new RowCountReader();

  @BeforeEach
  void writeFile() throws IOException {
    DataWriter<StructLike> writer =
        Parquet.writeData(file)
            .schema(SCHEMA)
            .createWriterFunc(InternalWriter::createWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
            .set(
                TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
                String.valueOf(ROWS_PER_ROW_GROUP))
            .set(
                TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT,
                String.valueOf(ROWS_PER_ROW_GROUP))
            .build();

    try (writer) {
      for (long id = 0; id < ROWS; id += 1) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.set(0, id);
        writer.write(record);
      }
    }

    assertThat(writer.toDataFile().splitOffsets())
        .hasSize((ROWS + ROWS_PER_ROW_GROUP - 1) / ROWS_PER_ROW_GROUP);
  }

  @Test
  void positionOfBatches() throws IOException {
    try (CloseableIterable<Integer> reader = read(Expressions.alwaysTrue())) {
      SkippingCloseableIterator<Integer> batches = iterator(reader);
      List<Long> positions = Lists.newArrayList();
      List<Integer> sizes = Lists.newArrayList();
      while (batches.hasNext()) {
        positions.add(batches.position());
        sizes.add(batches.next());
      }

      assertThat(positions).containsExactly(0L, 3L, 4L, 7L, 8L);
      assertThat(sizes).containsExactly(3, 1, 3, 1, 2);
    }
  }

  @Test
  void advanceKeepsTheBatchHoldingThePosition() throws IOException {
    try (CloseableIterable<Integer> reader = read(Expressions.alwaysTrue())) {
      SkippingCloseableIterator<Integer> batches = iterator(reader);

      batches.advanceTo(5);
      assertThat(batches.position()).isEqualTo(4);

      batches.advanceTo(7);
      assertThat(batches.position()).isEqualTo(7);

      batches.advanceTo(9);
      assertThat(batches.position()).isEqualTo(8);
      assertThat(batches.next()).isEqualTo(2);
    }
  }

  @Test
  void advanceOverWholeRowGroupsWithoutReadingThem() throws IOException {
    try (CloseableIterable<Integer> reader = read(Expressions.alwaysTrue())) {
      SkippingCloseableIterator<Integer> batches = iterator(reader);
      batches.advanceTo(8);

      assertThat(model.rowGroups).isEmpty();
      assertThat(model.reads).isEmpty();
      assertThat(batches.position()).isEqualTo(8);
    }
  }

  @Test
  void advanceOverRowGroupFilteredOut() throws IOException {
    try (CloseableIterable<Integer> reader = read(Expressions.in("id", 0L, 9L))) {
      SkippingCloseableIterator<Integer> batches = iterator(reader);
      batches.advanceTo(9);

      assertThat(batches.position()).isEqualTo(8);
      assertThat(batches.next()).isEqualTo(2);
      assertThat(batches.hasNext()).isFalse();
    }
  }

  @Test
  void positionsAfterRowGroupFilteredOut() throws IOException {
    try (CloseableIterable<Integer> reader = read(Expressions.greaterThanOrEqual("id", 4L))) {
      SkippingCloseableIterator<Integer> batches = iterator(reader);

      assertThat(batches.position()).isEqualTo(4);
      assertThat(batches.next()).isEqualTo(3);
      assertThat(batches.position()).isEqualTo(7);
    }
  }

  private CloseableIterable<Integer> read(Expression filter) {
    return Parquet.read(file.toInputFile())
        .project(SCHEMA)
        .filter(filter)
        .recordsPerBatch(BATCH_SIZE)
        .createBatchedReaderFunc(fileSchema -> model)
        .build();
  }

  private static SkippingCloseableIterator<Integer> iterator(CloseableIterable<Integer> reader) {
    CloseableIterator<Integer> iterator = reader.iterator();
    assertThat(iterator).isInstanceOf(SkippingCloseableIterator.class);
    return (SkippingCloseableIterator<Integer>) iterator;
  }

  /** Produces batches that only hold their number of rows, and records what it reads. */
  private static class RowCountReader implements VectorizedReader<Integer> {
    private final List<Long> rowGroups = Lists.newArrayList();
    private final List<Integer> reads = Lists.newArrayList();

    @Override
    public Integer read(Integer reuse, int numRows) {
      reads.add(numRows);
      return numRows;
    }

    @Override
    public void setBatchSize(int batchSize) {}

    @Override
    public void setRowGroupInfo(
        PageReadStore pages, Map<ColumnPath, ColumnChunkMetaData> metadata) {
      rowGroups.add(pages.getRowCount());
    }

    @Override
    public void close() {}
  }
}
