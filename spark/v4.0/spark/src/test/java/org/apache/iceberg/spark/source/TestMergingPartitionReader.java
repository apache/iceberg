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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

public class TestMergingPartitionReader {

  @Test
  public void testTwoSortedReaders() throws IOException {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    // Reader 1: [1, 3, 5]
    PartitionReader<InternalRow> reader1 = createMockReader(ImmutableList.of(1, 3, 5));

    // Reader 2: [2, 4, 6]
    PartitionReader<InternalRow> reader2 = createMockReader(ImmutableList.of(2, 4, 6));

    MergingPartitionReader<InternalRow> mergingReader =
        new MergingPartitionReader<>(
            ImmutableList.of(reader1, reader2), sortOrder, sparkSchema, schema);

    List<Integer> results = readAll(mergingReader);
    assertThat(results).containsExactly(1, 2, 3, 4, 5, 6);

    mergingReader.close();
  }

  @Test
  public void testThreeSortedReaders() throws IOException {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    // Reader 1: [1, 4, 7]
    PartitionReader<InternalRow> reader1 = createMockReader(ImmutableList.of(1, 4, 7));

    // Reader 2: [2, 5, 8]
    PartitionReader<InternalRow> reader2 = createMockReader(ImmutableList.of(2, 5, 8));

    // Reader 3: [3, 6, 9]
    PartitionReader<InternalRow> reader3 = createMockReader(ImmutableList.of(3, 6, 9));

    MergingPartitionReader<InternalRow> mergingReader =
        new MergingPartitionReader<>(
            ImmutableList.of(reader1, reader2, reader3), sortOrder, sparkSchema, schema);

    List<Integer> results = readAll(mergingReader);
    assertThat(results).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);

    mergingReader.close();
  }

  @Test
  public void testDescendingOrder() throws IOException {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).desc("id").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    // Reader 1: [5, 3, 1] (descending)
    PartitionReader<InternalRow> reader1 = createMockReader(ImmutableList.of(5, 3, 1));

    // Reader 2: [6, 4, 2] (descending)
    PartitionReader<InternalRow> reader2 = createMockReader(ImmutableList.of(6, 4, 2));

    MergingPartitionReader<InternalRow> mergingReader =
        new MergingPartitionReader<>(
            ImmutableList.of(reader1, reader2), sortOrder, sparkSchema, schema);

    List<Integer> results = readAll(mergingReader);
    assertThat(results).containsExactly(6, 5, 4, 3, 2, 1);

    mergingReader.close();
  }

  @Test
  public void testDuplicateValues() throws IOException {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    // Reader 1: [1, 2, 3]
    PartitionReader<InternalRow> reader1 = createMockReader(ImmutableList.of(1, 2, 3));

    // Reader 2: [2, 3, 4] (has duplicates with reader1)
    PartitionReader<InternalRow> reader2 = createMockReader(ImmutableList.of(2, 3, 4));

    MergingPartitionReader<InternalRow> mergingReader =
        new MergingPartitionReader<>(
            ImmutableList.of(reader1, reader2), sortOrder, sparkSchema, schema);

    List<Integer> results = readAll(mergingReader);
    assertThat(results).containsExactly(1, 2, 2, 3, 3, 4);

    mergingReader.close();
  }

  @Test
  public void testEmptyReader() throws IOException {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    // Reader 1: [1, 2, 3]
    PartitionReader<InternalRow> reader1 = createMockReader(ImmutableList.of(1, 2, 3));

    // Reader 2: [] (empty)
    PartitionReader<InternalRow> reader2 = createMockReader(ImmutableList.of());

    MergingPartitionReader<InternalRow> mergingReader =
        new MergingPartitionReader<>(
            ImmutableList.of(reader1, reader2), sortOrder, sparkSchema, schema);

    List<Integer> results = readAll(mergingReader);
    assertThat(results).containsExactly(1, 2, 3);

    mergingReader.close();
  }

  @Test
  public void testSingleReader() throws IOException {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    PartitionReader<InternalRow> reader = createMockReader(ImmutableList.of(1, 2, 3));

    MergingPartitionReader<InternalRow> mergingReader =
        new MergingPartitionReader<>(ImmutableList.of(reader), sortOrder, sparkSchema, schema);

    List<Integer> results = readAll(mergingReader);
    assertThat(results).containsExactly(1, 2, 3);

    mergingReader.close();
  }

  @Test
  public void testMultipleFieldsSorting() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").asc("data").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    // Reader 1: [(1, "a"), (2, "a")]
    PartitionReader<InternalRow> reader1 =
        createMockReaderWithStrings(ImmutableList.of(row(1, "a"), row(2, "a")));

    // Reader 2: [(1, "b"), (2, "b")]
    PartitionReader<InternalRow> reader2 =
        createMockReaderWithStrings(ImmutableList.of(row(1, "b"), row(2, "b")));

    MergingPartitionReader<InternalRow> mergingReader =
        new MergingPartitionReader<>(
            ImmutableList.of(reader1, reader2), sortOrder, sparkSchema, schema);

    List<InternalRow> results = readAllRows(mergingReader);
    assertThat(results).hasSize(4);
    assertThat(results.get(0).getInt(0)).isEqualTo(1);
    assertThat(results.get(0).getUTF8String(1).toString()).isEqualTo("a");
    assertThat(results.get(1).getInt(0)).isEqualTo(1);
    assertThat(results.get(1).getUTF8String(1).toString()).isEqualTo("b");
    assertThat(results.get(2).getInt(0)).isEqualTo(2);
    assertThat(results.get(2).getUTF8String(1).toString()).isEqualTo("a");
    assertThat(results.get(3).getInt(0)).isEqualTo(2);
    assertThat(results.get(3).getUTF8String(1).toString()).isEqualTo("b");

    mergingReader.close();
  }

  @Test
  public void testEmptyReadersListThrows() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").build();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    assertThatThrownBy(
            () -> new MergingPartitionReader<>(ImmutableList.of(), sortOrder, sparkSchema, schema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Readers cannot be empty");
  }

  @Test
  public void testUnsortedOrderThrows() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SortOrder unsortedOrder = SortOrder.unsorted();
    StructType sparkSchema = SparkSchemaUtil.convert(schema);

    PartitionReader<InternalRow> reader = createMockReader(ImmutableList.of(1, 2, 3));

    assertThatThrownBy(
            () ->
                new MergingPartitionReader<>(
                    ImmutableList.of(reader), unsortedOrder, sparkSchema, schema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Sort order must be sorted");
  }

  private PartitionReader<InternalRow> createMockReader(List<Integer> values) {
    return new MockPartitionReader(
        values.stream().map(this::createSingleIntRow).collect(ImmutableList.toImmutableList()));
  }

  private PartitionReader<InternalRow> createMockReaderWithStrings(List<InternalRow> rows) {
    return new MockPartitionReader(rows);
  }

  private InternalRow createSingleIntRow(int value) {
    GenericInternalRow row = new GenericInternalRow(1);
    row.setInt(0, value);
    return row;
  }

  private InternalRow row(int id, String data) {
    GenericInternalRow row = new GenericInternalRow(2);
    row.setInt(0, id);
    row.update(1, UTF8String.fromString(data));
    return row;
  }

  private List<Integer> readAll(MergingPartitionReader<InternalRow> reader) throws IOException {
    List<Integer> results = Lists.newArrayList();
    while (reader.next()) {
      results.add(reader.get().getInt(0));
    }
    return results;
  }

  private List<InternalRow> readAllRows(MergingPartitionReader<InternalRow> reader)
      throws IOException {
    List<InternalRow> results = Lists.newArrayList();
    while (reader.next()) {
      // Copy the row because the reader may reuse the object
      InternalRow current = reader.get();
      GenericInternalRow copy = new GenericInternalRow(current.numFields());
      for (int i = 0; i < current.numFields(); i++) {
        if (!current.isNullAt(i)) {
          if (i == 0) {
            copy.setInt(i, current.getInt(i));
          } else {
            copy.update(i, current.getUTF8String(i).clone());
          }
        }
      }
      results.add(copy);
    }
    return results;
  }

  private static class MockPartitionReader implements PartitionReader<InternalRow> {
    private final List<InternalRow> rows;
    private int index = -1;

    MockPartitionReader(List<InternalRow> rows) {
      this.rows = Lists.newArrayList(rows);
    }

    @Override
    public boolean next() {
      index++;
      return index < rows.size();
    }

    @Override
    public InternalRow get() {
      return rows.get(index);
    }

    @Override
    public void close() {
      // No-op for mock
    }
  }
}
