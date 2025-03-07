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
package org.apache.iceberg.spark.data;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcConf;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestVectorizedOrcDataReader implements WithAssertions {
  @TempDir public static Path temp;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "binary", Types.BinaryType.get()),
          Types.NestedField.required(
              4, "array", Types.ListType.ofOptional(5, Types.IntegerType.get())));
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

    outputFile = Files.localOutput(File.createTempFile("test", ".orc", temp.toFile()));

    try (DataWriter<Record> dataWriter =
        ORC.writeData(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      for (Record record : builder.build()) {
        dataWriter.write(record);
      }
    }
  }

  private Iterator<InternalRow> batchesToRows(Iterator<ColumnarBatch> batches) {
    return Iterators.concat(Iterators.transform(batches, ColumnarBatch::rowIterator));
  }

  private void validateAllRows(Iterator<InternalRow> rows) {
    long rowCount = 0;
    long expId = 1;
    char expChar = 'a';
    while (rows.hasNext()) {
      InternalRow row = rows.next();
      assertThat(row.getLong(0)).isEqualTo(expId);
      assertThat(row.getString(1)).isEqualTo(Character.toString(expChar));
      assertThat(row.isNullAt(2)).isTrue();
      expId += 1;
      expChar += 1;
      rowCount += 1;
    }
    assertThat(rowCount).isEqualTo(5);
  }

  @Test
  public void testReader() throws IOException {
    try (CloseableIterable<ColumnarBatch> reader =
        ORC.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createBatchedReaderFunc(
                readOrcSchema ->
                    VectorizedSparkOrcReaders.buildReader(SCHEMA, readOrcSchema, ImmutableMap.of()))
            .build()) {
      validateAllRows(batchesToRows(reader.iterator()));
    }
  }

  @Test
  public void testReaderWithFilter() throws IOException {
    try (CloseableIterable<ColumnarBatch> reader =
        ORC.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createBatchedReaderFunc(
                readOrcSchema ->
                    VectorizedSparkOrcReaders.buildReader(SCHEMA, readOrcSchema, ImmutableMap.of()))
            .filter(Expressions.equal("id", 3L))
            .config(OrcConf.ALLOW_SARG_TO_FILTER.getAttribute(), String.valueOf(true))
            .build()) {
      validateAllRows(batchesToRows(reader.iterator()));
    }
  }

  @Test
  public void testWithFilterWithSelected() throws IOException {
    try (CloseableIterable<ColumnarBatch> reader =
        ORC.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createBatchedReaderFunc(
                readOrcSchema ->
                    VectorizedSparkOrcReaders.buildReader(SCHEMA, readOrcSchema, ImmutableMap.of()))
            .filter(Expressions.equal("id", 3L))
            .config(OrcConf.ALLOW_SARG_TO_FILTER.getAttribute(), String.valueOf(true))
            .config(OrcConf.READER_USE_SELECTED.getAttribute(), String.valueOf(true))
            .build()) {
      Iterator<InternalRow> rows = batchesToRows(reader.iterator());
      assertThat(rows).hasNext();
      InternalRow row = rows.next();
      assertThat(row.getLong(0)).isEqualTo(3L);
      assertThat(row.getString(1)).isEqualTo("c");
      assertThat(row.getArray(3).toIntArray()).isEqualTo(new int[] {3, 4, 5});
      assertThat(rows).isExhausted();
    }
  }
}
