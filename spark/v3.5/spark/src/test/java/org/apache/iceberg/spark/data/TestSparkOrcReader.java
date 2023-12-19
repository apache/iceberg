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

import static org.apache.iceberg.spark.data.TestHelpers.assertEquals;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;

public class TestSparkOrcReader extends AvroDataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    final Iterable<InternalRow> expected = RandomData.generateSpark(schema, 100, 0L);

    writeAndValidateRecords(schema, expected);
  }

  @Test
  public void writeAndValidateRepeatingRecords() throws IOException {
    Schema structSchema =
        new Schema(
            required(100, "id", Types.LongType.get()),
            required(101, "data", Types.StringType.get()));
    List<InternalRow> expectedRepeating =
        Collections.nCopies(100, RandomData.generateSpark(structSchema, 1, 0L).iterator().next());

    writeAndValidateRecords(structSchema, expectedRepeating);
  }

  private void writeAndValidateRecords(Schema schema, Iterable<InternalRow> expected)
      throws IOException {
    File testFile = temp.toFile();
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    try (FileAppender<InternalRow> writer =
        ORC.write(Files.localOutput(testFile))
            .createWriterFunc(SparkOrcWriter::new)
            .schema(schema)
            .build()) {
      writer.addAll(expected);
    }

    try (CloseableIterable<InternalRow> reader =
        ORC.read(Files.localInput(testFile))
            .project(schema)
            .createReaderFunc(readOrcSchema -> new SparkOrcReader(schema, readOrcSchema))
            .build()) {
      final Iterator<InternalRow> actualRows = reader.iterator();
      final Iterator<InternalRow> expectedRows = expected.iterator();
      while (expectedRows.hasNext()) {
        assertThat(actualRows).as("Should have expected number of rows").hasNext();
        assertEquals(schema, expectedRows.next(), actualRows.next());
      }
      assertThat(actualRows).as("Should not have extra rows").isExhausted();
    }

    try (CloseableIterable<ColumnarBatch> reader =
        ORC.read(Files.localInput(testFile))
            .project(schema)
            .createBatchedReaderFunc(
                readOrcSchema ->
                    VectorizedSparkOrcReaders.buildReader(schema, readOrcSchema, ImmutableMap.of()))
            .build()) {
      final Iterator<InternalRow> actualRows = batchesToRows(reader.iterator());
      final Iterator<InternalRow> expectedRows = expected.iterator();
      while (expectedRows.hasNext()) {
        assertThat(actualRows).as("Should have expected number of rows").hasNext();
        assertEquals(schema, expectedRows.next(), actualRows.next());
      }
      assertThat(actualRows).as("Should not have extra rows").isExhausted();
    }
  }

  private Iterator<InternalRow> batchesToRows(Iterator<ColumnarBatch> batches) {
    return Iterators.concat(Iterators.transform(batches, ColumnarBatch::rowIterator));
  }
}
