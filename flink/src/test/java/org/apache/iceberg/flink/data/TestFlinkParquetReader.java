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

package org.apache.iceberg.flink.data;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.flink.data.RandomData.COMPLEX_SCHEMA;

public class TestFlinkParquetReader {
  private static final int NUM_RECORDS = 20_000;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private void testReadCorrectness(Schema schema, int numRecords, Iterable<Record> iterable) throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer = Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .build()) {
      writer.addAll(iterable);
    }

    try (CloseableIterable<RowData> reader = Parquet.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(type -> FlinkParquetReaders.buildReader(schema, type))
        .build()) {
      Iterator<Record> expected = iterable.iterator();
      Iterator<RowData> rows = reader.iterator();
      for (int i = 0; i < numRecords; i += 1) {
        Assert.assertTrue("Should have expected number of rows", rows.hasNext());
        TestHelpers.assertRowData(schema.asStruct(), expected.next(), rows.next());
      }
      Assert.assertFalse("Should not have extra rows", rows.hasNext());
    }
  }

  @Test
  public void testNormalRowData() throws IOException {
    testReadCorrectness(COMPLEX_SCHEMA, NUM_RECORDS, RandomData.generateRecords(COMPLEX_SCHEMA, NUM_RECORDS, 19981));
  }

  @Test
  public void testDictionaryEncodedData() throws IOException {
    testReadCorrectness(COMPLEX_SCHEMA, NUM_RECORDS,
        RandomData.generateDictionaryEncodableRecords(COMPLEX_SCHEMA, NUM_RECORDS, 21124));
  }

  @Test
  public void testFallbackData() throws IOException {
    testReadCorrectness(COMPLEX_SCHEMA, NUM_RECORDS,
        RandomData.generateFallbackRecords(COMPLEX_SCHEMA, NUM_RECORDS, 21124, NUM_RECORDS / 20));
  }
}
