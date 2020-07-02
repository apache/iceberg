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
import org.apache.flink.types.Row;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestFlinkParquetReaderWriter {
  private static final int NUM_RECORDS = 20_000;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final Schema COMPLEX_SCHEMA = new Schema(
      required(1, "roots", Types.LongType.get()),
      optional(3, "lime", Types.ListType.ofRequired(4, Types.DoubleType.get())),
      required(5, "strict", Types.StructType.of(
          required(9, "tangerine", Types.StringType.get()),
          optional(6, "hopeful", Types.StructType.of(
              required(7, "steel", Types.FloatType.get()),
              required(8, "lantern", Types.DateType.get())
          )),
          optional(10, "vehement", Types.LongType.get())
      )),
      optional(11, "metamorphosis", Types.MapType.ofRequired(12, 13,
          Types.StringType.get(), Types.TimestampType.withZone())),
      required(14, "winter", Types.ListType.ofOptional(15, Types.StructType.of(
          optional(16, "beet", Types.DoubleType.get()),
          required(17, "stamp", Types.FloatType.get()),
          optional(18, "wheeze", Types.StringType.get())
      ))),
      optional(19, "renovate", Types.MapType.ofRequired(20, 21,
          Types.StringType.get(), Types.StructType.of(
              optional(22, "jumpy", Types.DoubleType.get()),
              required(23, "koala", Types.IntegerType.get()),
              required(24, "couch rope", Types.IntegerType.get())
          ))),
      optional(2, "slide", Types.StringType.get())
  );

  private void testCorrectness(Schema schema, int numRecords, Iterable<Row> iterable) throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Row> writer = Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc(FlinkParquetWriters::buildWriter)
        .build()) {
      writer.addAll(iterable);
    }

    try (CloseableIterable<Row> reader = Parquet.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(type -> FlinkParquetReaders.buildReader(schema, type))
        .build()) {
      Iterator<Row> expected = iterable.iterator();
      Iterator<Row> rows = reader.iterator();
      for (int i = 0; i < numRecords; i += 1) {
        Assert.assertTrue("Should have expected number of rows", rows.hasNext());
        Assert.assertEquals(expected.next(), rows.next());
      }
      Assert.assertFalse("Should not have extra rows", rows.hasNext());
    }
  }

  @Test
  public void testNormalRowData() throws IOException {
    testCorrectness(COMPLEX_SCHEMA, NUM_RECORDS, RandomData.generate(COMPLEX_SCHEMA, NUM_RECORDS, 19981));
  }

  @Test
  public void testDictionaryEncodedData() throws IOException {
    testCorrectness(COMPLEX_SCHEMA, NUM_RECORDS,
        RandomData.generateDictionaryEncodableData(COMPLEX_SCHEMA, NUM_RECORDS, 21124));
  }

  @Test
  public void testFallbackData() throws IOException {
    testCorrectness(COMPLEX_SCHEMA, NUM_RECORDS,
        RandomData.generateFallbackData(COMPLEX_SCHEMA, NUM_RECORDS, 21124, NUM_RECORDS / 20));
  }
}
