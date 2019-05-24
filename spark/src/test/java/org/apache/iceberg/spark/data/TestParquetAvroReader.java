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
import java.util.Iterator;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetAvroValueReaders;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestParquetAvroReader {
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
          Types.StringType.get(), Types.TimestampType.withoutZone())),
      required(14, "winter", Types.ListType.ofOptional(15, Types.StructType.of(
          optional(16, "beet", Types.DoubleType.get()),
          required(17, "stamp", Types.TimeType.get()),
          optional(18, "wheeze", Types.StringType.get())
      ))),
      optional(19, "renovate", Types.MapType.ofRequired(20, 21,
          Types.StringType.get(), Types.StructType.of(
              optional(22, "jumpy", Types.DoubleType.get()),
              required(23, "koala", Types.TimeType.get())
          ))),
      optional(2, "slide", Types.StringType.get())
  );

  @Ignore
  public void testStructSchema() throws IOException {
    Schema structSchema = new Schema(
        required(1, "circumvent", Types.LongType.get()),
        optional(2, "antarctica", Types.StringType.get()),
        optional(3, "fluent", Types.DoubleType.get()),
        required(4, "quell", Types.StructType.of(
            required(5, "operator", Types.BooleanType.get()),
            optional(6, "fanta", Types.IntegerType.get()),
            optional(7, "cable", Types.FloatType.get())
        )),
        required(8, "chimney", Types.TimestampType.withZone()),
        required(9, "wool", Types.DateType.get())
    );

    File testFile = writeTestData(structSchema, 5_000_000, 1059);
    // RandomData uses the root record name "test", which must match for records to be equal
    MessageType readSchema = ParquetSchemaUtil.convert(structSchema, "test");

    long sum = 0;
    long sumSq = 0;
    int warmups = 2;
    int n = 10;

    for (int i = 0; i < warmups + n; i += 1) {
      // clean up as much memory as possible to avoid a large GC during the timed run
      System.gc();

      try (CloseableIterable<Record> reader = Parquet.read(Files.localInput(testFile))
          .project(structSchema)
          .createReaderFunc(
              fileSchema -> ParquetAvroValueReaders.buildReader(structSchema, readSchema))
           .build()) {
        long start = System.currentTimeMillis();
        long val = 0;
        long count = 0;
        for (Record record : reader) {
          // access something to ensure the compiler doesn't optimize this away
          val ^= (Long) record.get(0);
          count += 1;
        }
        long end = System.currentTimeMillis();
        long duration = end - start;

        System.err.println("XOR val: " + val);
        System.err.println(String.format("Reassembled %d records in %d ms", count, duration));

        if (i >= warmups) {
          sum += duration;
          sumSq += (duration * duration);
        }
      }
    }

    double mean = ((double) sum) / n;
    double stddev = Math.sqrt((((double) sumSq) / n) - (mean * mean));

    System.err.println(String.format(
        "Ran %d trials: mean time: %.3f ms, stddev: %.3f ms", n, mean, stddev));
  }

  @Ignore
  public void testWithOldReadPath() throws IOException {
    File testFile = writeTestData(COMPLEX_SCHEMA, 500_000, 1985);
    // RandomData uses the root record name "test", which must match for records to be equal
    MessageType readSchema = ParquetSchemaUtil.convert(COMPLEX_SCHEMA, "test");

    for (int i = 0; i < 5; i += 1) {
      // clean up as much memory as possible to avoid a large GC during the timed run
      System.gc();

      try (CloseableIterable<Record> reader =  Parquet.read(Files.localInput(testFile))
          .project(COMPLEX_SCHEMA)
          .build()) {
        long start = System.currentTimeMillis();
        long val = 0;
        long count = 0;
        for (Record record : reader) {
          // access something to ensure the compiler doesn't optimize this away
          val ^= (Long) record.get(0);
          count += 1;
        }
        long end = System.currentTimeMillis();

        System.err.println("XOR val: " + val);
        System.err.println("Old read path: read " + count + " records in " + (end - start) + " ms");
      }

      // clean up as much memory as possible to avoid a large GC during the timed run
      System.gc();

      try (CloseableIterable<Record> reader = Parquet.read(Files.localInput(testFile))
           .project(COMPLEX_SCHEMA)
           .createReaderFunc(
               fileSchema -> ParquetAvroValueReaders.buildReader(COMPLEX_SCHEMA, readSchema))
           .build()) {
        long start = System.currentTimeMillis();
        long val = 0;
        long count = 0;
        for (Record record : reader) {
          // access something to ensure the compiler doesn't optimize this away
          val ^= (Long) record.get(0);
          count += 1;
        }
        long end = System.currentTimeMillis();

        System.err.println("XOR val: " + val);
        System.err.println("New read path: read " + count + " records in " + (end - start) + " ms");
      }
    }
  }

  @Test
  public void testCorrectness() throws IOException {
    Iterable<Record> records = RandomData.generate(COMPLEX_SCHEMA, 250_000, 34139);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer = Parquet.write(Files.localOutput(testFile))
        .schema(COMPLEX_SCHEMA)
        .build()) {
      writer.addAll(records);
    }

    // RandomData uses the root record name "test", which must match for records to be equal
    MessageType readSchema = ParquetSchemaUtil.convert(COMPLEX_SCHEMA, "test");

    // verify that the new read path is correct
    try (CloseableIterable<Record> reader = Parquet.read(Files.localInput(testFile))
        .project(COMPLEX_SCHEMA)
        .createReaderFunc(
            fileSchema -> ParquetAvroValueReaders.buildReader(COMPLEX_SCHEMA, readSchema))
        .reuseContainers()
        .build()) {
      int i = 0;
      Iterator<Record> iter = records.iterator();
      for (Record actual : reader) {
        Record expected = iter.next();
        Assert.assertEquals("Record " + i + " should match expected", expected, actual);
        i += 1;
      }
    }
  }

  private File writeTestData(Schema schema, int n, int seed) throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer = Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .build()) {
      writer.addAll(RandomData.generate(schema, n, seed));
    }

    return testFile;
  }
}
