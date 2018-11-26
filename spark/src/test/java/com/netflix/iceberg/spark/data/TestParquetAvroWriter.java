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

package com.netflix.iceberg.spark.data;

import com.netflix.iceberg.Files;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.parquet.Parquet;
import com.netflix.iceberg.parquet.ParquetAvroValueReaders;
import com.netflix.iceberg.parquet.ParquetAvroWriter;
import com.netflix.iceberg.parquet.ParquetReader;
import com.netflix.iceberg.parquet.ParquetSchemaUtil;
import com.netflix.iceberg.types.Types;
import org.apache.avro.generic.GenericData.Record;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;

public class TestParquetAvroWriter {
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

  @Test
  public void testCorrectness() throws IOException {
    Iterable<Record> records = RandomData.generate(COMPLEX_SCHEMA, 250_000, 34139);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer = Parquet.write(Files.localOutput(testFile))
        .schema(COMPLEX_SCHEMA)
        .createWriterFunc(ParquetAvroWriter::buildWriter)
        .build()) {
      writer.addAll(records);
    }

    MessageType readSchema = ParquetSchemaUtil.convert(COMPLEX_SCHEMA, "test");

    // verify that the new read path is correct
    try (ParquetReader<Record> reader = new ParquetReader<>(
        Files.localInput(testFile), COMPLEX_SCHEMA, ParquetReadOptions.builder().build(),
        fileSchema -> ParquetAvroValueReaders.buildReader(COMPLEX_SCHEMA, readSchema),
        Expressions.alwaysTrue(), false)) {
      int i = 0;
      Iterator<Record> iter = records.iterator();
      for (Record actual : reader) {
        Record expected = iter.next();
        Assert.assertEquals("Record " + i + " should match expected", expected, actual);
        i += 1;
      }
    }
  }
}
