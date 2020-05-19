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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkAvroWriter;
import org.apache.iceberg.spark.data.SparkOrcWriter;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestPositionDeleteFileReadWrite {

  private static final Configuration CONF = new Configuration();
  private final FileFormat format;
  private static final Schema SCHEMA = new Schema(
      required(1, "path", Types.StringType.get()),
      required(2, "position", Types.LongType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "parquet" },
        new Object[] { "avro" },
        new Object[] { "orc" }
    };
  }

  public TestPositionDeleteFileReadWrite(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Test
  public void writeAndValidate() throws IOException {
    Iterable<InternalRow> expected = RandomData.generateSpark(SCHEMA, 100, 0L);
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());
    switch (format) {
      case PARQUET:
        try (FileAppender<InternalRow> writer = Parquet.write(Files.localOutput(testFile))
            .schema(SCHEMA)
            .named("test")
            .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(SCHEMA, msgType))
            .build()) {
          writer.addAll(expected);
        }
        break;
      case AVRO:
        try (FileAppender<InternalRow> writer = Avro.write(Files.localOutput(testFile))
            .schema(SCHEMA)
            .createWriterFunc(ignored -> new SparkAvroWriter(SCHEMA))
            .named("test")
            .build()) {
          writer.addAll(expected);
        }
        break;
      case ORC:
        try (FileAppender<InternalRow> writer = ORC.write(Files.localOutput(testFile))
            .createWriterFunc(SparkOrcWriter::new)
            .schema(SCHEMA)
            .build()) {
          writer.addAll(expected);
        }
        break;
      default:
        throw new UnsupportedOperationException(
            "Cannot read unknown format: " + format);
    }

    InputFile inputFile = HadoopInputFile.fromPath(new Path(testFile.toURI().getPath()), CONF);

    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
        .withInputFile(inputFile)
        .withFormat(format)
        .withRecordCount(100)
        .withFileSizeInBytes(testFile.length())
        .build();

    PositionDeleteFileReader reader = new PositionDeleteFileReader(inputFile, format);
    Iterator<InternalRow> rows = reader.open(0, file.fileSizeInBytes()).iterator();

    for (InternalRow row : expected) {
      Assert.assertTrue("Should have expected number of rows", rows.hasNext());
      TestHelpers.assertEquals(SCHEMA, row, rows.next());
    }
  }
}
