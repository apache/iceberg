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
package org.apache.iceberg.data.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericData extends DataTest {

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    List<Record> expected = RandomGenericData.generate(schema, 100, 0L);

    writeAndValidateRecords(schema, expected);
  }

  @Test
  public void writeAndValidateRepeatingRecords() throws IOException {
    Schema structSchema =
        new Schema(
            required(100, "id", Types.LongType.get()),
            required(101, "data", Types.StringType.get()));
    List<Record> expectedRepeating =
        Collections.nCopies(100, RandomGenericData.generate(structSchema, 1, 0L).get(0));

    writeAndValidateRecords(structSchema, expectedRepeating);
  }

  @Test
  public void writeAndValidateTimestamps() throws IOException {
    TimeZone currentTz = TimeZone.getDefault();
    try {
      Schema timestampSchema =
          new Schema(
              required(1, "tsTzCol", Types.TimestampType.withZone()),
              required(2, "tsCol", Types.TimestampType.withoutZone()));

      // Write using America/New_York timezone
      TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
      GenericRecord record1 = GenericRecord.create(timestampSchema);
      record1.setField("tsTzCol", OffsetDateTime.parse("2017-01-16T17:10:34-08:00"));
      record1.setField("tsCol", LocalDateTime.parse("1970-01-01T00:01:00"));
      GenericRecord record2 = GenericRecord.create(timestampSchema);
      record2.setField("tsTzCol", OffsetDateTime.parse("2017-05-16T17:10:34-08:00"));
      record2.setField("tsCol", LocalDateTime.parse("1970-05-01T00:01:00"));
      GenericRecord record3 = GenericRecord.create(timestampSchema);
      record3.setField("tsTzCol", OffsetDateTime.parse("1935-01-16T17:10:34-08:00"));
      record3.setField("tsCol", LocalDateTime.parse("1935-01-01T00:01:00"));
      GenericRecord record4 = GenericRecord.create(timestampSchema);
      record4.setField("tsTzCol", OffsetDateTime.parse("1935-05-16T17:10:34-08:00"));
      record4.setField("tsCol", LocalDateTime.parse("1935-05-01T00:01:00"));

      File testFile = temp.newFile();
      Assert.assertTrue("Delete should succeed", testFile.delete());

      try (FileAppender<Record> writer =
          ORC.write(Files.localOutput(testFile))
              .schema(timestampSchema)
              .createWriterFunc(GenericOrcWriter::buildWriter)
              .build()) {
        writer.add(record1);
        writer.add(record2);
        writer.add(record3);
        writer.add(record4);
      }

      // Read using Asia/Kolkata timezone
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));
      List<Record> rows;
      try (CloseableIterable<Record> reader =
          ORC.read(Files.localInput(testFile))
              .project(timestampSchema)
              .createReaderFunc(
                  fileSchema -> GenericOrcReader.buildReader(timestampSchema, fileSchema))
              .build()) {
        rows = Lists.newArrayList(reader);
      }

      Assert.assertEquals(
          OffsetDateTime.parse("2017-01-17T01:10:34Z"), rows.get(0).getField("tsTzCol"));
      Assert.assertEquals(
          LocalDateTime.parse("1970-01-01T00:01:00"), rows.get(0).getField("tsCol"));
      Assert.assertEquals(
          OffsetDateTime.parse("2017-05-17T01:10:34Z"), rows.get(1).getField("tsTzCol"));
      Assert.assertEquals(
          LocalDateTime.parse("1970-05-01T00:01:00"), rows.get(1).getField("tsCol"));
      Assert.assertEquals(
          OffsetDateTime.parse("1935-01-17T01:10:34Z"), rows.get(2).getField("tsTzCol"));
      Assert.assertEquals(
          LocalDateTime.parse("1935-01-01T00:01:00"), rows.get(2).getField("tsCol"));
      Assert.assertEquals(
          OffsetDateTime.parse("1935-05-17T01:10:34Z"), rows.get(3).getField("tsTzCol"));
      Assert.assertEquals(
          LocalDateTime.parse("1935-05-01T00:01:00"), rows.get(3).getField("tsCol"));
    } finally {
      TimeZone.setDefault(currentTz);
    }
  }

  @Test
  public void writeAndValidateExternalData() throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    Configuration conf = new Configuration();
    TypeDescription writerSchema =
        TypeDescription.fromString("struct<a:tinyint,b:smallint,c:char(10),d:varchar(10)>");
    Writer writer =
        OrcFile.createWriter(
            new Path(testFile.toString()), OrcFile.writerOptions(conf).setSchema(writerSchema));
    VectorizedRowBatch batch = writerSchema.createRowBatch();
    batch.ensureSize(1);
    batch.size = 1;
    ((LongColumnVector) batch.cols[0]).vector[0] = 1;
    ((LongColumnVector) batch.cols[1]).vector[0] = 123;
    ((BytesColumnVector) batch.cols[2]).setVal(0, "1".getBytes(StandardCharsets.UTF_8));
    ((BytesColumnVector) batch.cols[3]).setVal(0, "123".getBytes(StandardCharsets.UTF_8));
    writer.addRowBatch(batch);
    writer.close();

    List<Record> rows;
    Schema readSchema =
        new Schema(
            optional(1, "a", Types.IntegerType.get()),
            optional(2, "b", Types.IntegerType.get()),
            optional(3, "c", Types.StringType.get()),
            optional(4, "d", Types.StringType.get()));
    try (CloseableIterable<Record> reader =
        ORC.read(Files.localInput(testFile))
            .project(readSchema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema))
            .build()) {
      rows = Lists.newArrayList(reader);
    }
    Assert.assertEquals(1, rows.get(0).getField("a"));
    Assert.assertEquals(123, rows.get(0).getField("b"));
    Assert.assertEquals("1", rows.get(0).getField("c"));
    Assert.assertEquals("123", rows.get(0).getField("d"));
  }

  private void writeAndValidateRecords(Schema schema, List<Record> expected) throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer =
        ORC.write(Files.localOutput(testFile))
            .schema(schema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      for (Record rec : expected) {
        writer.add(rec);
      }
    }

    List<Record> rows;
    try (CloseableIterable<Record> reader =
        ORC.read(Files.localInput(testFile))
            .project(schema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      DataTestHelpers.assertEquals(schema.asStruct(), expected.get(i), rows.get(i));
    }
  }
}
