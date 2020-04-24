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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
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
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestGenericData extends DataTest {

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    List<Record> expected = RandomGenericData.generate(schema, 100, 0L);

    writeAndValidateRecords(schema, expected);
  }

  @Test
  public void writeAndValidateRepeatingRecords() throws IOException {
    Schema structSchema = new Schema(
        required(100, "id", Types.LongType.get()),
        required(101, "data", Types.StringType.get())
    );
    List<Record> expectedRepeating = Collections.nCopies(100, RandomGenericData.generate(structSchema, 1, 0L).get(0));

    writeAndValidateRecords(structSchema, expectedRepeating);
  }

  @Test
  public void writeAndValidateTimestamps() throws IOException {
    Schema timestampSchema = new Schema(
        required(1, "tsTzCol", Types.TimestampType.withZone()),
        required(2, "tsCol", Types.TimestampType.withoutZone())
    );

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

    try (FileAppender<Record> writer = ORC.write(Files.localOutput(testFile))
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
    try (CloseableIterable<Record> reader = ORC.read(Files.localInput(testFile))
        .project(timestampSchema)
        .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(timestampSchema, fileSchema))
        .build()) {
      rows = Lists.newArrayList(reader);
    }

    Assert.assertEquals(OffsetDateTime.parse("2017-01-17T01:10:34Z"), rows.get(0).getField("tsTzCol"));
    Assert.assertEquals(LocalDateTime.parse("1970-01-01T00:01:00"), rows.get(0).getField("tsCol"));
    Assert.assertEquals(OffsetDateTime.parse("2017-05-17T01:10:34Z"), rows.get(1).getField("tsTzCol"));
    Assert.assertEquals(LocalDateTime.parse("1970-05-01T00:01:00"), rows.get(1).getField("tsCol"));
    Assert.assertEquals(OffsetDateTime.parse("1935-01-17T01:10:34Z"), rows.get(2).getField("tsTzCol"));
    Assert.assertEquals(LocalDateTime.parse("1935-01-01T00:01:00"), rows.get(2).getField("tsCol"));
    Assert.assertEquals(OffsetDateTime.parse("1935-05-17T01:10:34Z"), rows.get(3).getField("tsTzCol"));
    Assert.assertEquals(LocalDateTime.parse("1935-05-01T00:01:00"), rows.get(3).getField("tsCol"));
  }

  private void writeAndValidateRecords(Schema schema, List<Record> expected) throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer = ORC.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc(GenericOrcWriter::buildWriter)
        .build()) {
      for (Record rec : expected) {
        writer.add(rec);
      }
    }

    List<Record> rows;
    try (CloseableIterable<Record> reader = ORC.read(Files.localInput(testFile))
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
