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
package org.apache.iceberg.avro;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAvroFileSplit {
  private static final Schema SCHEMA =
      new Schema(
          NestedField.required(1, "id", Types.LongType.get()),
          NestedField.required(2, "data", Types.StringType.get()));

  private static final int NUM_RECORDS = 100_000;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  public List<Record> expected = null;
  public InputFile file = null;

  @Before
  public void writeDataFile() throws IOException {
    this.expected = Lists.newArrayList();

    OutputFile out = Files.localOutput(temp.newFile());

    try (FileAppender<Object> writer =
        Avro.write(out)
            .set(TableProperties.AVRO_COMPRESSION, "uncompressed")
            .createWriterFunc(DataWriter::create)
            .schema(SCHEMA)
            .overwrite()
            .build()) {

      Record record = GenericRecord.create(SCHEMA);
      for (long i = 0; i < NUM_RECORDS; i += 1) {
        Record next = record.copy(ImmutableMap.of("id", i, "data", UUID.randomUUID().toString()));
        expected.add(next);
        writer.add(next);
      }
    }

    this.file = out.toInputFile();
  }

  @Test
  public void testSplitDataSkipping() throws IOException {
    long end = file.getLength();
    long splitLocation = end / 2;

    List<Record> firstHalf = readAvro(file, SCHEMA, 0, splitLocation);
    Assert.assertNotEquals("First split should not be empty", 0, firstHalf.size());

    List<Record> secondHalf = readAvro(file, SCHEMA, splitLocation + 1, end - splitLocation - 1);
    Assert.assertNotEquals("Second split should not be empty", 0, secondHalf.size());

    Assert.assertEquals(
        "Total records should match expected",
        expected.size(),
        firstHalf.size() + secondHalf.size());

    for (int i = 0; i < firstHalf.size(); i += 1) {
      Assert.assertEquals(expected.get(i), firstHalf.get(i));
    }

    for (int i = 0; i < secondHalf.size(); i += 1) {
      Assert.assertEquals(expected.get(firstHalf.size() + i), secondHalf.get(i));
    }
  }

  @Test
  public void testPosField() throws IOException {
    Schema projection =
        new Schema(SCHEMA.columns().get(0), MetadataColumns.ROW_POSITION, SCHEMA.columns().get(1));

    List<Record> records = readAvro(file, projection, 0, file.getLength());

    for (int i = 0; i < expected.size(); i += 1) {
      Assert.assertEquals(
          "Field _pos should match",
          (long) i,
          records.get(i).getField(MetadataColumns.ROW_POSITION.name()));
      Assert.assertEquals(
          "Field id should match", expected.get(i).getField("id"), records.get(i).getField("id"));
      Assert.assertEquals(
          "Field data should match",
          expected.get(i).getField("data"),
          records.get(i).getField("data"));
    }
  }

  @Test
  public void testPosFieldWithSplits() throws IOException {
    Schema projection =
        new Schema(SCHEMA.columns().get(0), MetadataColumns.ROW_POSITION, SCHEMA.columns().get(1));

    long end = file.getLength();
    long splitLocation = end / 2;

    List<Record> secondHalf =
        readAvro(file, projection, splitLocation + 1, end - splitLocation - 1);
    Assert.assertNotEquals("Second split should not be empty", 0, secondHalf.size());

    List<Record> firstHalf = readAvro(file, projection, 0, splitLocation);
    Assert.assertNotEquals("First split should not be empty", 0, firstHalf.size());

    Assert.assertEquals(
        "Total records should match expected",
        expected.size(),
        firstHalf.size() + secondHalf.size());

    for (int i = 0; i < firstHalf.size(); i += 1) {
      Assert.assertEquals(
          "Field _pos should match",
          (long) i,
          firstHalf.get(i).getField(MetadataColumns.ROW_POSITION.name()));
      Assert.assertEquals(
          "Field id should match", expected.get(i).getField("id"), firstHalf.get(i).getField("id"));
      Assert.assertEquals(
          "Field data should match",
          expected.get(i).getField("data"),
          firstHalf.get(i).getField("data"));
    }

    for (int i = 0; i < secondHalf.size(); i += 1) {
      Assert.assertEquals(
          "Field _pos should match",
          (long) (firstHalf.size() + i),
          secondHalf.get(i).getField(MetadataColumns.ROW_POSITION.name()));
      Assert.assertEquals(
          "Field id should match",
          expected.get(firstHalf.size() + i).getField("id"),
          secondHalf.get(i).getField("id"));
      Assert.assertEquals(
          "Field data should match",
          expected.get(firstHalf.size() + i).getField("data"),
          secondHalf.get(i).getField("data"));
    }
  }

  @Test
  public void testPosWithEOFSplit() throws IOException {
    Schema projection =
        new Schema(SCHEMA.columns().get(0), MetadataColumns.ROW_POSITION, SCHEMA.columns().get(1));

    long end = file.getLength();

    List<Record> records = readAvro(file, projection, end - 10, 10);
    Assert.assertEquals("Should not read any records", 0, records.size());
  }

  public List<Record> readAvro(InputFile in, Schema projection, long start, long length)
      throws IOException {
    try (AvroIterable<Record> reader =
        Avro.read(in)
            .createReaderFunc(DataReader::create)
            .split(start, length)
            .project(projection)
            .build()) {
      return Lists.newArrayList(reader);
    }
  }
}
