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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
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
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionTestHelpers;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestEncryptedAvroFileSplit {
  private static final Schema SCHEMA =
      new Schema(
          NestedField.required(1, "id", Types.LongType.get()),
          NestedField.required(2, "data", Types.StringType.get()));

  private static final EncryptionManager ENCRYPTION_MANAGER =
      EncryptionTestHelpers.createEncryptionManager();

  private static final int NUM_RECORDS = 100_000;

  @TempDir Path temp;

  public List<Record> expected = null;
  public InputFile file = null;

  @BeforeEach
  public void writeDataFile() throws IOException {
    this.expected = Lists.newArrayList();

    OutputFile out = Files.localOutput(temp.toFile());
    EncryptedOutputFile eOut = ENCRYPTION_MANAGER.encrypt(out);

    long plainContentLength;
    try (FileAppender<Object> writer =
        Avro.write(eOut)
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

      writer.close();
      plainContentLength = writer.length();
    }

    InputFile in = Files.localInput(temp.toFile(), plainContentLength);
    EncryptedInputFile encryptedIn = EncryptedFiles.encryptedInput(in, eOut.keyMetadata());
    this.file = ENCRYPTION_MANAGER.decrypt(encryptedIn);
  }

  @Test
  public void testSplitDataSkipping() throws IOException {
    long end = file.getLength();
    long splitLocation = end / 2;

    List<Record> firstHalf = readAvro(file, SCHEMA, 0, splitLocation);
    assertThat(firstHalf.size()).as("First split should not be empty").isNotEqualTo(0);

    List<Record> secondHalf = readAvro(file, SCHEMA, splitLocation + 1, end - splitLocation - 1);
    assertThat(secondHalf.size()).as("Second split should not be empty").isNotEqualTo(0);

    assertThat(firstHalf.size() + secondHalf.size())
        .as("Total records should match expected")
        .isEqualTo(expected.size());

    for (int i = 0; i < firstHalf.size(); i += 1) {
      assertThat(firstHalf.get(i)).isEqualTo(expected.get(i));
    }

    for (int i = 0; i < secondHalf.size(); i += 1) {
      assertThat(secondHalf.get(i)).isEqualTo(expected.get(firstHalf.size() + i));
    }
  }

  @Test
  public void testPosField() throws IOException {
    Schema projection =
        new Schema(SCHEMA.columns().get(0), MetadataColumns.ROW_POSITION, SCHEMA.columns().get(1));

    List<Record> records = readAvro(file, projection, 0, file.getLength());

    for (int i = 0; i < expected.size(); i += 1) {
      assertThat(records.get(i).getField(MetadataColumns.ROW_POSITION.name()))
          .as("Field _pos should match")
          .isEqualTo((long) i);

      assertThat(records.get(i).getField("id"))
          .as("Field id should match")
          .isEqualTo(expected.get(i).getField("id"));

      assertThat(records.get(i).getField("data"))
          .as("Field data should match")
          .isEqualTo(expected.get(i).getField("data"));
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
    assertThat(secondHalf.size()).as("Second split should not be empty").isNotEqualTo(0);

    List<Record> firstHalf = readAvro(file, projection, 0, splitLocation);
    assertThat(firstHalf.size()).as("First split should not be empty").isNotEqualTo(0);

    assertThat(firstHalf.size() + secondHalf.size())
        .as("Total records should match expected")
        .isEqualTo(expected.size());

    for (int i = 0; i < firstHalf.size(); i += 1) {
      assertThat(firstHalf.get(i).getField(MetadataColumns.ROW_POSITION.name()))
          .as("Field _pos should match")
          .isEqualTo((long) i);
      assertThat(firstHalf.get(i).getField("id"))
          .as("Field id should match")
          .isEqualTo(expected.get(i).getField("id"));
      assertThat(firstHalf.get(i).getField("data"))
          .as("Field data should match")
          .isEqualTo(expected.get(i).getField("data"));
    }

    for (int i = 0; i < secondHalf.size(); i += 1) {
      assertThat(secondHalf.get(i).getField(MetadataColumns.ROW_POSITION.name()))
          .as("Field _pos should match")
          .isEqualTo((long) (firstHalf.size() + i));
      assertThat(secondHalf.get(i).getField("id"))
          .as("Field id should match")
          .isEqualTo(expected.get(firstHalf.size() + i).getField("id"));
      assertThat(secondHalf.get(i).getField("data"))
          .as("Field data should match")
          .isEqualTo(expected.get(firstHalf.size() + i).getField("data"));
    }
  }

  @Test
  public void testPosWithEOFSplit() throws IOException {
    Schema projection =
        new Schema(SCHEMA.columns().get(0), MetadataColumns.ROW_POSITION, SCHEMA.columns().get(1));

    long end = file.getLength();

    List<Record> records = readAvro(file, projection, end - 10, 10);
    assertThat(records.size()).as("Should not read any records").isEqualTo(0);
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
