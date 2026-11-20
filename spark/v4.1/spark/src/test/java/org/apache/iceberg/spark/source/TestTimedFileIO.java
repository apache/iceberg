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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestTimedFileIO {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));
  private static final int NUM_RECORDS = 1000;

  @TempDir private File temp;

  @ParameterizedTest
  @EnumSource(
      value = FileFormat.class,
      names = {"PARQUET", "ORC", "AVRO"})
  void timesWritesThroughHadoopFileIO(FileFormat format) throws IOException {
    FileIO io = new HadoopFileIO(new Configuration());
    assertTimesWrite(io, location(format.addExtension("data")), format);
  }

  @Test
  void keepsCapabilitiesOfHadoopStreams() throws IOException {
    // the raw local FS supports hflush and hsync, the checksummed default does not
    Configuration conf = new Configuration();
    conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
    conf.setBoolean("fs.file.impl.disable.cache", true);
    FileIO io = new HadoopFileIO(conf);
    HadoopOutputFile untimed = (HadoopOutputFile) io.newOutputFile(location("untimed"));
    HadoopOutputFile timed =
        (HadoopOutputFile)
            new TimedFileIO(io, new WriteTimer(0, 0L)).newOutputFile(location("timed"));

    try (FSDataOutputStream expected = untimed.getFileSystem().create(untimed.getPath());
        FSDataOutputStream actual = timed.getFileSystem().create(timed.getPath())) {
      assertThat(expected.hasCapability(StreamCapabilities.HFLUSH)).isTrue();
      assertThat(actual.hasCapability(StreamCapabilities.HFLUSH)).isTrue();
      assertThat(actual.hasCapability(StreamCapabilities.HSYNC))
          .isEqualTo(expected.hasCapability(StreamCapabilities.HSYNC));
    }
  }

  private String location(String name) {
    return new File(temp, name).toURI().toString();
  }

  private static void assertTimesWrite(FileIO io, String location, FileFormat format)
      throws IOException {
    WriteTimer timer = new WriteTimer(0, 0L);
    List<Record> records = RandomGenericData.generate(SCHEMA, NUM_RECORDS, 0L);

    DataWriter<Record> writer =
        new GenericFileWriterFactory.Builder()
            .dataSchema(SCHEMA)
            .dataFileFormat(format)
            .build()
            .newDataWriter(
                EncryptedFiles.plainAsEncryptedOutput(
                    new TimedFileIO(io, timer).newOutputFile(location)),
                PartitionSpec.unpartitioned(),
                null);
    try (writer) {
      records.forEach(writer::write);
    }

    InputFile written = io.newInputFile(location);
    assertThat(timer.storageWriteBytes()).isEqualTo(written.getLength());
    assertThat(timer.storageWriteNanos()).isGreaterThan(0L);

    try (CloseableIterable<Record> read =
        FormatModelRegistry.readBuilder(format, Record.class, written).project(SCHEMA).build()) {
      assertThat(Lists.newArrayList(read)).hasSize(NUM_RECORDS);
    }
  }
}
