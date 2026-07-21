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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

public class TestTrackingFile {

  /** In-memory OutputFile backed by a ByteArrayOutputStream. */
  private static class InMemoryOutputFile implements OutputFile {
    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final String location;

    InMemoryOutputFile(String location) {
      this.location = location;
    }

    @Override
    public PositionOutputStream create() {
      return new PositionOutputStream() {
        private long pos = 0;

        @Override
        public long getPos() {
          return pos;
        }

        @Override
        public void write(int b) throws IOException {
          buffer.write(b);
          pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          buffer.write(b, off, len);
          pos += len;
        }
      };
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      buffer.reset();
      return create();
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public InputFile toInputFile() {
      return new InMemoryInputFile(location, buffer.toByteArray());
    }
  }

  /** In-memory InputFile backed by a byte array. */
  private static class InMemoryInputFile implements InputFile {
    private final String location;
    private final byte[] data;

    InMemoryInputFile(String location, byte[] data) {
      this.location = location;
      this.data = data;
    }

    @Override
    public long getLength() {
      return data.length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new SeekableInputStream() {
        private final InputStream delegate = new ByteArrayInputStream(data);
        private long pos = 0;

        @Override
        public long getPos() {
          return pos;
        }

        @Override
        public void seek(long newPos) throws IOException {
          throw new UnsupportedOperationException("seek not supported in test");
        }

        @Override
        public int read() throws IOException {
          int b = delegate.read();
          if (b >= 0) pos++;
          return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          int n = delegate.read(b, off, len);
          if (n > 0) pos += n;
          return n;
        }
      };
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return true;
    }
  }

  private List<TrackingFileEntry> writeAndRead(List<TrackingFileEntry> entries) {
    InMemoryOutputFile outputFile = new InMemoryOutputFile("test://tracking.avro");
    try (TrackingFileWriter writer = new TrackingFileWriter(outputFile)) {
      writer.addAll(entries);
    }
    return TrackingFileReader.readAll(outputFile.toInputFile());
  }

  @Test
  void roundTripSingleEntry() {
    TrackingFileEntry entry =
        TrackingFileEntry.builder()
            .location("s3://warehouse/db/orders/index/leaf-00001.parquet")
            .fileFormat("parquet")
            .recordCount(1000L)
            .fileSizeInBytes(204800L)
            .transformValueLowerBound(0L)
            .transformValueUpperBound(63L)
            .build();

    List<TrackingFileEntry> restored = writeAndRead(List.of(entry));

    assertThat(restored).hasSize(1);
    TrackingFileEntry r = restored.get(0);
    assertThat(r.location()).isEqualTo(entry.location());
    assertThat(r.fileFormat()).isEqualTo("parquet");
    assertThat(r.recordCount()).isEqualTo(1000L);
    assertThat(r.fileSizeInBytes()).isEqualTo(204800L);
    assertThat(r.transformValueLowerBound()).isEqualTo(0L);
    assertThat(r.transformValueUpperBound()).isEqualTo(63L);
    assertThat(r.keyMetadata()).isNull();
  }

  @Test
  void roundTripMultipleEntries() {
    List<TrackingFileEntry> entries = List.of(
        TrackingFileEntry.builder()
            .location("s3://.../leaf-0.parquet").recordCount(500)
            .fileSizeInBytes(1024).transformValueLowerBound(0).transformValueUpperBound(63).build(),
        TrackingFileEntry.builder()
            .location("s3://.../leaf-1.parquet").recordCount(600)
            .fileSizeInBytes(2048).transformValueLowerBound(64).transformValueUpperBound(127).build(),
        TrackingFileEntry.builder()
            .location("s3://.../leaf-2.parquet").recordCount(700)
            .fileSizeInBytes(3072).transformValueLowerBound(128).transformValueUpperBound(255).build()
    );

    List<TrackingFileEntry> restored = writeAndRead(entries);
    assertThat(restored).hasSize(3);
    assertThat(restored.get(0).transformValueLowerBound()).isEqualTo(0L);
    assertThat(restored.get(1).transformValueLowerBound()).isEqualTo(64L);
    assertThat(restored.get(2).transformValueUpperBound()).isEqualTo(255L);
  }

  @Test
  void readMatchingFiltersCorrectly() {
    List<TrackingFileEntry> entries = List.of(
        TrackingFileEntry.builder().location("leaf-0.parquet").recordCount(100)
            .fileSizeInBytes(1024).transformValueLowerBound(0).transformValueUpperBound(63).build(),
        TrackingFileEntry.builder().location("leaf-1.parquet").recordCount(100)
            .fileSizeInBytes(1024).transformValueLowerBound(64).transformValueUpperBound(127).build(),
        TrackingFileEntry.builder().location("leaf-2.parquet").recordCount(100)
            .fileSizeInBytes(1024).transformValueLowerBound(128).transformValueUpperBound(255).build()
    );

    InMemoryOutputFile outputFile = new InMemoryOutputFile("test://tracking.avro");
    try (TrackingFileWriter writer = new TrackingFileWriter(outputFile)) {
      writer.addAll(entries);
    }
    InputFile inputFile = outputFile.toInputFile();

    // Query for bucket 42 — only leaf-0 (0..63) should match
    List<TrackingFileEntry> matches = TrackingFileReader.readMatching(inputFile, 42, 42);
    assertThat(matches).hasSize(1);
    assertThat(matches.get(0).location()).isEqualTo("leaf-0.parquet");

    // Query for range 60..70 — leaf-0 (0..63) and leaf-1 (64..127) both overlap
    List<TrackingFileEntry> rangeMatches = TrackingFileReader.readMatching(inputFile, 60, 70);
    assertThat(rangeMatches).hasSize(2);

    // Query for bucket 300 — no match
    List<TrackingFileEntry> noMatches = TrackingFileReader.readMatching(inputFile, 300, 300);
    assertThat(noMatches).isEmpty();
  }

  @Test
  void writerCountsEntries() {
    InMemoryOutputFile outputFile = new InMemoryOutputFile("test://tracking.avro");
    try (TrackingFileWriter writer = new TrackingFileWriter(outputFile)) {
      writer.add(TrackingFileEntry.builder().location("a.parquet").recordCount(1)
          .fileSizeInBytes(100).transformValueLowerBound(0).transformValueUpperBound(10).build());
      writer.add(TrackingFileEntry.builder().location("b.parquet").recordCount(2)
          .fileSizeInBytes(200).transformValueLowerBound(11).transformValueUpperBound(20).build());
      assertThat(writer.entryCount()).isEqualTo(2);
    }
  }
}
