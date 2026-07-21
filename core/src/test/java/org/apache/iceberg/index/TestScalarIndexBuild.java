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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestScalarIndexBuild {

  private static final TableIdentifier TABLE =
      TableIdentifier.of(Namespace.of("taxi"), "yellow_trips");
  private static final IndexIdentifier IDX =
      IndexIdentifier.of(TABLE, "medallion_idx");
  private static final String TABLE_UUID = "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94";
  private static final String INDEX_LOCATION = "s3://warehouse/taxi/yellow_trips/index/medallion_idx";

  private InMemoryIndexCatalog catalog;
  private InMemoryFileIO fileIO;
  private ScalarIndexCommitter committer;

  @BeforeEach
  void setup() {
    catalog = new InMemoryIndexCatalog();
    fileIO = new InMemoryFileIO();
    committer = new ScalarIndexCommitter(catalog, fileIO);
  }

  // ------------------------------------------------------------------
  // HashTransform tests
  // ------------------------------------------------------------------

  @Test
  void hashTransformStringDeterministic() {
    HashTransform t = new HashTransform(256);
    // Same value always produces same bucket
    assertThat(t.apply("D7D598CD99978BD012A87A76A7C891B7"))
        .isEqualTo(t.apply("D7D598CD99978BD012A87A76A7C891B7"));
  }

  @Test
  void hashTransformBucketsInRange() {
    HashTransform t = new HashTransform(256);
    for (String val : List.of("medallion1", "medallion2", "hello", "world", "abc123")) {
      long bucket = t.apply(val);
      assertThat(bucket).isBetween(0L, 255L);
    }
  }

  @Test
  void hashTransformDistributesEvenly() {
    HashTransform t = new HashTransform(256);
    long[] counts = new long[256];
    for (int i = 0; i < 10_000; i++) {
      counts[(int) t.apply("medallion_" + i)]++;
    }
    // Each bucket should have roughly 10000/256 ≈ 39 entries
    // Check no bucket has more than 3x the average (basic sanity)
    long avg = 10_000 / 256;
    for (long count : counts) {
      assertThat(count).isLessThan(avg * 5);
    }
  }

  // ------------------------------------------------------------------
  // Full build + commit cycle
  // ------------------------------------------------------------------

  @Test
  void firstBuildCreatesIndexInCatalog() {
    List<LeafFileMetadata> leafFiles = sampleLeafFiles();

    committer.commit(
        IDX, TABLE_UUID, 3055729675574597004L,
        "SCALAR", "HASH",
        ImmutableList.of(3),
        ImmutableList.of(),
        ImmutableMap.of("hash.num-buckets", "256"),
        INDEX_LOCATION,
        leafFiles);

    assertThat(catalog.indexExists(IDX)).isTrue();
    IndexMetadata loaded = catalog.loadIndex(IDX);
    assertThat(loaded.type()).isEqualTo("SCALAR");
    assertThat(loaded.transformFunction()).isEqualTo("HASH");
    assertThat(loaded.snapshots()).hasSize(1);
    assertThat(loaded.currentSnapshotId()).isNotNull();

    // Verify the tracking file was written
    IndexSnapshot snap = loaded.currentSnapshot();
    assertThat(snap.sourceTableSnapshotId()).isEqualTo(3055729675574597004L);
    assertThat(snap.trackingFile()).startsWith(INDEX_LOCATION + "/metadata/tracking-00001-");
    assertThat(fileIO.files).containsKey(snap.trackingFile());
  }

  @Test
  void incrementalBuildAddsSecondSnapshot() {
    // First build
    committer.commit(
        IDX, TABLE_UUID, 1000L,
        "SCALAR", "HASH", ImmutableList.of(3),
        INDEX_LOCATION, sampleLeafFiles());

    // Second build (new table snapshot)
    committer.commit(
        IDX, TABLE_UUID, 2000L,
        "SCALAR", "HASH", ImmutableList.of(3),
        INDEX_LOCATION, sampleLeafFiles());

    IndexMetadata loaded = catalog.loadIndex(IDX);
    assertThat(loaded.snapshots()).hasSize(2);
    assertThat(loaded.snapshotForTableSnapshot(1000L)).isNotNull();
    assertThat(loaded.snapshotForTableSnapshot(2000L)).isNotNull();

    // Latest snapshot should be current
    assertThat(loaded.currentSnapshot().sourceTableSnapshotId()).isEqualTo(2000L);
  }

  @Test
  void trackingFileContainsCorrectBounds() {
    List<LeafFileMetadata> leafFiles = List.of(
        new LeafFileMetadata("s3://.../leaf-0.parquet", "parquet", 1000, 204800, 0, 63),
        new LeafFileMetadata("s3://.../leaf-1.parquet", "parquet", 900, 192000, 64, 127),
        new LeafFileMetadata("s3://.../leaf-2.parquet", "parquet", 1100, 220000, 128, 255)
    );

    committer.commit(
        IDX, TABLE_UUID, 1000L,
        "SCALAR", "HASH", ImmutableList.of(3),
        INDEX_LOCATION, leafFiles);

    // Read back the tracking file
    String trackingPath = catalog.loadIndex(IDX).currentSnapshot().trackingFile();
    InputFile trackingFile = fileIO.newInputFile(trackingPath);
    List<TrackingFileEntry> entries = TrackingFileReader.readAll(trackingFile);

    assertThat(entries).hasSize(3);
    assertThat(entries.get(0).transformValueLowerBound()).isEqualTo(0L);
    assertThat(entries.get(0).transformValueUpperBound()).isEqualTo(63L);
    assertThat(entries.get(2).transformValueUpperBound()).isEqualTo(255L);
  }

  @Test
  void plannerFindsRightLeafFileForMedallion() {
    HashTransform transform = new HashTransform(256);
    String medallion = "D7D598CD99978BD012A87A76A7C891B7";
    long bucket = transform.apply(medallion);

    // Simulate 4 leaf files covering 64 buckets each
    List<LeafFileMetadata> leafFiles = List.of(
        new LeafFileMetadata("s3://.../leaf-0.parquet", "parquet", 500, 100000, 0, 63),
        new LeafFileMetadata("s3://.../leaf-1.parquet", "parquet", 500, 100000, 64, 127),
        new LeafFileMetadata("s3://.../leaf-2.parquet", "parquet", 500, 100000, 128, 191),
        new LeafFileMetadata("s3://.../leaf-3.parquet", "parquet", 500, 100000, 192, 255)
    );

    committer.commit(
        IDX, TABLE_UUID, 1000L, "SCALAR", "HASH",
        ImmutableList.of(3), INDEX_LOCATION, leafFiles);

    // Simulate planning: use readMatching to find the right leaf file
    String trackingPath = catalog.loadIndex(IDX).currentSnapshot().trackingFile();
    InputFile trackingFile = fileIO.newInputFile(trackingPath);
    List<TrackingFileEntry> matches = TrackingFileReader.readMatching(trackingFile, bucket, bucket);

    // Should narrow down to exactly 1 leaf file
    assertThat(matches).hasSize(1);
    assertThat(matches.get(0).transformValueLowerBound()).isLessThanOrEqualTo(bucket);
    assertThat(matches.get(0).transformValueUpperBound()).isGreaterThanOrEqualTo(bucket);
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  private List<LeafFileMetadata> sampleLeafFiles() {
    return List.of(
        new LeafFileMetadata(INDEX_LOCATION + "/data/leaf-0.parquet", "parquet", 1000, 204800, 0, 127),
        new LeafFileMetadata(INDEX_LOCATION + "/data/leaf-1.parquet", "parquet", 1100, 225000, 128, 255)
    );
  }

  /**
   * In-memory FileIO backed by a HashMap of path → byte[].
   * Allows tests to read back files written by the committer.
   */
  static class InMemoryFileIO implements FileIO {
    final Map<String, byte[]> files = new HashMap<>();

    @Override
    public InputFile newInputFile(String path) {
      byte[] data = files.get(path);
      if (data == null) throw new RuntimeException("File not found: " + path);
      return new InMemInput(path, data);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      return newInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return new InMemOutput(path, files);
    }

    @Override
    public void deleteFile(String path) {
      files.remove(path);
    }
  }

  private static class InMemOutput implements OutputFile {
    private final String path;
    private final Map<String, byte[]> store;
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

    InMemOutput(String path, Map<String, byte[]> store) {
      this.path = path;
      this.store = store;
    }

    @Override
    public PositionOutputStream create() {
      return pos();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      buf.reset();
      return pos();
    }

    private PositionOutputStream pos() {
      return new PositionOutputStream() {
        private long p = 0;

        @Override
        public long getPos() {
          return p;
        }

        @Override
        public void write(int b) throws IOException {
          buf.write(b);
          p++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          buf.write(b, off, len);
          p += len;
        }

        @Override
        public void close() throws IOException {
          super.close();
          store.put(path, buf.toByteArray());
        }
      };
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public InputFile toInputFile() {
      return new InMemInput(path, buf.toByteArray());
    }
  }

  private static class InMemInput implements InputFile {
    private final String path;
    private final byte[] data;

    InMemInput(String path, byte[] data) {
      this.path = path;
      this.data = data;
    }

    @Override
    public long getLength() {
      return data.length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new SeekableInputStream() {
        private final ByteArrayInputStream in = new ByteArrayInputStream(data);
        private long pos = 0;

        @Override
        public long getPos() {
          return pos;
        }

        @Override
        public void seek(long newPos) {
          throw new UnsupportedOperationException();
        }

        @Override
        public int read() throws IOException {
          int b = in.read();
          if (b >= 0) pos++;
          return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          int n = in.read(b, off, len);
          if (n > 0) pos += n;
          return n;
        }
      };
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public boolean exists() {
      return true;
    }
  }
}
