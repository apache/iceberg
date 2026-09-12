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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestManifestMergeManager extends TestBase {
  private static final int MANIFEST_COUNT = 100;
  private static final int ENTRIES_PER_MANIFEST = 20;
  private static final int WORKER_THREADS = 4;
  private static final long CURRENT_SNAPSHOT_ID = 1_000_000L;
  private static final String INJECTED_FAILURE = "injected read failure";

  @TestTemplate
  public void concurrentMergeMatchesSequentialMerge() throws IOException {
    List<ManifestFile> bin = writeBin();
    // the bin holds more entries than the read-ahead bound
    assertThat(bin.stream().mapToLong(TestManifestMergeManager::entryCount).sum())
        .isGreaterThan(ManifestMergeManager.MAX_READ_AHEAD_ENTRIES);

    assertConcurrentMergeMatchesSequentialMerge(bin);
  }

  @TestTemplate
  public void mergeStreamsManifestLargerThanReadAheadBound() throws IOException {
    List<ManifestFile> bin = writeBin();
    int largeEntryCount = (int) ManifestMergeManager.MAX_READ_AHEAD_ENTRIES + 1;
    bin.add(MANIFEST_COUNT / 2, writePastManifest(MANIFEST_COUNT, largeEntryCount));

    assertConcurrentMergeMatchesSequentialMerge(bin);
  }

  @TestTemplate
  public void mergeStreamsManifestWithUnknownEntryCounts() throws IOException {
    List<ManifestFile> bin = writeBin();
    for (int index = 0; index < bin.size(); index += 10) {
      bin.set(index, withoutEntryCounts(bin.get(index)));
    }

    assertConcurrentMergeMatchesSequentialMerge(bin);
  }

  @TestTemplate
  public void readsManifestsOfBinConcurrently() throws IOException {
    List<ManifestFile> bin = writeBin();
    StreamCountingFileIO countingIO = new StreamCountingFileIO(FILE_IO, null);

    ExecutorService workerPool = Executors.newFixedThreadPool(WORKER_THREADS);
    try {
      Iterables.getOnlyElement(mergeManager(countingIO, workerPool).mergeManifests(bin));
    } finally {
      workerPool.shutdownNow();
    }

    assertThat(countingIO.openStreams()).isEqualTo(0);
    assertThat(countingIO.maxOpenStreams()).isGreaterThan(1).isLessThanOrEqualTo(WORKER_THREADS);
  }

  @TestTemplate
  public void readFailurePropagatesAndClosesStreams() throws Exception {
    List<ManifestFile> bin = writeBin();
    StreamCountingFileIO countingIO =
        new StreamCountingFileIO(FILE_IO, bin.get(MANIFEST_COUNT / 2).path());

    ExecutorService workerPool = Executors.newFixedThreadPool(WORKER_THREADS);
    try {
      assertThatThrownBy(() -> mergeManager(countingIO, workerPool).mergeManifests(bin))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(INJECTED_FAILURE);
    } finally {
      workerPool.shutdown();
      assertThat(workerPool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(countingIO.openStreams()).isEqualTo(0);
  }

  private void assertConcurrentMergeMatchesSequentialMerge(List<ManifestFile> bin)
      throws IOException {
    ManifestFile expected = mergeSequentially(bin);

    ExecutorService workerPool = Executors.newFixedThreadPool(WORKER_THREADS);
    try {
      ManifestFile merged =
          Iterables.getOnlyElement(mergeManager(FILE_IO, workerPool).mergeManifests(bin));
      assertManifestsEquivalent(expected, merged);
    } finally {
      workerPool.shutdownNow();
    }
  }

  private ManifestMergeManager<DataFile> mergeManager(FileIO io, ExecutorService workerPool) {
    return new ManifestMergeManager<DataFile>(Long.MAX_VALUE, 2, true, () -> workerPool) {
      @Override
      protected long snapshotId() {
        return CURRENT_SNAPSHOT_ID;
      }

      @Override
      protected PartitionSpec spec(int specId) {
        return table.specs().get(specId);
      }

      @Override
      protected void deleteFile(String location) {
        io.deleteFile(location);
      }

      @Override
      protected ManifestWriter<DataFile> newManifestWriter(PartitionSpec manifestSpec) {
        return TestManifestMergeManager.this.newManifestWriter(io, manifestSpec);
      }

      @Override
      protected ManifestReader<DataFile> newManifestReader(ManifestFile manifest) {
        return newManifestReader(manifest, true);
      }

      @Override
      protected ManifestReader<DataFile> newManifestReader(
          ManifestFile manifest, boolean isCommitted) {
        return ManifestFiles.read(manifest, io, table.specs(), isCommitted);
      }
    };
  }

  private ManifestWriter<DataFile> newManifestWriter(FileIO io, PartitionSpec manifestSpec) {
    String fileName = manifestFormat().addExtension("merged-" + System.nanoTime());
    OutputFile outputFile = io.newOutputFile(temp.resolve(fileName).toString());
    return ManifestFiles.write(formatVersion, manifestSpec, outputFile, CURRENT_SNAPSHOT_ID);
  }

  // one manifest per past snapshot plus one for the current snapshot, mixing all entry statuses
  private List<ManifestFile> writeBin() throws IOException {
    List<ManifestFile> bin = Lists.newArrayList();
    for (int index = 0; index < MANIFEST_COUNT; index += 1) {
      bin.add(writePastManifest(index, ENTRIES_PER_MANIFEST));
    }

    bin.add(
        writeManifest(
            CURRENT_SNAPSHOT_ID,
            manifestFormat().addExtension("input-current"),
            manifestEntry(Status.ADDED, CURRENT_SNAPSHOT_ID, newDataFile(0)),
            manifestEntry(Status.ADDED, CURRENT_SNAPSHOT_ID, newDataFile(1)),
            manifestEntry(Status.DELETED, CURRENT_SNAPSHOT_ID, 1L, 1L, newDataFile(2))));

    return bin;
  }

  private ManifestFile writePastManifest(int index, int entryCount) throws IOException {
    long snapshotId = index + 1;
    long sequenceNumber = index + 1;
    List<ManifestEntry<?>> entries = Lists.newArrayList();
    for (int entryIndex = 0; entryIndex < entryCount; entryIndex += 1) {
      DataFile file = newDataFile(entryIndex);
      switch (entryIndex % 3) {
        case 0 ->
            entries.add(
                manifestEntry(Status.ADDED, snapshotId, sequenceNumber, sequenceNumber, file));
        case 1 ->
            entries.add(
                manifestEntry(
                    Status.EXISTING, snapshotId - 1, sequenceNumber, sequenceNumber, file));
        default ->
            entries.add(
                manifestEntry(Status.DELETED, snapshotId, sequenceNumber, sequenceNumber, file));
      }
    }

    String fileName = manifestFormat().addExtension("input-" + index);
    return writeManifest(snapshotId, fileName, entries.toArray(new ManifestEntry<?>[0]));
  }

  // v1 manifest lists may omit the entry counts of a manifest
  private static ManifestFile withoutEntryCounts(ManifestFile manifest) {
    return new GenericManifestFile(
        manifest.path(),
        manifest.length(),
        manifest.partitionSpecId(),
        manifest.content(),
        manifest.sequenceNumber(),
        manifest.minSequenceNumber(),
        manifest.snapshotId(),
        manifest.partitions(),
        manifest.keyMetadata(),
        null,
        null,
        null,
        null,
        null,
        null,
        manifest.firstRowId());
  }

  private static long entryCount(ManifestFile manifest) {
    return (long) manifest.addedFilesCount()
        + manifest.existingFilesCount()
        + manifest.deletedFilesCount();
  }

  private DataFile newDataFile(int index) {
    return FileGenerationUtil.generateDataFile(table, Row.of(index % BUCKETS_NUMBER));
  }

  private ManifestFile mergeSequentially(List<ManifestFile> bin) throws IOException {
    ManifestWriter<DataFile> writer = newManifestWriter(FILE_IO, table.spec());
    try {
      for (ManifestFile manifest : bin) {
        boolean isCommitted =
            manifest.snapshotId() != null && CURRENT_SNAPSHOT_ID != manifest.snapshotId();
        try (ManifestReader<DataFile> reader =
            ManifestFiles.read(manifest, FILE_IO, table.specs(), isCommitted)) {
          for (ManifestEntry<DataFile> entry : reader.entries()) {
            if (entry.status() == Status.DELETED) {
              if (entry.snapshotId() == CURRENT_SNAPSHOT_ID) {
                writer.delete(entry);
              }
            } else if (entry.status() == Status.ADDED
                && entry.snapshotId() == CURRENT_SNAPSHOT_ID) {
              writer.add(entry);
            } else {
              writer.existing(entry);
            }
          }
        }
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  private void assertManifestsEquivalent(ManifestFile expected, ManifestFile actual) {
    assertThat(actual.length()).isEqualTo(expected.length());
    assertThat(actual.addedFilesCount()).isEqualTo(expected.addedFilesCount());
    assertThat(actual.existingFilesCount()).isEqualTo(expected.existingFilesCount());
    assertThat(actual.deletedFilesCount()).isEqualTo(expected.deletedFilesCount());
    assertThat(actual.addedRowsCount()).isEqualTo(expected.addedRowsCount());
    assertThat(actual.existingRowsCount()).isEqualTo(expected.existingRowsCount());
    assertThat(actual.deletedRowsCount()).isEqualTo(expected.deletedRowsCount());
    assertThat(actual.minSequenceNumber()).isEqualTo(expected.minSequenceNumber());
    assertThat(actual.partitions()).hasSameSizeAs(expected.partitions());
    for (int index = 0; index < expected.partitions().size(); index += 1) {
      ManifestFile.PartitionFieldSummary expectedSummary = expected.partitions().get(index);
      ManifestFile.PartitionFieldSummary actualSummary = actual.partitions().get(index);
      assertThat(actualSummary.containsNull()).isEqualTo(expectedSummary.containsNull());
      assertThat(actualSummary.containsNaN()).isEqualTo(expectedSummary.containsNaN());
      assertThat(actualSummary.lowerBound()).isEqualTo(expectedSummary.lowerBound());
      assertThat(actualSummary.upperBound()).isEqualTo(expectedSummary.upperBound());
    }

    List<ManifestEntry<DataFile>> expectedEntries = readEntries(expected);
    List<ManifestEntry<DataFile>> actualEntries = readEntries(actual);
    assertThat(actualEntries).hasSameSizeAs(expectedEntries);
    assertThat(expectedEntries).extracting(ManifestEntry::status).contains(Status.ADDED);
    assertThat(expectedEntries).extracting(ManifestEntry::status).contains(Status.EXISTING);
    assertThat(expectedEntries).extracting(ManifestEntry::status).contains(Status.DELETED);

    for (int index = 0; index < expectedEntries.size(); index += 1) {
      ManifestEntry<DataFile> expectedEntry = expectedEntries.get(index);
      ManifestEntry<DataFile> actualEntry = actualEntries.get(index);
      assertThat(actualEntry.status()).isEqualTo(expectedEntry.status());
      assertThat(actualEntry.snapshotId()).isEqualTo(expectedEntry.snapshotId());
      assertThat(actualEntry.dataSequenceNumber()).isEqualTo(expectedEntry.dataSequenceNumber());
      assertThat(actualEntry.fileSequenceNumber()).isEqualTo(expectedEntry.fileSequenceNumber());

      DataFile expectedFile = expectedEntry.file();
      DataFile actualFile = actualEntry.file();
      assertThat(actualFile.location()).isEqualTo(expectedFile.location());
      assertThat(actualFile.partition()).isEqualTo(expectedFile.partition());
      assertThat(actualFile.recordCount()).isEqualTo(expectedFile.recordCount());
      assertThat(actualFile.fileSizeInBytes()).isEqualTo(expectedFile.fileSizeInBytes());
      assertThat(actualFile.columnSizes()).isEqualTo(expectedFile.columnSizes());
      assertThat(actualFile.valueCounts()).isEqualTo(expectedFile.valueCounts());
      assertThat(actualFile.nullValueCounts()).isEqualTo(expectedFile.nullValueCounts());
      assertThat(actualFile.nanValueCounts()).isEqualTo(expectedFile.nanValueCounts());
      assertThat(actualFile.lowerBounds()).isEqualTo(expectedFile.lowerBounds());
      assertThat(actualFile.upperBounds()).isEqualTo(expectedFile.upperBounds());
      assertThat(actualFile.splitOffsets()).isEqualTo(expectedFile.splitOffsets());
      assertThat(actualFile.sortOrderId()).isEqualTo(expectedFile.sortOrderId());
      assertThat(actualFile.firstRowId()).isEqualTo(expectedFile.firstRowId());
    }
  }

  private List<ManifestEntry<DataFile>> readEntries(ManifestFile manifest) {
    List<ManifestEntry<DataFile>> entries = Lists.newArrayList();
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, table.specs())) {
      for (ManifestEntry<DataFile> entry : reader.entries()) {
        entries.add(entry.copy());
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }

    return entries;
  }

  /** Tracks the highest number of input streams open at the same time and fails one path. */
  private static class StreamCountingFileIO implements FileIO {
    private final FileIO delegate;
    private final String failingPath;
    private final AtomicInteger openStreams = new AtomicInteger();
    private final AtomicInteger maxOpenStreams = new AtomicInteger();

    StreamCountingFileIO(FileIO delegate, String failingPath) {
      this.delegate = delegate;
      this.failingPath = failingPath;
    }

    int openStreams() {
      return openStreams.get();
    }

    int maxOpenStreams() {
      return maxOpenStreams.get();
    }

    @Override
    public InputFile newInputFile(String path) {
      if (path.equals(failingPath)) {
        throw new IllegalStateException(INJECTED_FAILURE);
      }

      return new StreamCountingInputFile(delegate.newInputFile(path));
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    private class StreamCountingInputFile implements InputFile {
      private final InputFile delegate;

      StreamCountingInputFile(InputFile delegate) {
        this.delegate = delegate;
      }

      @Override
      public long getLength() {
        return delegate.getLength();
      }

      @Override
      public SeekableInputStream newStream() {
        maxOpenStreams.accumulateAndGet(openStreams.incrementAndGet(), Math::max);
        // hold the stream open long enough for concurrent reads to overlap
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        return new StreamCountingInputStream(delegate.newStream());
      }

      @Override
      public String location() {
        return delegate.location();
      }

      @Override
      public boolean exists() {
        return delegate.exists();
      }
    }

    private class StreamCountingInputStream extends SeekableInputStream {
      private final SeekableInputStream delegate;
      private boolean closed = false;

      StreamCountingInputStream(SeekableInputStream delegate) {
        this.delegate = delegate;
      }

      @Override
      public long getPos() throws IOException {
        return delegate.getPos();
      }

      @Override
      public void seek(long newPos) throws IOException {
        delegate.seek(newPos);
      }

      @Override
      public int read() throws IOException {
        return delegate.read();
      }

      @Override
      public int read(byte[] buffer, int offset, int length) throws IOException {
        return delegate.read(buffer, offset, length);
      }

      @Override
      public void close() throws IOException {
        if (!closed) {
          closed = true;
          openStreams.decrementAndGet();
        }

        delegate.close();
      }
    }
  }
}
