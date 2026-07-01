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

import static org.apache.iceberg.TableMetadata.newTableMetadata;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class TestTables {

  private TestTables() {}

  public static TestTable upgrade(File temp, String name, int newFormatVersion) {
    TestTable table = load(temp, name);
    TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    ops.commit(base, ops.current().upgradeToFormatVersion(newFormatVersion));
    return table;
  }

  public static TestTable create(
      File temp, String name, Schema schema, PartitionSpec spec, int formatVersion) {
    return create(temp, name, schema, spec, SortOrder.unsorted(), formatVersion);
  }

  public static TestTable create(
      File temp,
      String name,
      Schema schema,
      PartitionSpec spec,
      SortOrder sortOrder,
      int formatVersion) {
    return create(temp, null, name, schema, spec, sortOrder, formatVersion);
  }

  public static TestTable create(
      File temp,
      File metaTemp,
      String name,
      Schema schema,
      PartitionSpec spec,
      SortOrder sortOrder,
      int formatVersion) {
    TestTableOperations ops = new TestTableOperations(name, temp);

    return createTable(
        temp, metaTemp, name, schema, spec, formatVersion, ImmutableMap.of(), sortOrder, null, ops);
  }

  public static TestTable create(
      File temp,
      String name,
      Schema schema,
      PartitionSpec spec,
      SortOrder sortOrder,
      int formatVersion,
      TestTableOperations ops) {
    return createTable(
        temp, null, name, schema, spec, formatVersion, ImmutableMap.of(), sortOrder, null, ops);
  }

  public static TestTable create(
      File temp,
      String name,
      Schema schema,
      PartitionSpec spec,
      SortOrder sortOrder,
      int formatVersion,
      MetricsReporter reporter) {
    TestTableOperations ops = new TestTableOperations(name, temp);

    return createTable(
        temp, null, name, schema, spec, formatVersion, ImmutableMap.of(), sortOrder, reporter, ops);
  }

  public static TestTable create(
      File temp,
      String name,
      Schema schema,
      PartitionSpec spec,
      int formatVersion,
      Map<String, String> properties) {
    TestTableOperations ops = new TestTableOperations(name, temp);

    return createTable(
        temp, null, name, schema, spec, formatVersion, properties, SortOrder.unsorted(), null, ops);
  }

  private static TestTable createTable(
      File temp,
      File metaTemp,
      String name,
      Schema schema,
      PartitionSpec spec,
      int formatVersion,
      Map<String, String> properties,
      SortOrder sortOrder,
      MetricsReporter reporter,
      TestTableOperations ops) {
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table %s already exists at location: %s", name, temp);
    }

    TableMetadata metadata =
        newTableMetadata(
            schema, spec, sortOrder, temp.toURI().toString(), properties, formatVersion);

    if (metaTemp != null) {
      metadata =
          TableMetadata.buildFrom(metadata)
              .discardChanges()
              .withMetadataLocation(metaTemp.toString())
              .build();
    }

    ops.commit(null, metadata);

    if (reporter != null) {
      return new TestTable(ops, reporter);
    } else {
      return new TestTable(ops);
    }
  }

  public static Transaction beginCreate(File temp, String name, Schema schema, PartitionSpec spec) {
    return beginCreate(temp, name, schema, spec, SortOrder.unsorted());
  }

  public static Transaction beginCreate(
      File temp, String name, Schema schema, PartitionSpec spec, SortOrder sortOrder) {
    TableOperations ops = new TestTableOperations(name, temp);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table %s already exists at location: %s", name, temp);
    }

    TableMetadata metadata =
        newTableMetadata(schema, spec, sortOrder, temp.toURI().toString(), ImmutableMap.of(), 1);

    return Transactions.createTableTransaction(name, ops, metadata);
  }

  public static Transaction beginReplace(
      File temp, String name, Schema schema, PartitionSpec spec) {
    return beginReplace(
        temp,
        name,
        schema,
        spec,
        SortOrder.unsorted(),
        ImmutableMap.of(),
        new TestTableOperations(name, temp));
  }

  public static Transaction beginReplace(
      File temp,
      String name,
      Schema schema,
      PartitionSpec spec,
      SortOrder sortOrder,
      Map<String, String> properties) {
    return beginReplace(
        temp, name, schema, spec, sortOrder, properties, new TestTableOperations(name, temp));
  }

  public static Transaction beginReplace(
      File temp,
      String name,
      Schema schema,
      PartitionSpec spec,
      SortOrder sortOrder,
      Map<String, String> properties,
      TestTableOperations ops) {
    TableMetadata current = ops.current();
    TableMetadata metadata;
    if (current != null) {
      metadata = current.buildReplacement(schema, spec, sortOrder, current.location(), properties);
      return Transactions.replaceTableTransaction(name, ops, metadata);
    } else {
      metadata = newTableMetadata(schema, spec, sortOrder, temp.toURI().toString(), properties);
      return Transactions.createTableTransaction(name, ops, metadata);
    }
  }

  public static TestTable load(File temp, String name) {
    TestTableOperations ops = new TestTableOperations(name, temp);
    return new TestTable(ops);
  }

  public static TestTable tableWithCommitSucceedButStateUnknown(File temp, String name) {
    TestTableOperations ops = opsWithCommitSucceedButStateUnknown(temp, name);
    return new TestTable(ops);
  }

  public static TestTableOperations opsWithCommitSucceedButStateUnknown(File temp, String name) {
    return new TestTableOperations(name, temp) {
      @Override
      public void commit(TableMetadata base, TableMetadata updatedMetadata) {
        super.commit(base, updatedMetadata);
        throw new CommitStateUnknownException(new RuntimeException("datacenter on fire"));
      }
    };
  }

  public static class TestTable extends BaseTable {
    private final TestTableOperations ops;

    private TestTable(TestTableOperations ops) {
      super(ops, ops.tableName);
      this.ops = ops;
    }

    private TestTable(TestTableOperations ops, MetricsReporter reporter) {
      super(ops, ops.tableName, reporter);
      this.ops = ops;
    }

    TestTableOperations ops() {
      return ops;
    }
  }

  private static final Map<String, TableMetadata> METADATA = Maps.newHashMap();
  private static final Map<String, Integer> VERSIONS = Maps.newHashMap();

  public static void clearTables() {
    synchronized (METADATA) {
      METADATA.clear();
      VERSIONS.clear();
    }
  }

  static TableMetadata readMetadata(String tableName) {
    synchronized (METADATA) {
      return METADATA.get(tableName);
    }
  }

  static Integer metadataVersion(String tableName) {
    synchronized (METADATA) {
      return VERSIONS.get(tableName);
    }
  }

  public static class TestTableOperations implements TableOperations {

    private final String tableName;
    private final File metadata;
    private final FileIO fileIO;
    protected TableMetadata current = null;
    private long lastSnapshotId = 0;
    private int failCommits = 0;

    public TestTableOperations(String tableName, File location) {
      this(tableName, location, new LocalFileIO());
    }

    public TestTableOperations(String tableName, File location, FileIO fileIO) {
      this.tableName = tableName;
      this.metadata = new File(location, "metadata");
      this.fileIO = fileIO;
      metadata.mkdirs();
      refresh();
      if (current != null) {
        for (Snapshot snap : current.snapshots()) {
          this.lastSnapshotId = Math.max(lastSnapshotId, snap.snapshotId());
        }
      } else {
        this.lastSnapshotId = 0;
      }
    }

    void failCommits(int numFailures) {
      this.failCommits = numFailures;
    }

    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      synchronized (METADATA) {
        this.current = METADATA.get(tableName);
      }
      return current;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata updatedMetadata) {
      if (base != current) {
        throw new CommitFailedException("Cannot commit changes based on stale metadata");
      }
      synchronized (METADATA) {
        refresh();
        if (base == current) {
          if (failCommits > 0) {
            this.failCommits -= 1;
            throw new CommitFailedException("Injected failure");
          }
          Integer version = VERSIONS.get(tableName);
          // remove changes from the committed metadata
          this.current =
              TableMetadata.buildFrom(updatedMetadata)
                  .discardChanges()
                  .withMetadataLocation((current != null) ? current.metadataFileLocation() : null)
                  .build();
          VERSIONS.put(tableName, version == null ? 0 : version + 1);
          METADATA.put(tableName, current);
        } else {
          throw new CommitFailedException(
              "Commit failed: table was updated at %d", current.lastUpdatedMillis());
        }
      }
    }

    @Override
    public FileIO io() {
      return fileIO;
    }

    @Override
    public LocationProvider locationProvider() {
      Preconditions.checkNotNull(
          current, "Current metadata should not be null when locationProvider is called");
      return LocationProviders.locationsFor(current.location(), current.properties());
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return new File(metadata, fileName).getAbsolutePath();
    }

    @Override
    public long newSnapshotId() {
      TableMetadata currentMetadata = current();
      if (currentMetadata != null
          && currentMetadata.propertyAsBoolean("random-snapshot-ids", false)) {
        return SnapshotIdGeneratorUtil.generateSnapshotID();
      } else {
        long nextSnapshotId = lastSnapshotId + 1;
        this.lastSnapshotId = nextSnapshotId;
        return nextSnapshotId;
      }
    }
  }

  public static class LocalFileIO implements FileIO {

    @Override
    public InputFile newInputFile(String path) {
      return Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      String localPath = path.startsWith("file:") ? path.replaceFirst("file:", "") : path;
      if (!new File(localPath).delete()) {
        throw new RuntimeIOException("Failed to delete file: " + path);
      }
    }
  }

  static class TestBulkLocalFileIO extends TestTables.LocalFileIO
      implements SupportsBulkOperations {

    @Override
    public void deleteFile(String path) {
      throw new RuntimeException("Expected to call the bulk delete interface.");
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
      throw new RuntimeException("Expected to mock this function");
    }
  }

  /** A {@link FileIO} that enforces a limit on concurrent open input streams. */
  public static class TrackingFileIO implements FileIO {

    private final FileIO delegate;
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicInteger peakCount = new AtomicInteger(0);

    TrackingFileIO(FileIO delegate) {
      this.delegate = delegate;
    }

    @Override
    public InputFile newInputFile(String path) {
      return new TrackingInputFile(delegate.newInputFile(path), count, peakCount);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }

    public int getPeakCount() {
      return peakCount.get();
    }
  }

  private static class TrackingInputFile implements InputFile {

    private final InputFile delegate;
    private final AtomicInteger count;
    private final AtomicInteger peakCount;

    TrackingInputFile(InputFile delegate, AtomicInteger count, AtomicInteger peakCount) {
      this.delegate = delegate;
      this.count = count;
      this.peakCount = peakCount;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      peakCount.accumulateAndGet(count.incrementAndGet(), Math::max);
      return new TrackingSeekableInputStream(delegate.newStream(), count);
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

  private static class TrackingSeekableInputStream extends SeekableInputStream {

    private final SeekableInputStream delegate;
    private final AtomicInteger count;

    TrackingSeekableInputStream(SeekableInputStream delegate, AtomicInteger count) {
      this.delegate = delegate;
      this.count = count;
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
    public int read(byte[] b, int off, int len) throws IOException {
      return delegate.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
      count.decrementAndGet();
    }
  }
}
