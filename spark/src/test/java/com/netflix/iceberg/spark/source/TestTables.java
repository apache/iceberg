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

package com.netflix.iceberg.spark.source;

import com.google.common.collect.Maps;
import com.netflix.iceberg.BaseTable;
import com.netflix.iceberg.LocationProviders;
import com.netflix.iceberg.io.FileIO;
import com.netflix.iceberg.Files;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Snapshot;
import com.netflix.iceberg.TableMetadata;
import com.netflix.iceberg.TableOperations;
import com.netflix.iceberg.exceptions.AlreadyExistsException;
import com.netflix.iceberg.exceptions.CommitFailedException;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.LocationProvider;
import com.netflix.iceberg.io.OutputFile;
import parquet.Preconditions;
import java.io.File;
import java.util.Map;

// TODO: Use the copy of this from core.
class TestTables {
  private TestTables() {
  }

  static TestTable create(File temp, String name, Schema schema, PartitionSpec spec) {
    TestTableOperations ops = new TestTableOperations(name);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table %s already exists at location: %s", name, temp);
    }
    ops.commit(null, TableMetadata.newTableMetadata(ops, schema, spec, temp.toString()));
    return new TestTable(ops, name);
  }

  static TestTable load(String name) {
    TestTableOperations ops = new TestTableOperations(name);
    if (ops.current() == null) {
      return null;
    }
    return new TestTable(ops, name);
  }

  static boolean drop(String name) {
    synchronized (METADATA) {
      return METADATA.remove(name) != null;
    }
  }

  static class TestTable extends BaseTable {
    private final TestTableOperations ops;

    private TestTable(TestTableOperations ops, String name) {
      super(ops, name);
      this.ops = ops;
    }

    @Override
    public TestTableOperations operations() {
      return ops;
    }
  }

  private static final Map<String, TableMetadata> METADATA = Maps.newHashMap();

  static void clearTables() {
    synchronized (METADATA) {
      METADATA.clear();
    }
  }

  static TableMetadata readMetadata(String tableName) {
    synchronized (METADATA) {
      return METADATA.get(tableName);
    }
  }

  static void replaceMetadata(String tableName, TableMetadata metadata) {
    synchronized (METADATA) {
      METADATA.put(tableName, metadata);
    }
  }

  static class TestTableOperations implements TableOperations {

    private final String tableName;
    private TableMetadata current = null;
    private long lastSnapshotId = 0;
    private int failCommits = 0;

    TestTableOperations(String tableName) {
      this.tableName = tableName;
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
    public void commit(TableMetadata base, TableMetadata metadata) {
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
          METADATA.put(tableName, metadata);
          this.current = metadata;
        } else {
          throw new CommitFailedException(
              "Commit failed: table was updated at %d", base.lastUpdatedMillis());
        }
      }
    }

    @Override
    public FileIO io() {
      return new LocalFileIO();
    }

    @Override
    public LocationProvider locationProvider() {
      Preconditions.checkNotNull(current,
          "Current metadata should not be null when locatinProvider is called");
      return LocationProviders.locationsFor(current.location(), current.properties());
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return new File(new File(current.location(), "metadata"), fileName).getAbsolutePath();
    }

    @Override
    public long newSnapshotId() {
      long nextSnapshotId = lastSnapshotId + 1;
      this.lastSnapshotId = nextSnapshotId;
      return nextSnapshotId;
    }
  }
  
  static class LocalFileIO implements FileIO {

    @Override
    public InputFile newInputFile(String path) {
      return Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return Files.localOutput(new File(path));
    }

    @Override
    public void deleteFile(String path) {
      if (!new File(path).delete()) {
        throw new RuntimeIOException("Failed to delete file: " + path);
      }
    }
  }
}
