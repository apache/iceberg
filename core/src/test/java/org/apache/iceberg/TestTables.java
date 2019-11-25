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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.Map;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;

import static org.apache.iceberg.TableMetadata.newTableMetadata;

public class TestTables {

  private TestTables() {}

  public static TestTable create(File temp, String name, Schema schema, PartitionSpec spec) {
    TestTableOperations ops = new TestTableOperations(name, temp);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table %s already exists at location: %s", name, temp);
    }
    ops.commit(null, TableMetadata.newTableMetadata(schema, spec, temp.toString()));
    return new TestTable(ops, name);
  }

  public static Transaction beginCreate(File temp, String name, Schema schema, PartitionSpec spec) {
    TableOperations ops = new TestTableOperations(name, temp);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table %s already exists at location: %s", name, temp);
    }

    TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, temp.toString());

    return Transactions.createTableTransaction(ops, metadata);
  }

  public static Transaction beginReplace(File temp, String name, Schema schema, PartitionSpec spec) {
    return beginReplace(temp, name, schema, spec, ImmutableMap.of());
  }

  public static Transaction beginReplace(File temp, String name, Schema schema, PartitionSpec spec,
                                         Map<String, String> properties) {
    TestTableOperations ops = new TestTableOperations(name, temp);
    TableMetadata current = ops.current();

    TableMetadata metadata;
    if (current != null) {
      metadata = current.buildReplacement(schema, spec, properties);
      return Transactions.replaceTableTransaction(ops, metadata);
    } else {
      metadata = newTableMetadata(schema, spec, temp.toString(), properties);
      return Transactions.createTableTransaction(ops, metadata);
    }
  }

  public static TestTable load(File temp, String name) {
    TestTableOperations ops = new TestTableOperations(name, temp);
    return new TestTable(ops, name);
  }

  public static class TestTable extends BaseTable {
    private final TestTableOperations ops;

    private TestTable(TestTableOperations ops, String name) {
      super(ops, name);
      this.ops = ops;
    }

    TestTableOperations ops() {
      return ops;
    }
  }

  private static final Map<String, TableMetadata> METADATA = Maps.newHashMap();
  private static final Map<String, Integer> VERSIONS = Maps.newHashMap();

  static void clearTables() {
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
    private TableMetadata current = null;
    private long lastSnapshotId = 0;
    private int failCommits = 0;

    public TestTableOperations(String tableName, File location) {
      this.tableName = tableName;
      this.metadata = new File(location, "metadata");
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
          VERSIONS.put(tableName, version == null ? 0 : version + 1);
          METADATA.put(tableName, updatedMetadata);
          this.current = updatedMetadata;
        } else {
          throw new CommitFailedException(
              "Commit failed: table was updated at %d", current.lastUpdatedMillis());
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
      return new File(metadata, fileName).getAbsolutePath();
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
      return Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      if (!new File(path).delete()) {
        throw new RuntimeIOException("Failed to delete file: " + path);
      }
    }
  }
}
