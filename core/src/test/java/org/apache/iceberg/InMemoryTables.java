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

import java.util.Map;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class InMemoryTables {

  private InMemoryTables() {}

  private static final String WAREHOUSE_LOCATION = "memory:/warehouse";

  public static Table upgrade(String name, FileIO io, int newFormatVersion) {
    Table table = load(name, io);
    org.apache.iceberg.TableOperations ops = table.ops();
    TableMetadata base = ops.current();
    ops.commit(base, ops.current().upgradeToFormatVersion(newFormatVersion));
    return table;
  }

  public static Table create(
      String name, FileIO io, Schema schema, PartitionSpec spec, int formatVersion) {
    return createTable(name, io, schema, spec, Map.of(), formatVersion);
  }

  public static Table create(
      String name,
      FileIO io,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties,
      int formatVersion) {
    return createTable(name, io, schema, spec, properties, formatVersion);
  }

  private static Table createTable(
      String name,
      FileIO io,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties,
      int formatVersion) {
    TableOperations ops = new TableOperations(name, io);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists: %s", name);
    }

    String location = WAREHOUSE_LOCATION + "/" + name;
    TableMetadata metadata =
        newTableMetadata(schema, spec, SortOrder.unsorted(), location, properties, formatVersion);

    ops.commit(null, metadata);

    return new Table(ops);
  }

  public static Table load(String name, FileIO io) {
    TableOperations ops = new TableOperations(name, io);
    return new Table(ops);
  }

  public static class Table extends BaseTable {
    private final TableOperations ops;

    private Table(TableOperations ops) {
      super(ops, ops.tableName);
      this.ops = ops;
    }

    TableOperations ops() {
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

  public static class TableOperations implements org.apache.iceberg.TableOperations {
    private final String tableName;
    private final FileIO fileIO;
    protected TableMetadata current = null;
    private long lastSnapshotId = 0;
    private int failCommits = 0;

    public TableOperations(String tableName, FileIO fileIO) {
      this.tableName = tableName;
      this.fileIO = fileIO;
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
      return current.location() + "/metadata/" + fileName;
    }
  }
}
