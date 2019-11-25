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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * Represents a table.
 */
public interface Table {

  /**
   * Refresh the current table metadata.
   */
  void refresh();

  /**
   * Create a new {@link TableScan scan} for this table.
   * <p>
   * Once a table scan is created, it can be refined to project columns and filter data.
   *
   * @return a table scan for this table
   */
  TableScan newScan();

  /**
   * Return the {@link Schema schema} for this table.
   *
   * @return this table's schema
   */
  Schema schema();

  /**
   * Return the {@link PartitionSpec partition spec} for this table.
   *
   * @return this table's partition spec
   */
  PartitionSpec spec();

  /**
   * Return a map of {@link PartitionSpec partition specs} for this table.
   *
   * @return this table's partition specs map
   */
  Map<Integer, PartitionSpec> specs();

  /**
   * Return a map of string properties for this table.
   *
   * @return this table's properties map
   */
  Map<String, String> properties();

  /**
   * Return the table's base location.
   *
   * @return this table's location
   */
  String location();

  /**
   * Get the current {@link Snapshot snapshot} for this table, or null if there are no snapshots.
   *
   * @return the current table Snapshot.
   */
  Snapshot currentSnapshot();

  /**
   * Get the {@link Snapshot snapshot} of this table with the given id, or null if there is no
   * matching snapshot.
   *
   * @return the {@link Snapshot} with the given id.
   */
  Snapshot snapshot(long snapshotId);

  /**
   * Get the {@link Snapshot snapshots} of this table.
   *
   * @return an Iterable of snapshots of this table.
   */
  Iterable<Snapshot> snapshots();

  /**
   * Get the snapshot history of this table.
   *
   * @return a list of {@link HistoryEntry history entries}
   */
  List<HistoryEntry> history();

  /**
   * Create a new {@link UpdateSchema} to alter the columns of this table and commit the change.
   *
   * @return a new {@link UpdateSchema}
   */
  UpdateSchema updateSchema();

  /**
   * Create a new {@link UpdateProperties} to update table properties and commit the changes.
   *
   * @return a new {@link UpdateProperties}
   */
  UpdateProperties updateProperties();

  /**
   * Create a new {@link UpdateLocation} to update table location and commit the changes.
   *
   * @return a new {@link UpdateLocation}
   */
  UpdateLocation updateLocation();

  /**
   * Create a new {@link AppendFiles append API} to add files to this table and commit.
   *
   * @return a new {@link AppendFiles}
   */
  AppendFiles newAppend();

  /**
   * Create a new {@link AppendFiles append API} to add files to this table and commit.
   * <p>
   * Using this method signals to the underlying implementation that the append should not perform
   * extra work in order to commit quickly. Fast appends are not recommended for normal writes
   * because the fast commit may cause split planning to slow down over time.
   * <p>
   * Implementations may not support fast appends, in which case this will return the same appender
   * as {@link #newAppend()}.
   *
   * @return a new {@link AppendFiles}
   */
  default AppendFiles newFastAppend() {
    return newAppend();
  }

  /**
   * Create a new {@link RewriteFiles rewrite API} to replace files in this table and commit.
   *
   * @return a new {@link RewriteFiles}
   */
  RewriteFiles newRewrite();

  /**
   * Create a new {@link RewriteManifests rewrite manifests API} to replace manifests for this
   * table and commit.
   *
   * @return a new {@link RewriteManifests}
   */
  RewriteManifests rewriteManifests();

  /**
   * Create a new {@link OverwriteFiles overwrite API} to overwrite files by a filter expression.
   *
   * @return a new {@link OverwriteFiles}
   */
  OverwriteFiles newOverwrite();

  /**
   * Not recommended: Create a new {@link ReplacePartitions replace partitions API} to dynamically
   * overwrite partitions in the table with new data.
   * <p>
   * This is provided to implement SQL compatible with Hive table operations but is not recommended.
   * Instead, use the {@link OverwriteFiles overwrite API} to explicitly overwrite data.
   *
   * @return a new {@link ReplacePartitions}
   */
  ReplacePartitions newReplacePartitions();

  /**
   * Create a new {@link DeleteFiles delete API} to replace files in this table and commit.
   *
   * @return a new {@link DeleteFiles}
   */
  DeleteFiles newDelete();

  /**
   * Create a new {@link ExpireSnapshots expire API} to manage snapshots in this table and commit.
   *
   * @return a new {@link ExpireSnapshots}
   */
  ExpireSnapshots expireSnapshots();

  /**
   * Create a new {@link Rollback rollback API} to roll back to a previous snapshot and commit.
   *
   * @return a new {@link Rollback}
   */
  Rollback rollback();

  /**
   * Create a new {@link Transaction transaction API} to commit multiple table operations at once.
   *
   * @return a new {@link Transaction}
   */
  Transaction newTransaction();

  /**
   * @return a {@link FileIO} to read and write table data and metadata files
   */
  FileIO io();

  /**
   * @return an {@link org.apache.iceberg.encryption.EncryptionManager} to encrypt and decrypt
   * data files.
   */
  EncryptionManager encryption();

  /**
   * @return a {@link LocationProvider} to provide locations for new data files
   */
  LocationProvider locationProvider();
}
