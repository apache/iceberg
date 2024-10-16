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
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/** Represents a table. */
public interface Table {

  /**
   * Return the full name for this table.
   *
   * @return this table's name
   */
  default String name() {
    return toString();
  }

  /** Refresh the current table metadata. */
  void refresh();

  /**
   * Create a new {@link TableScan scan} for this table.
   *
   * <p>Once a table scan is created, it can be refined to project columns and filter data.
   *
   * @return a table scan for this table
   */
  TableScan newScan();

  /**
   * Create a new {@link BatchScan batch scan} for this table.
   *
   * <p>Once a batch scan is created, it can be refined to project columns and filter data.
   *
   * @return a batch scan for this table
   */
  default BatchScan newBatchScan() {
    return new BatchScanAdapter(newScan());
  }

  /**
   * Create a new {@link IncrementalAppendScan scan} for this table.
   *
   * <p>Once a scan is created, it can be refined to project columns and filter data.
   *
   * @return an incremental scan for appends only snapshots
   */
  default IncrementalAppendScan newIncrementalAppendScan() {
    throw new UnsupportedOperationException("Incremental append scan is not supported");
  }

  /**
   * Create a new {@link IncrementalChangelogScan} for this table.
   *
   * <p>Once a scan is created, it can be refined to project columns and filter data.
   *
   * @return an incremental changelog scan
   */
  default IncrementalChangelogScan newIncrementalChangelogScan() {
    throw new UnsupportedOperationException("Incremental changelog scan is not supported");
  }

  /**
   * Return the {@link Schema schema} for this table.
   *
   * @return this table's schema
   */
  Schema schema();

  /**
   * Return a map of {@link Schema schema} for this table.
   *
   * @return this table's schema map
   */
  Map<Integer, Schema> schemas();

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
   * Return the {@link SortOrder sort order} for this table.
   *
   * @return this table's sort order
   */
  SortOrder sortOrder();

  /**
   * Return a map of sort order IDs to {@link SortOrder sort orders} for this table.
   *
   * @return this table's sort orders map
   */
  Map<Integer, SortOrder> sortOrders();

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
   * Create a new {@link UpdatePartitionSpec} to alter the partition spec of this table and commit
   * the change.
   *
   * @return a new {@link UpdatePartitionSpec}
   */
  UpdatePartitionSpec updateSpec();

  /**
   * Create a new {@link UpdateProperties} to update table properties and commit the changes.
   *
   * @return a new {@link UpdateProperties}
   */
  UpdateProperties updateProperties();

  /**
   * Create a new {@link ReplaceSortOrder} to set the table sort order and commit the change.
   *
   * @return a new {@link ReplaceSortOrder}
   */
  ReplaceSortOrder replaceSortOrder();

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
   *
   * <p>Using this method signals to the underlying implementation that the append should not
   * perform extra work in order to commit quickly. Fast appends are not recommended for normal
   * writes because the fast commit may cause split planning to slow down over time.
   *
   * <p>Implementations may not support fast appends, in which case this will return the same
   * appender as {@link #newAppend()}.
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
   * Create a new {@link RewriteManifests rewrite manifests API} to replace manifests for this table
   * and commit.
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
   * Create a new {@link RowDelta row-level delta API} to remove or replace rows in existing data
   * files.
   *
   * @return a new {@link RowDelta}
   */
  RowDelta newRowDelta();

  /**
   * Not recommended: Create a new {@link ReplacePartitions replace partitions API} to dynamically
   * overwrite partitions in the table with new data.
   *
   * <p>This is provided to implement SQL compatible with Hive table operations but is not
   * recommended. Instead, use the {@link OverwriteFiles overwrite API} to explicitly overwrite
   * data.
   *
   * @return a new {@link ReplacePartitions}
   */
  ReplacePartitions newReplacePartitions();

  /**
   * Create a new {@link DeleteFiles delete API} to delete files in this table and commit.
   *
   * @return a new {@link DeleteFiles}
   */
  DeleteFiles newDelete();

  /**
   * Create a new {@link UpdateStatistics update table statistics API} to add or remove statistics
   * files in this table.
   *
   * @return a new {@link UpdateStatistics}
   */
  default UpdateStatistics updateStatistics() {
    throw new UnsupportedOperationException(
        "Updating statistics is not supported by " + getClass().getName());
  }

  /**
   * Create a new {@link UpdatePartitionStatistics update partition statistics API} to add or remove
   * partition statistics files in this table.
   *
   * @return a new {@link UpdatePartitionStatistics}
   */
  default UpdatePartitionStatistics updatePartitionStatistics() {
    throw new UnsupportedOperationException(
        "Updating partition statistics is not supported by " + getClass().getName());
  }

  /**
   * Create a new {@link ExpireSnapshots expire API} to expire snapshots in this table and commit.
   *
   * @return a new {@link ExpireSnapshots}
   */
  ExpireSnapshots expireSnapshots();

  /**
   * Create a new {@link ManageSnapshots manage snapshots API} to manage snapshots in this table and
   * commit.
   *
   * @return a new {@link ManageSnapshots}
   */
  ManageSnapshots manageSnapshots();

  /**
   * Create a new {@link Transaction transaction API} to commit multiple table operations at once.
   *
   * @return a new {@link Transaction}
   */
  Transaction newTransaction();

  /** Returns a {@link FileIO} to read and write table data and metadata files. */
  FileIO io();

  /**
   * Returns an {@link org.apache.iceberg.encryption.EncryptionManager} to encrypt and decrypt data
   * files.
   */
  EncryptionManager encryption();

  /** Returns a {@link LocationProvider} to provide locations for new data files. */
  LocationProvider locationProvider();

  /**
   * Returns the current statistics files for the table
   *
   * @return the current statistics files for the table
   */
  List<StatisticsFile> statisticsFiles();

  /** Returns the current partition statistics files for the table. */
  default List<PartitionStatisticsFile> partitionStatisticsFiles() {
    return ImmutableList.of();
  }

  /**
   * Returns the current refs for the table
   *
   * @return the current refs for the table
   */
  Map<String, SnapshotRef> refs();

  /**
   * Returns the UUID of the table
   *
   * @return the UUID of the table
   */
  default UUID uuid() {
    throw new UnsupportedOperationException(this.getClass().getName() + " doesn't implement uuid");
  }

  /**
   * Returns the snapshot referenced by the given name or null if no such reference exists.
   *
   * @return the snapshot which is referenced by the given name or null if no such reference exists.
   */
  default Snapshot snapshot(String name) {
    SnapshotRef ref = refs().get(name);
    if (ref != null) {
      return snapshot(ref.snapshotId());
    }

    return null;
  }

  /**
   * Returns the statistics file for the given snapshot id, if available.
   *
   * @return the {@link StatisticsFile} for the given snapshot id, if available.
   */
  default Optional<StatisticsFile> statistics(long snapshotId) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement statistics");
  }
}
