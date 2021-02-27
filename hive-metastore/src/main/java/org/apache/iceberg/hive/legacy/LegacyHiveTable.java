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

package org.apache.iceberg.hive.legacy;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Rollback;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;


/**
 * A {@link Table} which uses Hive table/partition metadata to perform scans using {@link LegacyHiveTableScan}.
 * This table does not provide any time travel, snapshot isolation, incremental computation benefits.
 * It also does not allow any WRITE operations to either the data or metadata.
 */
public class LegacyHiveTable implements Table, HasTableOperations {
  private final TableOperations ops;
  private final String name;

  protected LegacyHiveTable(TableOperations ops, String name) {
    this.ops = ops;
    this.name = name;
  }

  @Override
  public TableOperations operations() {
    return ops;
  }

  @Override
  public void refresh() {
    ops.refresh();
  }

  @Override
  public TableScan newScan() {
    return new LegacyHiveTableScan(ops, this);
  }

  @Override
  public Schema schema() {
    return ops.current().schema();
  }

  @Override
  public PartitionSpec spec() {
    return ops.current().spec();
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    throw new UnsupportedOperationException(
        "Multiple partition specs not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public SortOrder sortOrder() {
    throw new UnsupportedOperationException("Sort order not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public Map<Integer, SortOrder> sortOrders() {
    throw new UnsupportedOperationException("Sort orders not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public Map<String, String> properties() {
    return ops.current().properties();
  }

  @Override
  public String location() {
    return ops.current().location();
  }

  @Override
  public Snapshot currentSnapshot() {
    throw new UnsupportedOperationException("Snapshots not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public Snapshot snapshot(long snapshotId) {
    throw new UnsupportedOperationException("Snapshots not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    throw new UnsupportedOperationException("Snapshots not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public List<HistoryEntry> history() {
    throw new UnsupportedOperationException("History not available for Hive tables without Iceberg metadata");
  }

  @Override
  public UpdateSchema updateSchema() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public UpdateProperties updateProperties() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public UpdateLocation updateLocation() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public AppendFiles newAppend() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public RewriteFiles newRewrite() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public RewriteManifests rewriteManifests() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public OverwriteFiles newOverwrite() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public RowDelta newRowDelta() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public DeleteFiles newDelete() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public Rollback rollback() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public Transaction newTransaction() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public FileIO io() {
    return ops.io();
  }

  @Override
  public EncryptionManager encryption() {
    return ops.encryption();
  }

  @Override
  public LocationProvider locationProvider() {
    return ops.locationProvider();
  }

  @Override
  public String toString() {
    return name;
  }
}
