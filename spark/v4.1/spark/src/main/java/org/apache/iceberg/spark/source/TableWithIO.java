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
package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatisticsScan;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdatePartitionStatistics;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * This is a wrapper around {@link Table} and {@link FileIO} where the {@link FileIO} instance might
 * have more specific storage credentials and should be used for a table scan
 */
public class TableWithIO implements Table, HasTableOperations {
  private final Table table;
  private final Supplier<FileIO> fileIOForScan;

  public TableWithIO(Table table, Supplier<FileIO> fileIOForScan) {
    Preconditions.checkArgument(null != table, "Invalid table: null");
    Preconditions.checkArgument(null != fileIOForScan, "Invalid FileIO supplier: null");
    this.table = table;
    this.fileIOForScan = fileIOForScan;
  }

  @Override
  public FileIO io() {
    return fileIOForScan.get();
  }

  @Override
  public void refresh() {
    table.refresh();
  }

  @Override
  public String name() {
    return table.name();
  }

  @Override
  public TableScan newScan() {
    return table.newScan();
  }

  @Override
  public Schema schema() {
    return table.schema();
  }

  @Override
  public Map<Integer, Schema> schemas() {
    return table.schemas();
  }

  @Override
  public PartitionSpec spec() {
    return table.spec();
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return table.specs();
  }

  @Override
  public SortOrder sortOrder() {
    return table.sortOrder();
  }

  @Override
  public Map<Integer, SortOrder> sortOrders() {
    return table.sortOrders();
  }

  @Override
  public Map<String, String> properties() {
    return table.properties();
  }

  @Override
  public String location() {
    return table.location();
  }

  @Override
  public Snapshot currentSnapshot() {
    return table.currentSnapshot();
  }

  @Override
  public Snapshot snapshot(long snapshotId) {
    return table.snapshot(snapshotId);
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    return table.snapshots();
  }

  @Override
  public List<HistoryEntry> history() {
    return table.history();
  }

  @Override
  public UpdateSchema updateSchema() {
    return table.updateSchema();
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    return table.updateSpec();
  }

  @Override
  public UpdateProperties updateProperties() {
    return table.updateProperties();
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    return table.replaceSortOrder();
  }

  @Override
  public UpdateLocation updateLocation() {
    return table.updateLocation();
  }

  @Override
  public AppendFiles newAppend() {
    return table.newAppend();
  }

  @Override
  public RewriteFiles newRewrite() {
    return table.newRewrite();
  }

  @Override
  public RewriteManifests rewriteManifests() {
    return table.rewriteManifests();
  }

  @Override
  public OverwriteFiles newOverwrite() {
    return table.newOverwrite();
  }

  @Override
  public RowDelta newRowDelta() {
    return table.newRowDelta();
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    return table.newReplacePartitions();
  }

  @Override
  public DeleteFiles newDelete() {
    return table.newDelete();
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    return table.expireSnapshots();
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    return table.manageSnapshots();
  }

  @Override
  public Transaction newTransaction() {
    return table.newTransaction();
  }

  @Override
  public EncryptionManager encryption() {
    return table.encryption();
  }

  @Override
  public LocationProvider locationProvider() {
    return table.locationProvider();
  }

  @Override
  public List<StatisticsFile> statisticsFiles() {
    return table.statisticsFiles();
  }

  @Override
  public Map<String, SnapshotRef> refs() {
    return table.refs();
  }

  @Override
  public IncrementalAppendScan newIncrementalAppendScan() {
    return table.newIncrementalAppendScan();
  }

  @Override
  public IncrementalChangelogScan newIncrementalChangelogScan() {
    return table.newIncrementalChangelogScan();
  }

  @Override
  public PartitionStatisticsScan newPartitionStatisticsScan() {
    return table.newPartitionStatisticsScan();
  }

  @Override
  public AppendFiles newFastAppend() {
    return table.newFastAppend();
  }

  @Override
  public UpdateStatistics updateStatistics() {
    return table.updateStatistics();
  }

  @Override
  public UpdatePartitionStatistics updatePartitionStatistics() {
    return table.updatePartitionStatistics();
  }

  @Override
  public List<PartitionStatisticsFile> partitionStatisticsFiles() {
    return table.partitionStatisticsFiles();
  }

  @Override
  public UUID uuid() {
    return table.uuid();
  }

  @Override
  public TableOperations operations() {
    return table instanceof HasTableOperations ? ((HasTableOperations) table).operations() : null;
  }
}
