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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

abstract class BaseMetadataTable implements Table {
  private PartitionSpec spec = PartitionSpec.unpartitioned();

  abstract Table table();
  abstract String metadataTableName();

  @Override
  public FileIO io() {
    return table().io();
  }

  @Override
  public String location() {
    return table().location();
  }

  @Override
  public EncryptionManager encryption() {
    return table().encryption();
  }

  @Override
  public LocationProvider locationProvider() {
    return table().locationProvider();
  }

  @Override
  public void refresh() {
    table().refresh();
  }

  @Override
  public PartitionSpec spec() {
    return spec;
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return ImmutableMap.of(spec.specId(), spec);
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of();
  }

  @Override
  public Snapshot currentSnapshot() {
    return table().currentSnapshot();
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    return table().snapshots();
  }

  @Override
  public Snapshot snapshot(long snapshotId) {
    return table().snapshot(snapshotId);
  }

  @Override
  public List<HistoryEntry> history() {
    return table().history();
  }

  @Override
  public UpdateSchema updateSchema() {
    throw new UnsupportedOperationException("Cannot update the schema of a metadata table");
  }

  @Override
  public UpdateProperties updateProperties() {
    throw new UnsupportedOperationException("Cannot update the properties of a metadata table");
  }

  @Override
  public UpdateLocation updateLocation() {
    throw new UnsupportedOperationException("Cannot update the location of a metadata table");
  }

  @Override
  public AppendFiles newAppend() {
    throw new UnsupportedOperationException("Cannot append to a metadata table");
  }

  @Override
  public RewriteFiles newRewrite() {
    throw new UnsupportedOperationException("Cannot rewrite in a metadata table");
  }

  @Override
  public RewriteManifests rewriteManifests() {
    throw new UnsupportedOperationException("Cannot rewrite manifests in a metadata table");
  }

  @Override
  public OverwriteFiles newOverwrite() {
    throw new UnsupportedOperationException("Cannot overwrite in a metadata table");
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    throw new UnsupportedOperationException("Cannot replace partitions in a metadata table");
  }

  @Override
  public DeleteFiles newDelete() {
    throw new UnsupportedOperationException("Cannot delete from a metadata table");
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    throw new UnsupportedOperationException("Cannot expire snapshots from a metadata table");
  }

  @Override
  public Rollback rollback() {
    throw new UnsupportedOperationException("Cannot roll back a metadata table");
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    throw new UnsupportedOperationException("Cannot manage snapshots in a metadata table");
  }

  @Override
  public Transaction newTransaction() {
    throw new UnsupportedOperationException("Cannot create transactions for a metadata table");
  }

  @Override
  public String toString() {
    return table().toString() + "." + metadataTableName();
  }
}
