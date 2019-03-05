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

package com.netflix.iceberg;

import com.netflix.iceberg.encryption.EncryptionManager;
import com.netflix.iceberg.io.FileIO;
import com.netflix.iceberg.io.LocationProvider;
import java.util.Map;

/**
 * Base {@link Table} implementation.
 * <p>
 * This can be extended by providing a {@link TableOperations} to the constructor.
 */
public class BaseTable implements Table, HasTableOperations {
  private final TableOperations ops;
  private final String name;

  public BaseTable(TableOperations ops, String name) {
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
    return new BaseTableScan(ops, this);
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
  public Map<String, String> properties() {
    return ops.current().properties();
  }

  @Override
  public String location() {
    return ops.current().location();
  }

  @Override
  public Snapshot currentSnapshot() {
    return ops.current().currentSnapshot();
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    return ops.current().snapshots();
  }

  @Override
  public UpdateSchema updateSchema() {
    return new SchemaUpdate(ops);
  }

  @Override
  public UpdateProperties updateProperties() {
    return new PropertiesUpdate(ops);
  }

  @Override
  public UpdateLocation updateLocation() {
    return new SetLocation(ops);
  }

  @Override
  public AppendFiles newAppend() {
    return new MergeAppend(ops);
  }

  @Override
  public AppendFiles newFastAppend() {
    return new FastAppend(ops);
  }

  @Override
  public RewriteFiles newRewrite() {
    return new ReplaceFiles(ops);
  }

  @Override
  public OverwriteFiles newOverwrite() {
    return new OverwriteData(ops);
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    return new ReplacePartitionsOperation(ops);
  }

  @Override
  public DeleteFiles newDelete() {
    return new StreamingDelete(ops);
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    return new RemoveSnapshots(ops);
  }

  @Override
  public Rollback rollback() {
    return new RollbackToSnapshot(ops);
  }

  @Override
  public Transaction newTransaction() {
    return BaseTransaction.newTransaction(ops);
  }

  @Override
  public FileIO io() {
    return operations().io();
  }

  @Override
  public EncryptionManager encryption() {
    return operations().encryption();
  }

  @Override
  public LocationProvider locationProvider() {
    return operations().locationProvider();
  }

  @Override
  public String toString() {
    return name;
  }
}
