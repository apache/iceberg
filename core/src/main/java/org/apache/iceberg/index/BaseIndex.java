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
package org.apache.iceberg.index;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.UpdateLocation;

/**
 * Base implementation of the {@link Index} interface.
 *
 * <p>This class provides a concrete implementation backed by {@link IndexOperations} which manages
 * the index metadata.
 */
public class BaseIndex implements Index, Serializable {

  private final IndexOperations ops;
  private final String name;

  public BaseIndex(IndexOperations ops, String name) {
    this.ops = ops;
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  public IndexOperations operations() {
    return ops;
  }

  @Override
  public UUID uuid() {
    return UUID.fromString(operations().current().uuid());
  }

  @Override
  public int formatVersion() {
    return operations().current().formatVersion();
  }

  @Override
  public IndexType type() {
    return operations().current().type();
  }

  @Override
  public List<Integer> indexColumnIds() {
    return operations().current().indexColumnIds();
  }

  @Override
  public List<Integer> optimizedColumnIds() {
    return operations().current().optimizedColumnIds();
  }

  @Override
  public String location() {
    return operations().current().location();
  }

  @Override
  public int currentVersionId() {
    return operations().current().currentVersionId();
  }

  @Override
  public IndexVersion currentVersion() {
    return operations().current().currentVersion();
  }

  @Override
  public Iterable<IndexVersion> versions() {
    return operations().current().versions();
  }

  @Override
  public IndexVersion version(int versionId) {
    return operations().current().version(versionId);
  }

  @Override
  public List<IndexHistoryEntry> history() {
    return operations().current().history();
  }

  @Override
  public List<IndexSnapshot> snapshots() {
    return operations().current().snapshots();
  }

  @Override
  public IndexSnapshot snapshot(long indexSnapshotId) {
    return operations().current().snapshot(indexSnapshotId);
  }

  @Override
  public IndexSnapshot snapshotForTableSnapshot(long tableSnapshotId) {
    return operations().current().snapshotForTableSnapshot(tableSnapshotId);
  }

  @Override
  public AddIndexVersion addVersion() {
    return new IndexVersionAdd(ops);
  }

  @Override
  public UpdateLocation updateLocation() {
    return new SetIndexLocation(ops);
  }

  @Override
  public AddIndexSnapshot addIndexSnapshot() {
    return new IndexSnapshotAdd(ops);
  }

  @Override
  public RemoveIndexSnapshots removeIndexSnapshots() {
    return new IndexSnapshotsRemove(ops);
  }

  @Override
  public String toString() {
    return name();
  }
}
