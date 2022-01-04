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

import java.io.IOException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class BaseTruncate extends MergingSnapshotProducer<Truncate> implements Truncate {
  private final TableOperations ops;

  private boolean fastTruncate = false;

  BaseTruncate(String tableName, TableOperations ops) {
    super(tableName, ops);
    this.ops = ops;
  }

  @Override
  public Truncate truncate(boolean fastMode) {
    this.fastTruncate = fastMode;
    if (!fastMode) {
      deleteByRowFilter(Expressions.alwaysTrue());
    }
    return this;
  }

  @Override
  protected Truncate self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.DELETE;
  }

  @Override
  public Snapshot apply() {
    return fastTruncate ? applyEmpty() : super.apply();
  }

  private Snapshot applyEmpty() {
    TableMetadata base = refresh();
    long sequenceNumber = base.nextSequenceNumber();
    Long parentSnapshotId = base.currentSnapshot() != null ? base.currentSnapshot().snapshotId() : null;

    OutputFile manifestList = manifestListPath();
    try (ManifestListWriter writer = ManifestLists.write(
        ops.current().formatVersion(), manifestList, snapshotId(), parentSnapshotId, sequenceNumber)) {
      // do nothing
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write manifest list file");
    }

    return new BaseSnapshot(ops.io(),
        sequenceNumber, snapshotId(), parentSnapshotId, System.currentTimeMillis(), operation(), ImmutableMap.of(),
        base.currentSchemaId(), manifestList.location());
  }
}
