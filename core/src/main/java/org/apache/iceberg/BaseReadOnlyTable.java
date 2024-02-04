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

abstract class BaseReadOnlyTable implements Table {

  private final String descriptor;

  BaseReadOnlyTable(String descriptor) {
    this.descriptor = descriptor;
  }

  @Override
  public UpdateSchema updateSchema() {
    throw new UnsupportedOperationException(
        "Cannot update the schema of a " + descriptor + " table");
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    throw new UnsupportedOperationException(
        "Cannot update the partition spec of a " + descriptor + " table");
  }

  @Override
  public UpdateProperties updateProperties() {
    throw new UnsupportedOperationException(
        "Cannot update the properties of a " + descriptor + " table");
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    throw new UnsupportedOperationException(
        "Cannot update the sort order of a " + descriptor + " table");
  }

  @Override
  public UpdateLocation updateLocation() {
    throw new UnsupportedOperationException(
        "Cannot update the location of a " + descriptor + " table");
  }

  @Override
  public AppendFiles newAppend() {
    throw new UnsupportedOperationException("Cannot append to a " + descriptor + " table");
  }

  @Override
  public RewriteFiles newRewrite() {
    throw new UnsupportedOperationException("Cannot rewrite in a " + descriptor + " table");
  }

  @Override
  public RewriteManifests rewriteManifests() {
    throw new UnsupportedOperationException(
        "Cannot rewrite manifests in a " + descriptor + " table");
  }

  @Override
  public OverwriteFiles newOverwrite() {
    throw new UnsupportedOperationException("Cannot overwrite in a " + descriptor + " table");
  }

  @Override
  public RowDelta newRowDelta() {
    throw new UnsupportedOperationException(
        "Cannot remove or replace rows in a " + descriptor + " table");
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    throw new UnsupportedOperationException(
        "Cannot replace partitions in a " + descriptor + " table");
  }

  @Override
  public DeleteFiles newDelete() {
    throw new UnsupportedOperationException("Cannot delete from a " + descriptor + " table");
  }

  @Override
  public UpdateStatistics updateStatistics() {
    throw new UnsupportedOperationException(
        "Cannot update statistics of a " + descriptor + " table");
  }

  @Override
  public UpdatePartitionStatistics updatePartitionStatistics() {
    throw new UnsupportedOperationException(
        "Cannot update partition statistics of a " + descriptor + " table");
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    throw new UnsupportedOperationException(
        "Cannot expire snapshots from a " + descriptor + " table");
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    throw new UnsupportedOperationException(
        "Cannot manage snapshots in a " + descriptor + " table");
  }

  @Override
  public Transaction newTransaction() {
    throw new UnsupportedOperationException(
        "Cannot create transactions for a " + descriptor + " table");
  }
}
