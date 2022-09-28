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

class CommitCallbackTransaction implements Transaction {
  static Transaction addCallback(Transaction txn, Runnable callback) {
    return new CommitCallbackTransaction(txn, callback);
  }

  private final Transaction wrapped;
  private final Runnable callback;

  private CommitCallbackTransaction(Transaction wrapped, Runnable callback) {
    this.wrapped = wrapped;
    this.callback = callback;
  }

  @Override
  public Table table() {
    return wrapped.table();
  }

  @Override
  public UpdateSchema updateSchema() {
    return wrapped.updateSchema();
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    return wrapped.updateSpec();
  }

  @Override
  public UpdateProperties updateProperties() {
    return wrapped.updateProperties();
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    return wrapped.replaceSortOrder();
  }

  @Override
  public UpdateLocation updateLocation() {
    return wrapped.updateLocation();
  }

  @Override
  public AppendFiles newFastAppend() {
    return wrapped.newFastAppend();
  }

  @Override
  public AppendFiles newAppend() {
    return wrapped.newAppend();
  }

  @Override
  public RewriteFiles newRewrite() {
    return wrapped.newRewrite();
  }

  @Override
  public RewriteManifests rewriteManifests() {
    return wrapped.rewriteManifests();
  }

  @Override
  public OverwriteFiles newOverwrite() {
    return wrapped.newOverwrite();
  }

  @Override
  public RowDelta newRowDelta() {
    return wrapped.newRowDelta();
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    return wrapped.newReplacePartitions();
  }

  @Override
  public DeleteFiles newDelete() {
    return wrapped.newDelete();
  }

  @Override
  public UpdateStatistics updateStatistics() {
    return wrapped.updateStatistics();
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    return wrapped.expireSnapshots();
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    return wrapped.manageSnapshots();
  }

  @Override
  public void commitTransaction() {
    wrapped.commitTransaction();
    callback.run();
  }
}
