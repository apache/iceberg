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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.util.Tasks;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

class BaseTransaction implements Transaction {
  private enum TransactionType {
    CREATE_TABLE,
    REPLACE_TABLE,
    SIMPLE
  }

  private final TableOperations ops;
  private final TransactionTable transactionTable;
  private final TableOperations transactionOps;
  private final List<PendingUpdate> updates;
  private final Set<Long> intermediateSnapshotIds;
  private final Set<String> deletedFiles = Sets.newHashSet(); // keep track of files deleted in the most recent commit
  private final Consumer<String> enqueueDelete = deletedFiles::add;
  private TransactionType type;
  private TableMetadata base;
  private TableMetadata lastBase;
  private TableMetadata current;

  BaseTransaction(TableOperations ops, TableMetadata start) {
    this.ops = ops;
    this.transactionTable = new TransactionTable();
    this.transactionOps = new TransactionTableOperations();
    this.updates = Lists.newArrayList();
    this.intermediateSnapshotIds = Sets.newHashSet();
    this.base = ops.current();
    if (base == null && start != null) {
      this.type = TransactionType.CREATE_TABLE;
    } else if (base != null && start != base) {
      this.type = TransactionType.REPLACE_TABLE;
    } else {
      this.type = TransactionType.SIMPLE;
    }
    this.lastBase = null;
    this.current = start;
  }

  @Override
  public Table table() {
    return transactionTable;
  }

  private void checkLastOperationCommitted(String operation) {
    Preconditions.checkState(lastBase != current,
        "Cannot create new %s: last operation has not committed", operation);
    this.lastBase = current;
  }

  @Override
  public UpdateSchema updateSchema() {
    checkLastOperationCommitted("UpdateSchema");
    UpdateSchema schemaChange = new SchemaUpdate(transactionOps);
    updates.add(schemaChange);
    return schemaChange;
  }

  @Override
  public UpdateProperties updateProperties() {
    checkLastOperationCommitted("UpdateProperties");
    UpdateProperties props = new PropertiesUpdate(transactionOps);
    updates.add(props);
    return props;
  }

  @Override
  public UpdateLocation updateLocation() {
    checkLastOperationCommitted("UpdateLocation");
    UpdateLocation setLocation = new SetLocation(transactionOps);
    updates.add(setLocation);
    return setLocation;
  }

  @Override
  public AppendFiles newAppend() {
    checkLastOperationCommitted("AppendFiles");
    AppendFiles append = new MergeAppend(transactionOps);
    append.deleteWith(enqueueDelete);
    updates.add(append);
    return append;
  }

  @Override
  public AppendFiles newFastAppend() {
    checkLastOperationCommitted("AppendFiles");
    AppendFiles append = new FastAppend(transactionOps);
    updates.add(append);
    return append;
  }

  @Override
  public RewriteFiles newRewrite() {
    checkLastOperationCommitted("RewriteFiles");
    RewriteFiles rewrite = new ReplaceFiles(transactionOps);
    rewrite.deleteWith(enqueueDelete);
    updates.add(rewrite);
    return rewrite;
  }

  @Override
  public RewriteManifests rewriteManifests() {
    checkLastOperationCommitted("RewriteManifests");
    RewriteManifests rewrite = new ReplaceManifests(transactionOps);
    rewrite.deleteWith(enqueueDelete);
    updates.add(rewrite);
    return rewrite;
  }

  @Override
  public OverwriteFiles newOverwrite() {
    checkLastOperationCommitted("OverwriteFiles");
    OverwriteFiles overwrite = new OverwriteData(transactionOps);
    overwrite.deleteWith(enqueueDelete);
    updates.add(overwrite);
    return overwrite;
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    checkLastOperationCommitted("ReplacePartitions");
    ReplacePartitionsOperation replacePartitions = new ReplacePartitionsOperation(transactionOps);
    replacePartitions.deleteWith(enqueueDelete);
    updates.add(replacePartitions);
    return replacePartitions;
  }

  @Override
  public DeleteFiles newDelete() {
    checkLastOperationCommitted("DeleteFiles");
    DeleteFiles delete = new StreamingDelete(transactionOps);
    delete.deleteWith(enqueueDelete);
    updates.add(delete);
    return delete;
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    checkLastOperationCommitted("ExpireSnapshots");
    ExpireSnapshots expire = new RemoveSnapshots(transactionOps);
    expire.deleteWith(enqueueDelete);
    updates.add(expire);
    return expire;
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(lastBase != current,
        "Cannot commit transaction: last operation has not committed");

    switch (type) {
      case CREATE_TABLE:
        // fix up the snapshot log, which should not contain intermediate snapshots
        TableMetadata createMetadata = current.removeSnapshotLogEntries(intermediateSnapshotIds);

        // this operation creates the table. if the commit fails, this cannot retry because another
        // process has created the same table.
        ops.commit(null, createMetadata);
        break;

      case REPLACE_TABLE:
        // fix up the snapshot log, which should not contain intermediate snapshots
        TableMetadata replaceMetadata = current.removeSnapshotLogEntries(intermediateSnapshotIds);

        Tasks.foreach(ops)
            .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
            .exponentialBackoff(
                base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
                base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
                base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
                2.0 /* exponential */)
            .onlyRetryOn(CommitFailedException.class)
            .run(underlyingOps -> {
              // because this is a replace table, it will always completely replace the table
              // metadata. even if it was just updated.
              if (base != underlyingOps.refresh()) {
                this.base = underlyingOps.current(); // just refreshed
              }

              underlyingOps.commit(base, replaceMetadata);
            });
        break;

      case SIMPLE:
        // if there were no changes, don't try to commit
        if (base == current) {
          return;
        }

        Tasks.foreach(ops)
            .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
            .exponentialBackoff(
                base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
                base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
                base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
                2.0 /* exponential */)
            .onlyRetryOn(CommitFailedException.class)
            .run(underlyingOps -> {
              if (base != underlyingOps.refresh()) {
                this.base = underlyingOps.current(); // just refreshed
                this.current = base;
                this.deletedFiles.clear(); // clear deletes from the last set of operation commits
                for (PendingUpdate update : updates) {
                  // re-commit each update in the chain to apply it and update current
                  update.commit();
                }
              }

              // fix up the snapshot log, which should not contain intermediate snapshots
              underlyingOps.commit(base, current.removeSnapshotLogEntries(intermediateSnapshotIds));
            });

        // delete all of the files that were deleted in the most recent set of operation commits
        deletedFiles.forEach(ops.io()::deleteFile);
        break;
    }
  }

  private static Long currentId(TableMetadata meta) {
    if (meta != null) {
      if (meta.currentSnapshot() != null) {
        return meta.currentSnapshot().snapshotId();
      }
    }
    return null;
  }

  public class TransactionTableOperations implements TableOperations {
    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      return current;
    }

    @Override
    public void commit(TableMetadata underlyingBase, TableMetadata metadata) {
      if (underlyingBase != current) {
        // trigger a refresh and retry
        throw new CommitFailedException("Table metadata refresh is required");
      }

      // track the intermediate snapshot ids for rewriting the snapshot log
      // an id is intermediate if it isn't the base snapshot id and it is replaced by a new current
      Long oldId = currentId(current);
      if (oldId != null && !oldId.equals(currentId(metadata)) && !oldId.equals(currentId(base))) {
        intermediateSnapshotIds.add(oldId);
      }

      BaseTransaction.this.current = metadata;
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
    public String metadataFileLocation(String fileName) {
      return ops.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return ops.locationProvider();
    }

    @Override
    public long newSnapshotId() {
      return ops.newSnapshotId();
    }
  }

  public class TransactionTable implements Table {
    @Override
    public void refresh() {
    }

    @Override
    public TableScan newScan() {
      throw new UnsupportedOperationException("Transaction tables do not support scans");
    }

    @Override
    public Schema schema() {
      return current.schema();
    }

    @Override
    public PartitionSpec spec() {
      return current.spec();
    }

    @Override
    public Map<String, String> properties() {
      return current.properties();
    }

    @Override
    public String location() {
      return current.location();
    }

    @Override
    public Snapshot currentSnapshot() {
      return current.currentSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
      return current.snapshot(snapshotId);
    }

    @Override
    public Iterable<Snapshot> snapshots() {
      return current.snapshots();
    }

    @Override
    public UpdateSchema updateSchema() {
      return BaseTransaction.this.updateSchema();
    }

    @Override
    public UpdateProperties updateProperties() {
      return BaseTransaction.this.updateProperties();
    }

    @Override
    public UpdateLocation updateLocation() {
      return BaseTransaction.this.updateLocation();
    }

    @Override
    public AppendFiles newAppend() {
      return BaseTransaction.this.newAppend();
    }

    @Override
    public AppendFiles newFastAppend() {
      return BaseTransaction.this.newFastAppend();
    }

    @Override
    public RewriteFiles newRewrite() {
      return BaseTransaction.this.newRewrite();
    }

    @Override
    public RewriteManifests rewriteManifests() {
      return BaseTransaction.this.rewriteManifests();
    }

    @Override
    public OverwriteFiles newOverwrite() {
      return BaseTransaction.this.newOverwrite();
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
      return BaseTransaction.this.newReplacePartitions();
    }

    @Override
    public DeleteFiles newDelete() {
      return BaseTransaction.this.newDelete();
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
      return BaseTransaction.this.expireSnapshots();
    }

    @Override
    public Rollback rollback() {
      throw new UnsupportedOperationException("Transaction tables do not support rollback");
    }

    @Override
    public Transaction newTransaction() {
      throw new UnsupportedOperationException("Cannot create a transaction within a transaction");
    }

    @Override
    public FileIO io() {
      return transactionOps.io();
    }

    @Override
    public EncryptionManager encryption() {
      return transactionOps.encryption();
    }

    @Override
    public LocationProvider locationProvider() {
      return transactionOps.locationProvider();
    }
  }

  @VisibleForTesting
  TableOperations ops() {
    return ops;
  }

  @VisibleForTesting
  Set<String> deletedFiles() {
    return deletedFiles;
  }
}
