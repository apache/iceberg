/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.iceberg.exceptions.CommitFailedException;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.util.Tasks;
import java.util.List;
import java.util.Map;

import static com.netflix.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static com.netflix.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

class BaseTransaction implements Transaction {
  static Transaction createTableTransaction(TableOperations ops, TableMetadata start) {
    Preconditions.checkArgument(ops.current() == null,
        "Cannot start create table transaction: table already exists");
    return new BaseTransaction(ops, start);
  }

  static Transaction newTransaction(TableOperations ops) {
    return new BaseTransaction(ops, ops.refresh());
  }

  private final TransactionTable transactionTable;
  private final TableOperations ops;
  private final TableOperations transactionOps;
  private final List<PendingUpdate> updates;
  private TableMetadata base;
  private TableMetadata lastBase;
  private TableMetadata current;

  private BaseTransaction(TableOperations ops, TableMetadata start) {
    this.transactionTable = new TransactionTable();
    this.ops = ops;
    this.transactionOps = new TransactionTableOperations();
    this.updates = Lists.newArrayList();
    this.base = ops.current();
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
  public UpdateProperties updateProperties() {
    checkLastOperationCommitted("UpdateProperties");
    UpdateProperties props = new PropertiesUpdate(transactionOps);
    updates.add(props);
    return props;
  }

  @Override
  public AppendFiles newAppend() {
    checkLastOperationCommitted("AppendFiles");
    AppendFiles append = new MergeAppend(transactionOps);
    updates.add(append);
    return append;
  }

  @Override
  public RewriteFiles newRewrite() {
    checkLastOperationCommitted("RewriteFiles");
    RewriteFiles rewrite = new ReplaceFiles(transactionOps);
    updates.add(rewrite);
    return rewrite;
  }

  @Override
  public DeleteFiles newDelete() {
    checkLastOperationCommitted("DeleteFiles");
    DeleteFiles delete = new StreamingDelete(transactionOps);
    updates.add(delete);
    return delete;
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    checkLastOperationCommitted("ExpireSnapshots");
    ExpireSnapshots expire = new RemoveSnapshots(transactionOps);
    updates.add(expire);
    return expire;
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(lastBase != current,
        "Cannot commit transaction: last operation has not committed");
    if (base != null) {
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
          .run(ops -> {
            if (base != ops.refresh()) {
              this.base = ops.current(); // just refreshed
              this.current = base;
              for (PendingUpdate update : updates) {
                // re-commit each update in the chain to apply it and update current
                update.commit();
              }
            }

            ops.commit(base, current);
          });

    } else {
      // this operation creates the table. if the commit fails, this cannot retry because another
      // process has created the same table.
      ops.commit(null, current);
    }
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
    public void commit(TableMetadata base, TableMetadata metadata) {
      if (base != current) {
        // trigger a refresh and retry
        throw new CommitFailedException("Table metadata refresh is required");
      }
      BaseTransaction.this.current = metadata;
    }

    @Override
    public InputFile newInputFile(String path) {
      return ops.newInputFile(path);
    }

    @Override
    public OutputFile newMetadataFile(String filename) {
      return ops.newMetadataFile(filename);
    }

    @Override
    public void deleteFile(String path) {
      ops.deleteFile(path);
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
    public Iterable<Snapshot> snapshots() {
      return current.snapshots();
    }

    @Override
    public UpdateSchema updateSchema() {
      throw new UnsupportedOperationException("Transaction tables do not support schema updates");
    }

    @Override
    public UpdateProperties updateProperties() {
      return BaseTransaction.this.updateProperties();
    }

    @Override
    public AppendFiles newAppend() {
      return BaseTransaction.this.newAppend();
    }

    @Override
    public RewriteFiles newRewrite() {
      return BaseTransaction.this.newRewrite();
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
  }
}
