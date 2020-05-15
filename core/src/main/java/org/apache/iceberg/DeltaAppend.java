package org.apache.iceberg;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.Exceptions;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iceberg.TableProperties.*;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

public class DeltaAppend implements AppendDeltaFiles {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaAppend.class);

  private final TableOperations ops;
  private volatile Long snapshotId = null;
  private TableMetadata tableMetadata;
  private final List<DeltaFile> newFiles = Lists.newArrayList();

  public DeltaAppend(TableOperations ops) {
    this.ops = ops;
    this.tableMetadata = ops.current();
  }

  @Override
  public AppendDeltaFiles appendFile(DeltaFile deltaFile) {
    newFiles.add(deltaFile);
    return this;
  }

  @Override
  public DeltaSnapshot apply() {
    this.tableMetadata = refresh();
    DeltaSnapshot currentSnapshot = tableMetadata.currentDeltaSnapshot();
    long snapshotId = snapshotId();
    return new BaseDeltaSnapshot(snapshotId, currentSnapshot, System.currentTimeMillis(), newFiles);
  }

  @Override
  public void commit() {
    // this is always set to the latest commit attempt's snapshot id.
    AtomicLong newSnapshotId = new AtomicLong(-1L);
    try {
      Tasks.foreach(ops)
              .retry(tableMetadata.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
              .exponentialBackoff(
                      tableMetadata.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
                      tableMetadata.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
                      tableMetadata.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
                      2.0 /* exponential */)
              .onlyRetryOn(CommitFailedException.class)
              .run(taskOps -> {
                DeltaSnapshot newSnapshot = apply();
                newSnapshotId.set(newSnapshot.snapshotId());
                taskOps.deltaCommit(newSnapshot);
              });

    } catch (RuntimeException e) {
      Exceptions.suppressAndThrow(e, () -> {});
    }

    LOG.info("Committed delta snapshot {} ({})", newSnapshotId.get(), getClass().getSimpleName());
  }

  protected TableMetadata refresh() {
    this.tableMetadata = ops.refresh();
    return tableMetadata;
  }

  protected long snapshotId() {
    if (snapshotId == null) {
      synchronized (this) {
        if (snapshotId == null) {
          this.snapshotId = ops.newSnapshotId();
        }
      }
    }
    return snapshotId;
  }
}
