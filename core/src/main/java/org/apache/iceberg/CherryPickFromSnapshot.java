package org.apache.iceberg;

import com.google.common.base.Preconditions;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;

class CherryPickFromSnapshot implements CherryPick {
  private final TableOperations ops;
  private TableMetadata base = null;
  private Long targetWapId = null;

  CherryPickFromSnapshot(TableOperations ops) {
    this.ops = ops;
    this.base = ops.refresh();
  }

  @Override
  public CherryPickFromSnapshot fromWapId(long wapId) {
    Preconditions.checkArgument(base.snapshot(wapId) != null,
        "Cannot cherry pick unknown snapshot id: %s", wapId);

    this.targetWapId = wapId;
    return this;
  }

  /**
   * Apply the pending changes and return the uncommitted changes for validation.
   * <p>
   * This does not result in a permanent update.
   *
   * @return the uncommitted changes that would be committed by calling {@link #commit()}
   * @throws ValidationException      If the pending changes cannot be applied to the current metadata
   * @throws IllegalArgumentException If the pending changes are conflicting or invalid
   */
  @Override
  public Snapshot apply() {
    ValidationException.check(targetWapId != null,
        "Cannot cherry pick unknown version: call fromWapId");

    Snapshot snapshot = base.snapshot(targetWapId);

    // only append operations are currently supported
    if (snapshot.operation().equals(DataOperations.APPEND)) {
      throw new UnsupportedOperationException("Can cherry pick append only operations");
    }

    return snapshot;
  }

  /**
   * Apply the pending changes and commit.
   * <p>
   * Changes are committed by calling the underlying table's commit method.
   * <p>
   * Once the commit is successful, the updated table will be refreshed.
   *
   * @throws ValidationException   If the update cannot be applied to the current table metadata.
   * @throws CommitFailedException If the update cannot be committed due to conflicts.
   */
  @Override
  public void commit() {
    // Todo: Need to add retry
    base = ops.refresh(); // refresh
    ops.commit(base, base.cherrypickFrom(apply()));
  }
}
