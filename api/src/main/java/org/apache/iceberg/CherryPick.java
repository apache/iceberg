package org.apache.iceberg;

public interface CherryPick extends PendingUpdate<Snapshot> {

  public CherryPick fromWapId(long wapId);

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
  public Snapshot apply();
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
  public void commit();
}
