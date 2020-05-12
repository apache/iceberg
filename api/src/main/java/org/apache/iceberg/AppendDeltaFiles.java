package org.apache.iceberg;

/**
 * API for appending new delta files in a table.
 * <p>
 * This API accumulates delta file additions, produces a new {@link DeltaSnapshot} of the table, and commits
 * that snapshot as the current.
 * <p>
 * When committing, these changes will be applied to the latest table delta snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 */
public interface AppendDeltaFiles extends PendingUpdate<DeltaSnapshot> {

  /**
   * Append a {@link DeltaFile} to the table.
   *
   * @param deltaFile a delta file
   * @return this for method chaining
   */
  AppendDeltaFiles appendFile(DeltaFile deltaFile);
}
