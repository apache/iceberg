package org.apache.iceberg;

/**
 * A snapshot of the delta data in a mutable table at a point in time.
 * Snapshots are created by table operations, like {@link AppendDeltaFiles}.
 */
public interface DeltaSnapshot {
  /**
   * Return this snapshot's ID.
   *
   * @return a long ID
   */
  long snapshotId();

  /**
   * Return this snapshot's parent
   * @return a long ID for this snapshot's parent, or -1 if it has no parent
   */
  Long parentId();

  /**
   * Return this snapshot's timestamp.
   * <p>
   * This timestamp is the same as those produced by {@link System#currentTimeMillis()}.
   *
   * @return a long timestamp in milliseconds
   */
  long timestampMillis();

  /**
   * Return all delta files added to the table in this snapshot.
   * <p>
   * The files returned include the following columns: file_path, file_format, partition,
   * record_count, and file_size_in_bytes. Other columns will be null.
   *
   * @return all files added to the table in this snapshot.
   */
  Iterable<DeltaFile> deltaFiles();

  /**
   * Return the location of this snapshot's manifest list, or null if it is not separate.
   *
   * @return the location of the manifest list for this Snapshot
   */
  String manifestLocation();
}
