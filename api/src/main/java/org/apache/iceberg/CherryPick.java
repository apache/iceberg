package org.apache.iceberg;

public interface CherryPick extends PendingUpdate<Snapshot> {

  CherryPick fromSnapshotId(long wapId);
  
}
