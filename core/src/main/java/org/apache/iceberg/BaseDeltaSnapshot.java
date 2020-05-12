package org.apache.iceberg;

import org.apache.iceberg.io.InputFile;

import java.util.List;

public class BaseDeltaSnapshot implements DeltaSnapshot {

  private final long snapshotId;
  private final Long parentId;
  private final long timestampMillis;
  private final InputFile manifestFile;

  // lazily initialized
  private List<DeltaFile> cachedDeltas = null;

  public BaseDeltaSnapshot(long snapshotId, Long parentId, long timestampMillis, InputFile manifestFile) {
    this.snapshotId = snapshotId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.manifestFile = manifestFile;
  }

  @Override
  public long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long parentId() {
    return parentId;
  }

  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  @Override
  public Iterable<DeltaFile> deltaFiles() {
    return null;
  }

  @Override
  public String manifestLocation() {
    return manifestFile != null ? manifestFile.location() : null;
  }
}
