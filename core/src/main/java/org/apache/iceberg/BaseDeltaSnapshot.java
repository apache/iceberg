package org.apache.iceberg;

import org.apache.iceberg.io.InputFile;

import java.util.List;

public class BaseDeltaSnapshot implements DeltaSnapshot {

  private final long snapshotId;
  private final long timestampMillis;
  private final List<DeltaFile> deltaFiles;
  private final DeltaSnapshot parentSnapshot;

  private InputFile manifestFile;

  public BaseDeltaSnapshot(long snapshotId, DeltaSnapshot parentSnapshot, long timestampMillis, List<DeltaFile> deltaFiles) {
    this.snapshotId = snapshotId;
    this.timestampMillis = timestampMillis;
    this.deltaFiles = deltaFiles;
    this.parentSnapshot = parentSnapshot;
  }

  @Override
  public long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long parentId() {
    return parentSnapshot == null ? null : parentSnapshot.snapshotId();
  }

  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  @Override
  public Iterable<DeltaFile> deltaFiles() {
    return deltaFiles;
  }

  @Override
  public String manifestLocation() {
    return manifestFile != null ? manifestFile.location() : null;
  }

  @Override
  public String parentManifestLocation() {
    return parentSnapshot == null ? null : parentSnapshot.manifestLocation();
  }

  @Override
  public DeltaSnapshot parent() {
    return parentSnapshot;
  }
}
