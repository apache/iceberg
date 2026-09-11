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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ByteBuffers;

/**
 * A {@link ManifestFile} for the root manifest of a v4 adaptive metadata tree.
 *
 * <p>The root is referenced by location (like a manifest list), spans partition specs, and does not
 * store per-manifest aggregates. Accessors for values it does not carry throw {@link
 * UnsupportedOperationException}.
 */
class RootManifestFile implements ManifestFile {
  private static final int FORMAT_VERSION = 4;

  private final InputFile file;
  private final long snapshotId;
  private final byte[] keyMetadata;
  private Long length;

  RootManifestFile(InputFile file, long snapshotId, ByteBuffer keyMetadata) {
    Preconditions.checkArgument(file != null, "Invalid file: null");
    this.file = file;
    this.snapshotId = snapshotId;
    this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
    this.length = null;
  }

  @Override
  public String path() {
    return file.location();
  }

  @Override
  public long length() {
    if (length == null) {
      this.length = file.getLength();
    }

    return length;
  }

  @Override
  public int partitionSpecId() {
    throw new UnsupportedOperationException("Root manifest has no partition spec");
  }

  @Override
  public ManifestContent content() {
    return ManifestContent.DATA;
  }

  @Override
  public long sequenceNumber() {
    throw new UnsupportedOperationException("Root manifest has no sequence number");
  }

  @Override
  public long minSequenceNumber() {
    throw new UnsupportedOperationException("Root manifest has no minimum sequence number");
  }

  @Override
  public Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Integer addedFilesCount() {
    throw new UnsupportedOperationException("Root manifest has no added files count");
  }

  @Override
  public Long addedRowsCount() {
    throw new UnsupportedOperationException("Root manifest has no added rows count");
  }

  @Override
  public Integer existingFilesCount() {
    throw new UnsupportedOperationException("Root manifest has no existing files count");
  }

  @Override
  public Long existingRowsCount() {
    throw new UnsupportedOperationException("Root manifest has no existing rows count");
  }

  @Override
  public Integer deletedFilesCount() {
    throw new UnsupportedOperationException("Root manifest has no deleted files count");
  }

  @Override
  public Long deletedRowsCount() {
    throw new UnsupportedOperationException("Root manifest has no deleted rows count");
  }

  @Override
  public List<PartitionFieldSummary> partitions() {
    throw new UnsupportedOperationException("Root manifest has no partition summaries");
  }

  @Override
  public ByteBuffer keyMetadata() {
    return keyMetadata == null ? null : ByteBuffer.wrap(keyMetadata);
  }

  @Override
  public int formatVersion() {
    return FORMAT_VERSION;
  }

  @Override
  public ManifestFile copy() {
    return new RootManifestFile(file, snapshotId, keyMetadata());
  }
}
