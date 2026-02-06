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

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class GenericStatisticsFile implements StatisticsFile {
  private final long snapshotId;
  private final String path;
  private final long fileSizeInBytes;
  private final long fileFooterSizeInBytes;
  private final List<BlobMetadata> blobMetadata;

  public GenericStatisticsFile(
      long snapshotId,
      String path,
      long fileSizeInBytes,
      long fileFooterSizeInBytes,
      List<BlobMetadata> blobMetadata) {
    Preconditions.checkNotNull(path, "path is null");
    Preconditions.checkNotNull(blobMetadata, "blobMetadata is null");
    this.snapshotId = snapshotId;
    this.path = path;
    this.fileSizeInBytes = fileSizeInBytes;
    this.fileFooterSizeInBytes = fileFooterSizeInBytes;
    this.blobMetadata = ImmutableList.copyOf(blobMetadata);
  }

  @Override
  public long snapshotId() {
    return snapshotId;
  }

  @Override
  public String path() {
    return path;
  }

  @Override
  public long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  @Override
  public long fileFooterSizeInBytes() {
    return fileFooterSizeInBytes;
  }

  @Override
  public List<BlobMetadata> blobMetadata() {
    return blobMetadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GenericStatisticsFile that = (GenericStatisticsFile) o;
    return snapshotId == that.snapshotId
        && fileSizeInBytes == that.fileSizeInBytes
        && fileFooterSizeInBytes == that.fileFooterSizeInBytes
        && Objects.equals(path, that.path)
        && Objects.equals(blobMetadata, that.blobMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotId, path, fileSizeInBytes, fileFooterSizeInBytes, blobMetadata);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GenericStatisticsFile.class.getSimpleName() + "[", "]")
        .add("snapshotId=" + snapshotId)
        .add("path='" + path + "'")
        .add("fileSizeInBytes=" + fileSizeInBytes)
        .add("fileFooterSizeInBytes=" + fileFooterSizeInBytes)
        .add("blobMetadata=" + blobMetadata)
        .toString();
  }
}
