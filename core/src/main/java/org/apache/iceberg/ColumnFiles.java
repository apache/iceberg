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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class ColumnFiles {

  private ColumnFiles() {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Integer formatVersion = null;
    private List<Integer> fieldIds = null;
    private String location = null;
    private FileFormat fileFormat = null;
    private Long fileSizeInBytes = null;
    private ByteBuffer keyMetadata = null;
    private List<Long> splitOffsets = null;

    private Builder() {}

    public Builder withFormatVersion(int newFormatVersion) {
      Preconditions.checkArgument(
          newFormatVersion >= 0, "Invalid format version: %s (must be >= 0)", newFormatVersion);
      this.formatVersion = newFormatVersion;
      return this;
    }

    public Builder withFieldIds(List<Integer> newFieldIds) {
      Preconditions.checkArgument(newFieldIds != null, "Invalid field IDs: null");
      Preconditions.checkArgument(!newFieldIds.isEmpty(), "Invalid field IDs: empty");
      Preconditions.checkArgument(
          Sets.newHashSet(newFieldIds).size() == newFieldIds.size(),
          "Invalid field IDs: duplicated IDs found in: %s",
          newFieldIds);
      this.fieldIds = newFieldIds;
      return this;
    }

    public Builder withLocation(String newLocation) {
      Preconditions.checkArgument(newLocation != null, "Invalid location: null");
      Preconditions.checkArgument(!newLocation.isEmpty(), "Invalid location: empty");
      this.location = newLocation;
      return this;
    }

    public Builder withFileFormat(FileFormat newFileFormat) {
      Preconditions.checkArgument(newFileFormat != null, "Invalid file format: null");
      this.fileFormat = newFileFormat;
      return this;
    }

    public Builder withFileSizeInBytes(long newFileSizeInBytes) {
      Preconditions.checkArgument(
          newFileSizeInBytes >= 0,
          "Invalid file size in bytes: %s (must be >= 0)",
          newFileSizeInBytes);
      this.fileSizeInBytes = newFileSizeInBytes;
      return this;
    }

    public Builder withKeyMetadata(ByteBuffer newKeyMetadata) {
      Preconditions.checkArgument(newKeyMetadata != null, "Invalid key metadata: null");
      this.keyMetadata = newKeyMetadata;
      return this;
    }

    public Builder withSplitOffsets(List<Long> newSplitOffsets) {
      Preconditions.checkArgument(newSplitOffsets != null, "Invalid split offsets: null");
      this.splitOffsets = newSplitOffsets;
      return this;
    }

    public ColumnFile build() {
      Preconditions.checkArgument(formatVersion != null, "Missing required value: format version");
      Preconditions.checkArgument(fieldIds != null, "Missing required value: field IDs");
      Preconditions.checkArgument(location != null, "Missing required value: location");
      Preconditions.checkArgument(fileFormat != null, "Missing required value: file format");
      Preconditions.checkArgument(
          fileSizeInBytes != null, "Missing required value: file size in bytes");
      return new ColumnFileStruct(
          formatVersion,
          fieldIds,
          location,
          fileFormat,
          fileSizeInBytes,
          keyMetadata,
          splitOffsets);
    }
  }
}
