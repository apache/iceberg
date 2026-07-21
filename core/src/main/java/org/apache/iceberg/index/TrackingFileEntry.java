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
package org.apache.iceberg.index;

import java.nio.ByteBuffer;
import javax.annotation.Nullable;

/**
 * A single entry in a tracking file, describing one leaf file in an index snapshot.
 *
 * <p>The {@link #transformValueLowerBound()} and {@link #transformValueUpperBound()} fields are
 * the key pruning statistics — the planner uses them to skip leaf files that cannot contain
 * matching entries for a given predicate.
 *
 * <p>Field IDs match the tracking file entry schema defined in format/index.md.
 */
public class TrackingFileEntry {

  private final String location;           // field 100
  private final String fileFormat;         // field 101
  private final long recordCount;          // field 103
  private final long fileSizeInBytes;      // field 104
  private final long transformValueLowerBound;  // field 146.lower
  private final long transformValueUpperBound;  // field 146.upper
  private final ByteBuffer keyMetadata;    // field 131 (optional)

  private TrackingFileEntry(
      String location,
      String fileFormat,
      long recordCount,
      long fileSizeInBytes,
      long transformValueLowerBound,
      long transformValueUpperBound,
      ByteBuffer keyMetadata) {
    this.location = location;
    this.fileFormat = fileFormat;
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSizeInBytes;
    this.transformValueLowerBound = transformValueLowerBound;
    this.transformValueUpperBound = transformValueUpperBound;
    this.keyMetadata = keyMetadata;
  }

  public String location() {
    return location;
  }

  public String fileFormat() {
    return fileFormat;
  }

  public long recordCount() {
    return recordCount;
  }

  public long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  /** Minimum transform value stored in this leaf file. Used for planning-time pruning. */
  public long transformValueLowerBound() {
    return transformValueLowerBound;
  }

  /** Maximum transform value stored in this leaf file. Used for planning-time pruning. */
  public long transformValueUpperBound() {
    return transformValueUpperBound;
  }

  @Nullable
  public ByteBuffer keyMetadata() {
    return keyMetadata;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String location;
    private String fileFormat = "parquet";
    private long recordCount;
    private long fileSizeInBytes;
    private long transformValueLowerBound;
    private long transformValueUpperBound;
    private ByteBuffer keyMetadata;

    public Builder location(String loc) {
      this.location = loc;
      return this;
    }

    public Builder fileFormat(String format) {
      this.fileFormat = format;
      return this;
    }

    public Builder recordCount(long count) {
      this.recordCount = count;
      return this;
    }

    public Builder fileSizeInBytes(long size) {
      this.fileSizeInBytes = size;
      return this;
    }

    public Builder transformValueLowerBound(long lower) {
      this.transformValueLowerBound = lower;
      return this;
    }

    public Builder transformValueUpperBound(long upper) {
      this.transformValueUpperBound = upper;
      return this;
    }

    public Builder keyMetadata(ByteBuffer metadata) {
      this.keyMetadata = metadata;
      return this;
    }

    public TrackingFileEntry build() {
      if (location == null || location.isEmpty()) {
        throw new IllegalArgumentException("location is required");
      }
      return new TrackingFileEntry(
          location,
          fileFormat,
          recordCount,
          fileSizeInBytes,
          transformValueLowerBound,
          transformValueUpperBound,
          keyMetadata);
    }
  }
}
