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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Metadata about a leaf file written during index build.
 *
 * <p>Captures the path, size, record count, and transform-value bounds of
 * one leaf file. These fields are written to the tracking file so the planner
 * can select the right leaf files at planning time without opening them.
 */
public class LeafFileMetadata {

  private final String path;
  private final String fileFormat;
  private final long recordCount;
  private final long fileSizeInBytes;
  private final long transformValueMin;
  private final long transformValueMax;

  public LeafFileMetadata(
      String path,
      String fileFormat,
      long recordCount,
      long fileSizeInBytes,
      long transformValueMin,
      long transformValueMax) {
    Preconditions.checkArgument(path != null && !path.isEmpty(), "path is required");
    Preconditions.checkArgument(
        transformValueMin <= transformValueMax,
        "transformValueMin (%s) must be <= transformValueMax (%s)",
        transformValueMin, transformValueMax);
    this.path = path;
    this.fileFormat = fileFormat != null ? fileFormat : "parquet";
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSizeInBytes;
    this.transformValueMin = transformValueMin;
    this.transformValueMax = transformValueMax;
  }

  public String path() {
    return path;
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

  /** Minimum transform value (e.g. hash bucket) stored in this leaf file. */
  public long transformValueMin() {
    return transformValueMin;
  }

  /** Maximum transform value (e.g. hash bucket) stored in this leaf file. */
  public long transformValueMax() {
    return transformValueMax;
  }

  /** Convert to a {@link TrackingFileEntry} for writing to the tracking file. */
  public TrackingFileEntry toTrackingEntry() {
    return TrackingFileEntry.builder()
        .location(path)
        .fileFormat(fileFormat)
        .recordCount(recordCount)
        .fileSizeInBytes(fileSizeInBytes)
        .transformValueLowerBound(transformValueMin)
        .transformValueUpperBound(transformValueMax)
        .build();
  }
}
