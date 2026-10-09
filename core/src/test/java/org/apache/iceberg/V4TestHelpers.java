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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

/** Factories for building V4 {@link TrackedFile}s and manifests in tests. */
class V4TestHelpers {
  private V4TestHelpers() {}

  static final long SNAPSHOT_ID = 42L;
  static final long RECORD_COUNT = 100L;
  static final long FILE_SIZE_IN_BYTES = 1024L;
  static final int FORMAT_VERSION_V4 = 4;
  static final Tracking ADDED_TRACKING = TrackingBuilder.added(SNAPSHOT_ID).build();

  static TrackedFile dataFile(Tracking tracking, String location) {
    return dataFile(tracking, location, null, null, null, null);
  }

  static TrackedFile dataFile(String location, Integer specId, PartitionData partition) {
    return dataFile(ADDED_TRACKING, location, specId, partition, null, null);
  }

  static TrackedFile dataFile(
      String location,
      Integer specId,
      PartitionData partition,
      ContentStats stats,
      DeletionVector dv) {
    return dataFile(ADDED_TRACKING, location, specId, partition, stats, dv);
  }

  static TrackedFile dataFileWithStats(String location, ContentStats stats) {
    return dataFile(ADDED_TRACKING, location, null, null, stats, null);
  }

  static TrackedFile dataFileWithDV(String location, DeletionVector dv) {
    return dataFile(ADDED_TRACKING, location, null, null, null, dv);
  }

  private static TrackedFile dataFile(
      Tracking tracking,
      String location,
      Integer specId,
      PartitionData partition,
      ContentStats stats,
      DeletionVector dv) {
    return trackedFile(
        tracking,
        FileContent.DATA,
        location,
        specId,
        partition,
        stats,
        dv,
        null, // manifestInfo
        null); // equalityIds
  }

  static TrackedFile deleteFile(
      FileContent content,
      String location,
      Integer specId,
      PartitionData partition,
      List<Integer> equalityIds) {
    return trackedFile(
        ADDED_TRACKING,
        content,
        location,
        specId,
        partition,
        null, // stats
        null, // dv
        null, // manifestInfo
        equalityIds);
  }

  static TrackedFile manifestRef(FileContent content, String location, ManifestInfo manifestInfo) {
    return manifestRefWithStats(content, location, null, manifestInfo);
  }

  static TrackedFile manifestRefWithStats(
      FileContent content, String location, ContentStats stats, ManifestInfo manifestInfo) {
    return trackedFile(
        ADDED_TRACKING,
        content,
        location,
        null, // specId
        null, // partition
        stats,
        null, // dv
        manifestInfo,
        null); // equalityIds
  }

  private static TrackedFile trackedFile(
      Tracking tracking,
      FileContent content,
      String location,
      Integer specId,
      PartitionData partition,
      ContentStats stats,
      DeletionVector dv,
      ManifestInfo manifestInfo,
      List<Integer> equalityIds) {
    return new TrackedFileStruct(
        tracking,
        content,
        location,
        FileFormat.fromFileName(location),
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        specId,
        partition,
        stats,
        null, // sortOrderId
        dv,
        manifestInfo,
        null, // keyMetadata
        null, // splitOffsets
        equalityIds);
  }

  static DeletionVector deletionVector(String location) {
    return deletionVector(location, 100L, 50L, 5L);
  }

  static DeletionVector deletionVector(
      String location, long offset, long sizeInBytes, long cardinality) {
    return DeletionVectorStruct.builder()
        .location(location)
        .offset(offset)
        .sizeInBytes(sizeInBytes)
        .cardinality(cardinality)
        .build();
  }

  static PartitionData partition(PartitionSpec spec, Object... values) {
    PartitionData data = new PartitionData(spec.partitionType());
    for (int i = 0; i < values.length; i += 1) {
      data.set(i, values[i]);
    }

    return data;
  }

  static OutputFile writeTrackedFiles(
      FileIO io,
      FileFormat format,
      Types.StructType partitionType,
      Types.StructType statsType,
      Iterable<TrackedFile> files)
      throws IOException {
    OutputFile out = io.newOutputFile(format.addExtension("manifest." + System.nanoTime()));
    return writeTrackedFiles(out, format, partitionType, statsType, files);
  }

  static OutputFile writeTrackedFiles(
      OutputFile out,
      FileFormat format,
      Types.StructType partitionType,
      Types.StructType statsType,
      Iterable<TrackedFile> files)
      throws IOException {
    Schema writeSchema = TrackedFile.schema(partitionType, statsType);
    try (FileAppender<StructLike> appender =
        InternalData.write(format, out).schema(writeSchema).named("tracked_file").build()) {
      for (TrackedFile file : files) {
        appender.add((StructLike) file);
      }
    }

    return out;
  }
}
