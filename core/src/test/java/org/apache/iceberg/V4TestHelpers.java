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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
    return dataFile(tracking, location, null, null, null, null, null);
  }

  static TrackedFile dataFile(String location, Integer specId, PartitionData partition) {
    return dataFile(ADDED_TRACKING, location, specId, partition, null, null, null);
  }

  static TrackedFile dataFile(
      String location,
      Integer specId,
      PartitionData partition,
      ContentStats stats,
      DeletionVector dv) {
    return dataFile(ADDED_TRACKING, location, specId, partition, stats, dv, null);
  }

  static TrackedFile dataFile(
      String location,
      Integer specId,
      PartitionData partition,
      ContentStats stats,
      DeletionVector dv,
      List<ColumnFile> columnFiles) {
    return dataFile(ADDED_TRACKING, location, specId, partition, stats, dv, columnFiles);
  }

  static TrackedFile dataFileWithStats(String location, ContentStats stats) {
    return dataFile(ADDED_TRACKING, location, null, null, stats, null, null);
  }

  static TrackedFile dataFileWithDV(String location, DeletionVector dv) {
    return dataFile(ADDED_TRACKING, location, null, null, null, dv, null);
  }

  static TrackedFile dataFileWithColumnFiles(String location, List<ColumnFile> columnFiles) {
    return dataFile(ADDED_TRACKING, location, null, null, null, null, columnFiles);
  }

  static TrackedFile dataFileWithColumnFiles(
      Tracking tracking, String location, List<ColumnFile> columnFiles) {
    return dataFile(tracking, location, null, null, null, null, columnFiles);
  }

  private static TrackedFile dataFile(
      Tracking tracking,
      String location,
      Integer specId,
      PartitionData partition,
      ContentStats stats,
      DeletionVector dv,
      List<ColumnFile> columnFiles) {
    return trackedFile(
        tracking,
        FileContent.DATA,
        location,
        specId,
        partition,
        stats,
        dv,
        null /* manifestInfo */,
        columnFiles);
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
        null); // columnFiles
  }

  static TrackedFile manifestRefWithColumnFiles(
      FileContent content,
      String location,
      ManifestInfo manifestInfo,
      List<ColumnFile> columnFiles) {
    return trackedFile(
        ADDED_TRACKING,
        content,
        location,
        null, // specId
        null, // partition
        null, // stats
        null, // dv
        manifestInfo,
        columnFiles);
  }

  static TrackedFile copyWithTracking(TrackedFile file, Tracking tracking) {
    return new TrackedFileStruct(
        tracking,
        file.contentType(),
        file.location(),
        file.fileFormat(),
        file.recordCount(),
        file.fileSizeInBytes(),
        file.specId(),
        file.partition(),
        file.contentStats(),
        file.sortOrderId(),
        file.deletionVector(),
        file.manifestInfo(),
        file.keyMetadata(),
        file.splitOffsets(),
        file.columnFiles());
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
      List<ColumnFile> columnFiles) {
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
        false, // tightStats
        null, // sortOrderId
        dv,
        manifestInfo,
        null, // keyMetadata
        null, // splitOffsets
        columnFiles);
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

  static ColumnFile columnFile(String location, Set<Integer> fieldIds) {
    return columnFile(location, fieldIds, null);
  }

  static ColumnFile columnFile(String location, Set<Integer> fieldIds, ByteBuffer keyMetadata) {
    return new ColumnFileStruct(
        location, fieldIds, FileFormat.fromFileName(location), FILE_SIZE_IN_BYTES, keyMetadata);
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
    try (FileAppender<StructLike> appender = newAppender(out, format, partitionType, statsType)) {
      for (TrackedFile file : files) {
        appender.add((StructLike) file);
      }
    }

    return out;
  }

  static OutputFile writeRows(
      FileIO io,
      FileFormat format,
      Types.StructType partitionType,
      Types.StructType statsType,
      Iterable<StructLike> rows)
      throws IOException {
    OutputFile out = io.newOutputFile(format.addExtension("manifest." + System.nanoTime()));
    try (FileAppender<StructLike> appender = newAppender(out, format, partitionType, statsType)) {
      appender.addAll(rows);
    }

    return out;
  }

  private static FileAppender<StructLike> newAppender(
      OutputFile out, FileFormat format, Types.StructType partitionType, Types.StructType statsType)
      throws IOException {
    Schema writeSchema = TrackedFile.schema(partitionType, statsType);
    return InternalData.write(format, out).schema(writeSchema).named("tracked_file").build();
  }

  static List<TrackedFile> readTrackedFiles(
      InputFile in, FileFormat format, Types.StructType partitionType, Types.StructType statsType)
      throws IOException {
    InternalData.ReadBuilder readBuilder =
        InternalData.read(format, in)
            .project(TrackedFile.readSchema(partitionType, statsType))
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .setCustomType(
                TrackedFile.COLUMN_FILES.type().asListType().elementId(), ColumnFileStruct.class);
    if (!partitionType.fields().isEmpty()) {
      readBuilder.setCustomType(TrackedFile.PARTITION_ID, PartitionData.class);
    }

    if (!statsType.fields().isEmpty()) {
      readBuilder.setCustomType(TrackedFile.CONTENT_STATS_ID, ContentStatsStruct.class);
      for (Types.NestedField fieldStats : statsType.fields()) {
        readBuilder.setCustomType(fieldStats.fieldId(), FieldStatsStruct.class);
        Types.StructType fieldStatsType = fieldStats.type().asStructType();
        Types.NestedField lowerBound = fieldStatsType.field(StatsUtil.LOWER_BOUND_NAME);
        Types.NestedField upperBound = fieldStatsType.field(StatsUtil.UPPER_BOUND_NAME);
        if (lowerBound != null && lowerBound.type().isStructType()) {
          readBuilder.setCustomType(lowerBound.fieldId(), GeospatialBound.class);
          readBuilder.setCustomType(upperBound.fieldId(), GeospatialBound.class);
        }
      }
    }

    List<TrackedFile> files = Lists.newArrayList();
    try (CloseableIterable<TrackedFile> reader = readBuilder.build()) {
      for (TrackedFile file : reader) {
        files.add(file.copy());
      }
    }

    return files;
  }
}
