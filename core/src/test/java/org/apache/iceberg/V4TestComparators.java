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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Comparators for manifest interfaces that compare the values returned by API methods.
 *
 * <p>Comparators for methods that return an object are null tolerant, as are the comparators for
 * the interfaces themselves.
 */
class V4TestComparators {
  private V4TestComparators() {}

  public static Comparator<TrackedFile> trackedFileStatusOnly(Types.StructType partitionType) {
    return trackedFile(STATUS_ONLY_TRACKING, partitionType);
  }

  public static Comparator<TrackedFile> trackedFile(Types.StructType partitionType) {
    return trackedFile(TRACKING, partitionType);
  }

  private static Comparator<TrackedFile> trackedFile(
      Comparator<Tracking> trackingComparator, Types.StructType partitionType) {
    Comparator<StructLike> partitionComparator =
        Comparator.nullsFirst(Comparators.forType(partitionType));

    return Comparator.nullsFirst(
        Comparator.comparing(TrackedFile::tracking, trackingComparator)
            .thenComparing(TrackedFile::contentType, natural())
            .thenComparingInt(TrackedFile::formatVersion)
            .thenComparing(TrackedFile::location, natural())
            .thenComparing(TrackedFile::fileFormat, natural())
            .thenComparingLong(TrackedFile::recordCount)
            .thenComparingLong(TrackedFile::fileSizeInBytes)
            .thenComparing(TrackedFile::specId, natural())
            .thenComparing(TrackedFile::partition, partitionComparator)
            .thenComparing(TrackedFile::contentStats, CONTENT_STATS)
            .thenComparing(TrackedFile::sortOrderId, natural())
            .thenComparing(TrackedFile::deletionVector, DELETION_VECTOR)
            .thenComparing(TrackedFile::manifestInfo, MANIFEST_INFO)
            .thenComparing(TrackedFile::keyMetadata, BYTES)
            .thenComparing(TrackedFile::splitOffsets, SPLIT_OFFSETS)
            .thenComparing(TrackedFile::equalityIds, EQ_IDS));
  }

  // convenience method for a null-safe natural order comparator
  private static <T extends Comparable<T>> Comparator<T> natural() {
    return Comparator.nullsFirst(Comparator.naturalOrder());
  }

  private static final Comparator<ByteBuffer> BYTES =
      Comparator.nullsFirst(Comparators.unsignedBytes());
  private static final Comparator<List<Long>> SPLIT_OFFSETS =
      Comparator.nullsFirst(Comparators.forType(TrackedFile.SPLIT_OFFSETS.type().asListType()));
  private static final Comparator<List<Integer>> EQ_IDS =
      Comparator.nullsFirst(Comparators.forType(TrackedFile.EQUALITY_IDS.type().asListType()));

  static final Comparator<Tracking> TRACKING =
      Comparator.nullsFirst(
          Comparator.comparing(Tracking::status, natural())
              .thenComparing(Tracking::snapshotId, natural())
              .thenComparing(Tracking::dataSequenceNumber, natural())
              .thenComparing(Tracking::fileSequenceNumber, natural())
              .thenComparing(Tracking::dvSnapshotId, natural())
              .thenComparing(Tracking::firstRowId, natural())
              .thenComparing(Tracking::deletedPositions, BYTES)
              .thenComparing(Tracking::replacedPositions, BYTES)
              .thenComparing(Tracking::manifestLocation, natural())
              .thenComparingLong(Tracking::manifestPos));

  // compare only status, ignoring inherited fields and fields set during a write
  static final Comparator<Tracking> STATUS_ONLY_TRACKING =
      Comparator.nullsFirst(Comparator.comparing(Tracking::status, natural()));

  private static final Comparator<DeletionVector> DELETION_VECTOR =
      Comparator.nullsFirst(
          Comparator.comparing(DeletionVector::location, natural())
              .thenComparingLong(DeletionVector::offset)
              .thenComparingLong(DeletionVector::sizeInBytes)
              .thenComparingLong(DeletionVector::cardinality)
              .thenComparing(DeletionVector::keyMetadata, BYTES));

  private static final Comparator<ManifestInfo> MANIFEST_INFO =
      Comparator.nullsFirst(
          Comparator.comparingInt(ManifestInfo::addedFilesCount)
              .thenComparingInt(ManifestInfo::existingFilesCount)
              .thenComparingInt(ManifestInfo::deletedFilesCount)
              .thenComparingInt(ManifestInfo::replacedFilesCount)
              .thenComparingLong(ManifestInfo::addedRowsCount)
              .thenComparingLong(ManifestInfo::existingRowsCount)
              .thenComparingLong(ManifestInfo::deletedRowsCount)
              .thenComparingLong(ManifestInfo::replacedRowsCount)
              .thenComparingLong(ManifestInfo::minSequenceNumber)
              .thenComparing(ManifestInfo::dv, BYTES)
              .thenComparing(ManifestInfo::dvCardinality, natural()));

  private static final Comparator<ContentStats> CONTENT_STATS =
      Comparator.nullsFirst(new ContentStatsComparator());

  private static class ContentStatsComparator implements Comparator<ContentStats> {
    @Override
    public int compare(ContentStats left, ContentStats right) {
      Map<Integer, FieldStats<?>> leftStats = statsById(left);
      Map<Integer, FieldStats<?>> rightStats = statsById(right);

      Set<Integer> fieldIds =
          Sets.newTreeSet(Iterables.concat(leftStats.keySet(), rightStats.keySet()));
      for (Integer fieldId : fieldIds) {
        FieldStats<?> leftField = leftStats.get(fieldId);
        FieldStats<?> rightField = rightStats.get(fieldId);
        Comparator<FieldStats<?>> fieldComparator = fieldStatsComparator(leftField);
        int cmp = fieldComparator.compare(leftField, rightField);
        if (cmp != 0) {
          return cmp;
        }
      }

      return 0;
    }
  }

  private static Map<Integer, FieldStats<?>> statsById(ContentStats stats) {
    Map<Integer, FieldStats<?>> statsById = Maps.newHashMap();
    for (FieldStats<?> fieldStats : stats.fieldStats()) {
      statsById.put(fieldStats.fieldId(), fieldStats);
    }

    return statsById;
  }

  private static Comparator<FieldStats<?>> fieldStatsComparator(FieldStats<?> stats) {
    Type.PrimitiveType type = type(stats);
    Comparator<FieldStats<?>> comparator =
        Comparator.<FieldStats<?>, Long>comparing(
                fs -> fs.hasValueCount() ? fs.valueCount() : null, natural())
            .thenComparing(fs -> fs.hasNullValueCount() ? fs.nullValueCount() : null, natural());

    if (type != null) {
      comparator =
          comparator
              .thenComparing(FieldStats::lowerBound, comparator(type))
              .thenComparing(FieldStats::upperBound, comparator(type))
              .thenComparing(FieldStats::tightBounds);

      if (type.typeId() == Type.TypeID.FLOAT || type.typeId() == Type.TypeID.DOUBLE) {
        comparator =
            comparator.thenComparing(
                fs -> fs.hasNanValueCount() ? fs.nanValueCount() : null, natural());
      } else if (type.typeId() == Type.TypeID.STRING || type.typeId() == Type.TypeID.BINARY) {
        comparator = comparator.thenComparing(FieldStats::avgValueSizeInBytes, natural());
      }
    }

    return comparator;
  }

  private static Type.PrimitiveType type(FieldStats<?> stats) {
    Type fieldType = stats.type().fieldType(StatsUtil.LOWER_BOUND_NAME);
    return fieldType != null ? fieldType.asPrimitiveType() : null;
  }

  private static Comparator<Object> comparator(Type.PrimitiveType type) {
    return type != null
        ? Comparator.nullsFirst(Comparators.forType(type))
        : Comparators.nullsFirst();
  }
}
