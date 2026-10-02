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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;

/**
 * Reads the statistics of data and delete files and compares them against the statistics recorded
 * in manifest entries.
 *
 * <p>Recomputed statistics always respect the metrics config of the table so that they are
 * comparable with the stored statistics.
 */
class RepairMetrics {

  private static final Set<Integer> POSITION_DELETE_FIELD_IDS =
      ImmutableSet.of(
          MetadataColumns.DELETE_FILE_PATH.fieldId(), MetadataColumns.DELETE_FILE_POS.fieldId());

  private RepairMetrics() {}

  /** Returns the name mapping of the table, or null if the table does not define one. */
  static NameMapping nameMapping(Table table) {
    String mapping = table.properties().get(DEFAULT_NAME_MAPPING);
    return mapping != null ? NameMappingParser.fromJson(mapping) : null;
  }

  /**
   * Returns the metrics config to use when recomputing the statistics of the given file.
   *
   * <p>Position delete files record statistics for the path and position columns only, which is a
   * fixed config rather than the config of the table.
   */
  static MetricsConfig metricsConfig(Table table, FileContent content) {
    return content == FileContent.POSITION_DELETES
        ? MetricsConfig.forPositionDelete()
        : MetricsConfig.forTable(table);
  }

  /**
   * Returns true if the statistics of the file can be recomputed by reading it.
   *
   * <p>Deletion vectors are stored as blobs inside a Puffin file, so their statistics cannot be
   * derived by reading the file they are stored in.
   */
  static boolean supportsMetrics(ContentFile<?> file) {
    FileFormat format = file.format();
    return format == FileFormat.PARQUET || format == FileFormat.ORC || format == FileFormat.AVRO;
  }

  /** Recomputes recoverable statistics, preserving metrics that require writer-side tracking. */
  static Metrics readMetrics(
      InputFile input,
      ContentFile<?> file,
      MetricsConfig config,
      NameMapping mapping,
      Schema schema) {
    Metrics metrics =
        switch (file.format()) {
          case PARQUET -> ParquetUtil.fileMetrics(input, config, mapping);
          case ORC -> OrcMetrics.fromInputFile(input, config, mapping);
          case AVRO -> new Metrics(Avro.rowCount(input));
          default ->
              throw new UnsupportedOperationException(
                  "Cannot read metrics of format: " + file.format());
        };
    if (file.format() == FileFormat.AVRO) {
      return recordCountOnly(file, metrics);
    }

    if (file.content() == FileContent.POSITION_DELETES) {
      // Match PositionDeleteWriter: counts are omitted, and bounds are kept only for a single path.
      int pathId = MetadataColumns.DELETE_FILE_PATH.fieldId();
      ByteBuffer lowerPath = normalize(metrics.lowerBounds()).get(pathId);
      ByteBuffer upperPath = normalize(metrics.upperBounds()).get(pathId);
      metrics =
          lowerPath != null && lowerPath.equals(upperPath)
              ? MetricsUtil.copyWithoutFieldCounts(metrics, POSITION_DELETE_FIELD_IDS)
              : MetricsUtil.copyWithoutFieldCountsAndBounds(metrics, POSITION_DELETE_FIELD_IDS);
    }

    // Footers cannot recover the writer-tracked statistics of some columns: the NaN counts and
    // NaN-safe bounds of floating-point columns, and the bounding boxes of geometry and geography
    // columns. Preserve the stored bounds of those columns so a correct file is not flagged as
    // corrupt and rewritten with weaker statistics. Include stored NaN-count IDs so that bounds for
    // dropped columns are preserved as well.
    Set<Integer> preservedBoundIds = Sets.newHashSet(normalize(file.nanValueCounts()).keySet());
    TypeUtil.indexById(schema.asStruct())
        .forEach(
            (id, field) -> {
              Type.TypeID typeId = field.type().typeId();
              if (typeId == Type.TypeID.FLOAT
                  || typeId == Type.TypeID.DOUBLE
                  || typeId == Type.TypeID.GEOMETRY
                  || typeId == Type.TypeID.GEOGRAPHY) {
                preservedBoundIds.add(id);
              }
            });

    return new Metrics(
        metrics.recordCount(),
        metrics.columnSizes(),
        metrics.valueCounts(),
        metrics.nullValueCounts(),
        file.nanValueCounts(),
        preserveBounds(file.lowerBounds(), metrics.lowerBounds(), preservedBoundIds),
        preserveBounds(file.upperBounds(), metrics.upperBounds(), preservedBoundIds),
        file.avgValueSizes(),
        null);
  }

  /**
   * Returns true if the statistics recorded for the file differ from the statistics of the file
   * itself.
   *
   * <p>The record count and the file size are always compared. Column level statistics are only
   * compared when requested, because a table whose metrics config changed after a file was written
   * reports statistics that legitimately differ from the recomputed ones.
   */
  static boolean statsAreIncorrect(
      ContentFile<?> file, Metrics metrics, long fileSizeInBytes, boolean compareColumnMetrics) {
    if (file.fileSizeInBytes() != fileSizeInBytes) {
      return true;
    }

    if (metrics.recordCount() != null && file.recordCount() != metrics.recordCount()) {
      return true;
    }

    if (!compareColumnMetrics) {
      return false;
    }

    return !countsMatch(file.columnSizes(), metrics.columnSizes())
        || !countsMatch(file.valueCounts(), metrics.valueCounts())
        || !countsMatch(file.nullValueCounts(), metrics.nullValueCounts())
        || !boundsMatch(file.lowerBounds(), metrics.lowerBounds())
        || !boundsMatch(file.upperBounds(), metrics.upperBounds());
  }

  /**
   * Rebuilds the file with the given statistics, preserving every other field.
   *
   * <p>The file size is passed separately as it is not part of the metrics of a file.
   */
  static ContentFile<?> withStats(
      ContentFile<?> file, PartitionSpec spec, Metrics metrics, long fileSizeInBytes) {
    if (file.content() == FileContent.DATA) {
      return DataFiles.builder(spec)
          .copy((DataFile) file)
          .withMetrics(metrics)
          .withFileSizeInBytes(fileSizeInBytes)
          .build();
    }

    DeleteFile delete = (DeleteFile) file;
    FileMetadata.Builder builder =
        FileMetadata.deleteFileBuilder(spec)
            .copy(delete)
            .withMetrics(metrics)
            .withFileSizeInBytes(fileSizeInBytes);
    List<Integer> equalityFieldIds = delete.equalityFieldIds();
    if (delete.content() == FileContent.EQUALITY_DELETES
        && equalityFieldIds != null
        && !equalityFieldIds.isEmpty()) {
      // copy(DeleteFile) drops the equality field ids, so they must be set again. Otherwise the
      // rewritten entry keeps content EQUALITY_DELETES with null equality ids, which makes reads
      // fail once the delete is applied. An entry that already lacks them is carried through as is.
      builder.ofEqualityDeletes(equalityFieldIds.stream().mapToInt(Integer::intValue).toArray());
    }

    return builder.build();
  }

  /**
   * Returns metrics carrying the recomputed record count but the column-level statistics stored for
   * the file, used when column metrics are not being repaired so that a flagged entry has only its
   * record count and file size corrected.
   */
  static Metrics recordCountOnly(ContentFile<?> file, Metrics recomputed) {
    return new Metrics(
        recomputed.recordCount(),
        file.columnSizes(),
        file.valueCounts(),
        file.nullValueCounts(),
        file.nanValueCounts(),
        file.lowerBounds(),
        file.upperBounds(),
        file.avgValueSizes(),
        null);
  }

  private static Map<Integer, ByteBuffer> preserveBounds(
      Map<Integer, ByteBuffer> stored, Map<Integer, ByteBuffer> actual, Set<Integer> ids) {
    Map<Integer, ByteBuffer> bounds = Maps.newHashMap(normalize(actual));
    for (int id : ids) {
      if (stored != null && stored.containsKey(id)) {
        bounds.put(id, stored.get(id));
      } else {
        bounds.remove(id);
      }
    }

    return bounds;
  }

  private static boolean countsMatch(Map<Integer, Long> stored, Map<Integer, Long> actual) {
    return Objects.equals(normalize(stored), normalize(actual));
  }

  private static boolean boundsMatch(
      Map<Integer, ByteBuffer> stored, Map<Integer, ByteBuffer> actual) {
    return Objects.equals(normalize(stored), normalize(actual));
  }

  /** Treats a missing map and an empty map as equivalent, as writers use both. */
  private static <T> Map<Integer, T> normalize(Map<Integer, T> map) {
    return map == null ? ImmutableMap.of() : map;
  }
}
