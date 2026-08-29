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
package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.AggregateEvaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Computes Flink {@link TableStats} from Iceberg metadata only (snapshot summary and manifests).
 *
 * <p>Manifests are read directly instead of through a table scan so that computing statistics
 * during query planning produces no observable side effects: no {@code ScanEvent} is fired and no
 * scan-planning metrics are reported. The same pruning as scan planning is applied via {@link
 * ManifestEvaluator} at the manifest level and the manifest reader's row filter at the file level.
 *
 * <p>Reported values are planner estimates: when delete files are present, row counts and null
 * counts are overestimates because manifest metrics describe files as written.
 *
 * <p>Min/max values for DATE, TIME, TIMESTAMP and TIMESTAMP_NANO columns are reported as Iceberg's
 * numeric internal representations (days/micros/nanos since epoch) — monotonic, {@link Number}
 * -typed values that are safe for the planner's interval arithmetic. String and binary columns
 * never report min/max because manifest bounds may be truncated.
 */
class FlinkTableStatistics {

  private static final Set<Type.TypeID> MIN_MAX_TYPES =
      EnumSet.of(
          Type.TypeID.BOOLEAN,
          Type.TypeID.INTEGER,
          Type.TypeID.LONG,
          Type.TypeID.FLOAT,
          Type.TypeID.DOUBLE,
          Type.TypeID.DATE,
          Type.TypeID.TIME,
          Type.TypeID.TIMESTAMP,
          Type.TypeID.TIMESTAMP_NANO,
          Type.TypeID.DECIMAL);

  private FlinkTableStatistics() {}

  static TableStats reportStatistics(
      Table table, List<Expression> filters, boolean columnStatsEnabled) {
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null) {
      return new TableStats(0L);
    }

    boolean filtered = filters != null && !filters.isEmpty();
    if (!filtered && !columnStatsEnabled) {
      long totalRecords =
          PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.TOTAL_RECORDS_PROP, -1L);
      if (totalRecords >= 0) {
        return new TableStats(totalRecords);
      }
      // very old writers may not have written total-records; fall through to the manifest path
    }

    return statsFromManifests(table, snapshot, filters, filtered, columnStatsEnabled);
  }

  private static TableStats statsFromManifests(
      Table table,
      Snapshot snapshot,
      List<Expression> filters,
      boolean filtered,
      boolean columnStatsEnabled) {
    Expression rowFilter =
        filtered
            ? filters.stream().reduce(Expressions.alwaysTrue(), Expressions::and)
            : Expressions.alwaysTrue();
    List<ColumnStatsCollector> collectors =
        columnStatsEnabled ? createCollectors(table.schema()) : ImmutableList.of();

    long rowCount = 0L;
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      if (filtered
          && !ManifestEvaluator.forRowFilter(
                  rowFilter, table.specs().get(manifest.partitionSpecId()), true)
              .eval(manifest)) {
        continue;
      }

      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs()).filterRows(rowFilter)) {
        for (DataFile file : reader) {
          long recordCount = file.recordCount();
          if (recordCount <= 0) {
            return TableStats.UNKNOWN;
          }

          try {
            rowCount = Math.addExact(rowCount, recordCount);
          } catch (ArithmeticException e) {
            return TableStats.UNKNOWN;
          }

          for (ColumnStatsCollector collector : collectors) {
            collector.update(file);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read manifest for statistics", e);
      }
    }

    return new TableStats(rowCount, buildColumnStats(table, collectors));
  }

  private static Map<String, ColumnStats> buildColumnStats(
      Table table, List<ColumnStatsCollector> collectors) {
    if (collectors.isEmpty()) {
      return ImmutableMap.of();
    }

    Map<Integer, Long> ndvs = ndvFromStatisticsFiles(table, table.currentSnapshot().snapshotId());
    Map<String, ColumnStats> colStats = Maps.newHashMap();
    for (ColumnStatsCollector collector : collectors) {
      ColumnStats columnStats = collector.toColumnStats(ndvs.get(collector.fieldId()));
      if (columnStats != null) {
        colStats.put(collector.columnName(), columnStats);
      }
    }

    return colStats;
  }

  private static Map<Integer, Long> ndvFromStatisticsFiles(Table table, long snapshotId) {
    Map<Integer, Long> ndvs = Maps.newHashMap();
    for (StatisticsFile statisticsFile : table.statisticsFiles()) {
      if (statisticsFile.snapshotId() != snapshotId) {
        continue;
      }

      for (BlobMetadata blob : statisticsFile.blobMetadata()) {
        if (StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equals(blob.type())
            && blob.fields().size() == 1) {
          String ndv = blob.properties().get("ndv");
          if (!Strings.isNullOrEmpty(ndv)) {
            ndvs.put(blob.fields().get(0), Long.parseLong(ndv));
          }
        }
      }
    }

    return ndvs;
  }

  private static List<ColumnStatsCollector> createCollectors(Schema schema) {
    List<ColumnStatsCollector> collectors = Lists.newArrayList();
    for (Types.NestedField field : schema.columns()) {
      if (field.type().isPrimitiveType()) {
        collectors.add(new ColumnStatsCollector(schema, field));
      }
    }

    return collectors;
  }

  /** Folds per-file manifest metrics for one top-level column. */
  private static class ColumnStatsCollector {
    private final Types.NestedField field;
    private final AggregateEvaluator minMaxEvaluator; // null when the type is not eligible
    private long nullCount = 0L;
    private boolean nullCountValid = true;

    ColumnStatsCollector(Schema schema, Types.NestedField field) {
      this.field = field;
      this.minMaxEvaluator =
          MIN_MAX_TYPES.contains(field.type().typeId())
              ? AggregateEvaluator.create(
                  schema,
                  ImmutableList.of(Expressions.min(field.name()), Expressions.max(field.name())))
              : null;
    }

    String columnName() {
      return field.name();
    }

    int fieldId() {
      return field.fieldId();
    }

    void update(DataFile file) {
      Map<Integer, Long> nullCounts = file.nullValueCounts();
      Long fileNullCount = nullCounts == null ? null : nullCounts.get(field.fieldId());
      if (fileNullCount == null) {
        this.nullCountValid = false;
      } else {
        this.nullCount += fileNullCount;
      }

      if (minMaxEvaluator != null) {
        minMaxEvaluator.update(file);
      }
    }

    ColumnStats toColumnStats(Long ndv) {
      ColumnStats.Builder builder = ColumnStats.Builder.builder();
      boolean hasAnyStat = false;
      if (ndv != null) {
        builder.setNdv(ndv);
        hasAnyStat = true;
      }

      if (nullCountValid) {
        builder.setNullCount(nullCount);
        hasAnyStat = true;
      }

      if (minMaxEvaluator != null && minMaxEvaluator.allAggregatorsValid()) {
        StructLike result = minMaxEvaluator.result();
        Object min = result.get(0, Object.class);
        Object max = result.get(1, Object.class);
        if (min instanceof Comparable && max instanceof Comparable) {
          builder.setMin((Comparable<?>) min);
          builder.setMax((Comparable<?>) max);
          hasAnyStat = true;
        }
      }

      return hasAnyStat ? builder.build() : null;
    }
  }
}
