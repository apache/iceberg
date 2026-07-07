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
package org.apache.iceberg.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

/**
 * OpenTelemetry instruments for {@link CommitReport} metrics, keyed by {@link CommitMetricsResult}.
 */
final class CommitInstruments {
  private final DoubleHistogram duration;
  private final LongCounter attempts;
  private final LongCounter addedDataFiles;
  private final LongCounter removedDataFiles;
  private final LongCounter totalDataFiles;
  private final LongCounter addedDeleteFiles;
  private final LongCounter addedEqualityDeleteFiles;
  private final LongCounter addedPositionalDeleteFiles;
  private final LongCounter addedDVs;
  private final LongCounter removedDeleteFiles;
  private final LongCounter removedEqualityDeleteFiles;
  private final LongCounter removedPositionalDeleteFiles;
  private final LongCounter removedDVs;
  private final LongCounter totalDeleteFiles;
  private final LongCounter addedRecords;
  private final LongCounter removedRecords;
  private final LongCounter totalRecords;
  private final LongCounter addedFileSizeBytes;
  private final LongCounter removedFileSizeBytes;
  private final LongCounter totalFileSizeBytes;
  private final LongCounter addedPositionalDeletes;
  private final LongCounter removedPositionalDeletes;
  private final LongCounter totalPositionalDeletes;
  private final LongCounter addedEqualityDeletes;
  private final LongCounter removedEqualityDeletes;
  private final LongCounter totalEqualityDeletes;
  private final LongCounter manifestsCreated;
  private final LongCounter manifestsReplaced;
  private final LongCounter manifestsKept;
  private final LongCounter manifestEntriesProcessed;

  private CommitInstruments(Meter meter) {
    duration = histogram(meter, "iceberg.commit.duration", "Time spent on commit operation", "ms");
    attempts = counter(meter, "iceberg.commit.attempts", "Number of commit attempts");
    addedDataFiles =
        counter(meter, "iceberg.commit.data_files.added", "Number of data files added by commit");
    removedDataFiles =
        counter(
            meter, "iceberg.commit.data_files.removed", "Number of data files removed by commit");
    totalDataFiles =
        counter(
            meter, "iceberg.commit.data_files.total", "Total number of data files after commit");
    addedDeleteFiles =
        counter(
            meter, "iceberg.commit.delete_files.added", "Number of delete files added by commit");
    addedEqualityDeleteFiles =
        counter(
            meter,
            "iceberg.commit.delete_files.equality.added",
            "Number of equality delete files added by commit");
    addedPositionalDeleteFiles =
        counter(
            meter,
            "iceberg.commit.delete_files.positional.added",
            "Number of positional delete files added by commit");
    addedDVs =
        counter(meter, "iceberg.commit.dvs.added", "Number of deletion vectors added by commit");
    removedDeleteFiles =
        counter(
            meter,
            "iceberg.commit.delete_files.removed",
            "Number of delete files removed by commit");
    removedEqualityDeleteFiles =
        counter(
            meter,
            "iceberg.commit.delete_files.equality.removed",
            "Number of equality delete files removed by commit");
    removedPositionalDeleteFiles =
        counter(
            meter,
            "iceberg.commit.delete_files.positional.removed",
            "Number of positional delete files removed by commit");
    removedDVs =
        counter(
            meter, "iceberg.commit.dvs.removed", "Number of deletion vectors removed by commit");
    totalDeleteFiles =
        counter(
            meter,
            "iceberg.commit.delete_files.total",
            "Total number of delete files after commit");
    addedRecords =
        counter(meter, "iceberg.commit.records.added", "Number of records added by commit");
    removedRecords =
        counter(meter, "iceberg.commit.records.removed", "Number of records removed by commit");
    totalRecords =
        counter(meter, "iceberg.commit.records.total", "Total number of records after commit");
    addedFileSizeBytes =
        counter(
            meter,
            "iceberg.commit.file_size.added_bytes",
            "Total size of data files added by commit",
            "By");
    removedFileSizeBytes =
        counter(
            meter,
            "iceberg.commit.file_size.removed_bytes",
            "Total size of data files removed by commit",
            "By");
    totalFileSizeBytes =
        counter(
            meter,
            "iceberg.commit.file_size.total_bytes",
            "Total size of all data files after commit",
            "By");
    addedPositionalDeletes =
        counter(
            meter,
            "iceberg.commit.positional_deletes.added",
            "Number of positional deletes added by commit");
    removedPositionalDeletes =
        counter(
            meter,
            "iceberg.commit.positional_deletes.removed",
            "Number of positional deletes removed by commit");
    totalPositionalDeletes =
        counter(
            meter,
            "iceberg.commit.positional_deletes.total",
            "Total number of positional deletes after commit");
    addedEqualityDeletes =
        counter(
            meter,
            "iceberg.commit.equality_deletes.added",
            "Number of equality deletes added by commit");
    removedEqualityDeletes =
        counter(
            meter,
            "iceberg.commit.equality_deletes.removed",
            "Number of equality deletes removed by commit");
    totalEqualityDeletes =
        counter(
            meter,
            "iceberg.commit.equality_deletes.total",
            "Total number of equality deletes after commit");
    manifestsCreated =
        counter(meter, "iceberg.commit.manifests.created", "Number of manifests created by commit");
    manifestsReplaced =
        counter(
            meter, "iceberg.commit.manifests.replaced", "Number of manifests replaced by commit");
    manifestsKept =
        counter(meter, "iceberg.commit.manifests.kept", "Number of manifests kept by commit");
    manifestEntriesProcessed =
        counter(
            meter,
            "iceberg.commit.manifest_entries.processed",
            "Number of manifest entries processed by commit");
  }

  static CommitInstruments create(Meter meter) {
    return new CommitInstruments(meter);
  }

  void record(CommitMetricsResult metrics, Attributes attrs) {
    if (metrics.totalDuration() != null) {
      duration.record((double) metrics.totalDuration().totalDuration().toMillis(), attrs);
    }

    recordCounter(attempts, metrics.attempts(), attrs);
    recordCounter(addedDataFiles, metrics.addedDataFiles(), attrs);
    recordCounter(removedDataFiles, metrics.removedDataFiles(), attrs);
    recordCounter(totalDataFiles, metrics.totalDataFiles(), attrs);
    recordCounter(addedDeleteFiles, metrics.addedDeleteFiles(), attrs);
    recordCounter(addedEqualityDeleteFiles, metrics.addedEqualityDeleteFiles(), attrs);
    recordCounter(addedPositionalDeleteFiles, metrics.addedPositionalDeleteFiles(), attrs);
    recordCounter(addedDVs, metrics.addedDVs(), attrs);
    recordCounter(removedDeleteFiles, metrics.removedDeleteFiles(), attrs);
    recordCounter(removedEqualityDeleteFiles, metrics.removedEqualityDeleteFiles(), attrs);
    recordCounter(removedPositionalDeleteFiles, metrics.removedPositionalDeleteFiles(), attrs);
    recordCounter(removedDVs, metrics.removedDVs(), attrs);
    recordCounter(totalDeleteFiles, metrics.totalDeleteFiles(), attrs);
    recordCounter(addedRecords, metrics.addedRecords(), attrs);
    recordCounter(removedRecords, metrics.removedRecords(), attrs);
    recordCounter(totalRecords, metrics.totalRecords(), attrs);
    recordCounter(addedFileSizeBytes, metrics.addedFilesSizeInBytes(), attrs);
    recordCounter(removedFileSizeBytes, metrics.removedFilesSizeInBytes(), attrs);
    recordCounter(totalFileSizeBytes, metrics.totalFilesSizeInBytes(), attrs);
    recordCounter(addedPositionalDeletes, metrics.addedPositionalDeletes(), attrs);
    recordCounter(removedPositionalDeletes, metrics.removedPositionalDeletes(), attrs);
    recordCounter(totalPositionalDeletes, metrics.totalPositionalDeletes(), attrs);
    recordCounter(addedEqualityDeletes, metrics.addedEqualityDeletes(), attrs);
    recordCounter(removedEqualityDeletes, metrics.removedEqualityDeletes(), attrs);
    recordCounter(totalEqualityDeletes, metrics.totalEqualityDeletes(), attrs);
    recordCounter(manifestsCreated, metrics.manifestsCreated(), attrs);
    recordCounter(manifestsReplaced, metrics.manifestsReplaced(), attrs);
    recordCounter(manifestsKept, metrics.manifestsKept(), attrs);
    recordCounter(manifestEntriesProcessed, metrics.manifestEntriesProcessed(), attrs);
  }

  private static LongCounter counter(Meter meter, String name, String description) {
    return meter.counterBuilder(name).setDescription(description).build();
  }

  private static LongCounter counter(Meter meter, String name, String description, String unit) {
    return meter.counterBuilder(name).setDescription(description).setUnit(unit).build();
  }

  private static DoubleHistogram histogram(
      Meter meter, String name, String description, String unit) {
    return meter.histogramBuilder(name).setDescription(description).setUnit(unit).build();
  }

  private static void recordCounter(LongCounter counter, CounterResult result, Attributes attrs) {
    if (result != null) {
      counter.add(result.value(), attrs);
    }
  }
}
