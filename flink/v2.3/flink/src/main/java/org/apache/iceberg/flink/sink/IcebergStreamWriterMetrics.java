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
package org.apache.iceberg.flink.sink;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.ScanTaskUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class IcebergStreamWriterMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergStreamWriterMetrics.class);

  // 1,024 reservoir size should cost about 8KB, which is quite small.
  // It should also produce good accuracy for histogram distribution (like percentiles).
  private static final int HISTOGRAM_RESERVOIR_SIZE = 1024;

  // Histogram metrics loaded through Flink's optional flink-metrics-dropwizard dependency.
  // Will be null if not available.
  private static final DropwizardCtors DROPWIZARD = loadDropwizardCtors();

  private final Counter flushedDataFiles;
  private final Counter flushedDeleteFiles;
  private final Counter flushedReferencedDataFiles;
  private final AtomicLong lastFlushDurationMs;
  private final Histogram dataFilesSizeHistogram;
  private final Histogram deleteFilesSizeHistogram;

  public IcebergStreamWriterMetrics(MetricGroup metrics, String fullTableName) {
    MetricGroup writerMetrics =
        metrics.addGroup("IcebergStreamWriter").addGroup("table", fullTableName);
    this.flushedDataFiles = writerMetrics.counter("flushedDataFiles");
    this.flushedDeleteFiles = writerMetrics.counter("flushedDeleteFiles");
    this.flushedReferencedDataFiles = writerMetrics.counter("flushedReferencedDataFiles");
    this.lastFlushDurationMs = new AtomicLong();
    writerMetrics.gauge("lastFlushDurationMs", lastFlushDurationMs::get);

    this.dataFilesSizeHistogram = registerHistogram(writerMetrics, "dataFilesSizeHistogram");
    this.deleteFilesSizeHistogram = registerHistogram(writerMetrics, "deleteFilesSizeHistogram");
  }

  public void updateFlushResult(WriteResult result) {
    flushedDataFiles.inc(result.dataFiles().length);
    flushedDeleteFiles.inc(result.deleteFiles().length);
    flushedReferencedDataFiles.inc(result.referencedDataFiles().length);

    // For file size distribution histogram, we don't have to update them after successful commits.
    // This should works equally well and we avoided the overhead of tracking the list of file sizes
    // in the {@link CommitSummary}, which currently stores simple stats for counters and gauges
    // metrics.
    if (dataFilesSizeHistogram != null) {
      Arrays.stream(result.dataFiles())
          .forEach(
              dataFile -> {
                dataFilesSizeHistogram.update(dataFile.fileSizeInBytes());
              });
    }

    if (deleteFilesSizeHistogram != null) {
      Arrays.stream(result.deleteFiles())
          .forEach(
              deleteFile -> {
                deleteFilesSizeHistogram.update(ScanTaskUtil.contentSizeInBytes(deleteFile));
              });
    }
  }

  public void flushDuration(long flushDurationMs) {
    lastFlushDurationMs.set(flushDurationMs);
  }

  public Counter getFlushedDataFiles() {
    return flushedDataFiles;
  }

  public Counter getFlushedDeleteFiles() {
    return flushedDeleteFiles;
  }

  @VisibleForTesting
  Histogram dataFilesSizeHistogram() {
    return dataFilesSizeHistogram;
  }

  @VisibleForTesting
  Histogram deleteFilesSizeHistogram() {
    return deleteFilesSizeHistogram;
  }

  private static Histogram registerHistogram(MetricGroup group, String name) {
    Histogram histogram = newDropwizardHistogram();
    return histogram != null ? group.histogram(name, histogram) : null;
  }

  private static Histogram newDropwizardHistogram() {
    if (DROPWIZARD == null) {
      return null;
    }

    Object reservoir = DROPWIZARD.reservoirCtor.newInstance(HISTOGRAM_RESERVOIR_SIZE);
    Object codahaleHistogram = DROPWIZARD.histogramCtor.newInstance(reservoir);
    return DROPWIZARD.wrapperCtor.newInstance(codahaleHistogram);
  }

  private static DropwizardCtors loadDropwizardCtors() {
    try {
      Class<?> reservoirInterface =
          DynClasses.builder().impl("com.codahale.metrics.Reservoir").buildChecked();
      Class<?> codahaleHistogramClass =
          DynClasses.builder().impl("com.codahale.metrics.Histogram").buildChecked();
      return new DropwizardCtors(
          DynConstructors.builder()
              .impl("com.codahale.metrics.SlidingWindowReservoir", int.class)
              .buildChecked(),
          DynConstructors.builder()
              .impl("com.codahale.metrics.Histogram", reservoirInterface)
              .buildChecked(),
          DynConstructors.builder(Histogram.class)
              .impl(
                  "org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper",
                  codahaleHistogramClass)
              .buildChecked());
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      LOG.warn(
          "Cannot load Dropwizard metrics; is org.apache.flink:flink-metrics-dropwizard on the classpath?",
          e);
      return null;
    }
  }

  private record DropwizardCtors(
      DynConstructors.Ctor<?> reservoirCtor,
      DynConstructors.Ctor<?> histogramCtor,
      DynConstructors.Ctor<Histogram> wrapperCtor) {}
}
