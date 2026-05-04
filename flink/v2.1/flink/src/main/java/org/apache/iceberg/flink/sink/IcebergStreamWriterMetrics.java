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

  // Histogram metrics loaded through Flink's optional flink-metrics-dropwizard dependency
  private static final boolean DROPWIZARD_AVAILABLE;
  private static final DynConstructors.Ctor<?> RESERVOIR_CTOR;
  private static final DynConstructors.Ctor<?> CODAHALE_HISTOGRAM_CTOR;
  private static final DynConstructors.Ctor<Histogram> WRAPPER_CTOR;

  static {
    DynConstructors.Ctor<?> reservoirCtor = null;
    DynConstructors.Ctor<?> codahaleHistogramCtor = null;
    DynConstructors.Ctor<Histogram> wrapperCtor = null;
    boolean available = false;

    try {
      Class<?> reservoirInterface = Class.forName("com.codahale.metrics.Reservoir");
      Class<?> codahaleHistogramClass = Class.forName("com.codahale.metrics.Histogram");
      reservoirCtor =
          DynConstructors.builder()
              .impl("com.codahale.metrics.SlidingWindowReservoir", int.class)
              .buildChecked();
      codahaleHistogramCtor =
          DynConstructors.builder()
              .impl("com.codahale.metrics.Histogram", reservoirInterface)
              .buildChecked();
      wrapperCtor =
          DynConstructors.builder(Histogram.class)
              .impl(
                  "org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper",
                  codahaleHistogramClass)
              .buildChecked();
      available = true;
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      LOG.warn(
          "Cannot load Dropwizard metrics; is org.apache.flink:flink-metrics-dropwizard on the classpath?",
          e);
    }

    RESERVOIR_CTOR = reservoirCtor;
    CODAHALE_HISTOGRAM_CTOR = codahaleHistogramCtor;
    WRAPPER_CTOR = wrapperCtor;
    DROPWIZARD_AVAILABLE = available;
  }

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

    this.dataFilesSizeHistogram =
        loadHistogramIfAvailable(writerMetrics, "dataFilesSizeHistogram", HISTOGRAM_RESERVOIR_SIZE);
    this.deleteFilesSizeHistogram =
        loadHistogramIfAvailable(
            writerMetrics, "deleteFilesSizeHistogram", HISTOGRAM_RESERVOIR_SIZE);
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

  /**
   * Checks whether the Dropwizard-based histogram wrapper provided through Flink's optional
   * flink-metrics-dropwizard dependency is available.
   */
  private static Histogram loadHistogramIfAvailable(
      MetricGroup group, String name, int reservoirSize) {

    if (!DROPWIZARD_AVAILABLE) {
      return null;
    }

    Object reservoir = RESERVOIR_CTOR.newInstance(reservoirSize);
    Object codahaleHistogram = CODAHALE_HISTOGRAM_CTOR.newInstance(reservoir);
    Histogram wrapper = WRAPPER_CTOR.newInstance(codahaleHistogram);
    return group.histogram(name, wrapper);
  }
}
