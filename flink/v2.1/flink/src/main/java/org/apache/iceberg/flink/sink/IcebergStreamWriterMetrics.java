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
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
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

  private final Counter flushedDataFiles;
  private final Counter flushedDeleteFiles;
  private final Counter flushedReferencedDataFiles;
  private final AtomicLong lastFlushDurationMs;
  private final Histogram dataFilesSizeHistogram;
  private final Histogram deleteFilesSizeHistogram;

  public IcebergStreamWriterMetrics(MetricGroup metrics, String fullTableName) {
    this(metrics, fullTableName, IcebergStreamWriterMetrics.class.getClassLoader());
  }

  @VisibleForTesting
  IcebergStreamWriterMetrics(MetricGroup metrics, String fullTableName, ClassLoader classLoader) {
    MetricGroup writerMetrics =
        metrics.addGroup("IcebergStreamWriter").addGroup("table", fullTableName);
    this.flushedDataFiles = writerMetrics.counter("flushedDataFiles");
    this.flushedDeleteFiles = writerMetrics.counter("flushedDeleteFiles");
    this.flushedReferencedDataFiles = writerMetrics.counter("flushedReferencedDataFiles");
    this.lastFlushDurationMs = new AtomicLong();
    writerMetrics.gauge("lastFlushDurationMs", lastFlushDurationMs::get);

    this.dataFilesSizeHistogram =
        loadHistogramIfAvailable(
            writerMetrics, "dataFilesSizeHistogram", HISTOGRAM_RESERVOIR_SIZE, classLoader);
    this.deleteFilesSizeHistogram =
        loadHistogramIfAvailable(
            writerMetrics, "deleteFilesSizeHistogram", HISTOGRAM_RESERVOIR_SIZE, classLoader);
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

  @Nullable
  Histogram dataFilesSizeHistogram() {
    return dataFilesSizeHistogram;
  }

  @Nullable
  Histogram deleteFilesSizeHistogram() {
    return deleteFilesSizeHistogram;
  }

  /**
   * Checks whether the Dropwizard-based histogram wrapper provided through Flink's optional
   * flink-metrics-dropwizard dependency is available.
   */
  @SuppressWarnings("CatchBlockLogException")
  static Histogram loadHistogramIfAvailable(
      MetricGroup group, String name, int reservoirSize, ClassLoader classLoader) {

    try {
      Class.forName(
          "org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper", false, classLoader);
      return HistogramLoader.load(name, group, reservoirSize);
    } catch (ClassNotFoundException e) {
      LOG.warn(
          "flink-metrics-dropwizard is not on the classpath. '{}' histogram metrics will be disabled. Add org.apache.flink:flink-metrics-dropwizard to enable them.",
          name);
      return null;
    }
  }

  /**
   * Must be encapsulated into a separate class to avoid the JVM eagerly loading any non-existing
   * referenced classes. This has been manually verified.
   */
  private static final class HistogramLoader {

    static Histogram load(String name, MetricGroup group, int reservoirSize) {
      com.codahale.metrics.Histogram codahaleHistogram =
          new com.codahale.metrics.Histogram(
              new com.codahale.metrics.SlidingWindowReservoir(reservoirSize));
      return group.histogram(name, new DropwizardHistogramWrapper(codahaleHistogram));
    }
  }
}
