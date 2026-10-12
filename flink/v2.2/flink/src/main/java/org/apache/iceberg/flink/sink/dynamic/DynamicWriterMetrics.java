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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.Arrays;
import java.util.Map;
import java.util.function.ToLongFunction;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.flink.sink.IcebergStreamWriterMetrics;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ScanTaskUtil;

class DynamicWriterMetrics {

  private final Map<String, IcebergStreamWriterMetrics> metrics;
  private final SinkWriterMetricGroup mainMetricsGroup;

  DynamicWriterMetrics(SinkWriterMetricGroup mainMetricsGroup) {
    this.mainMetricsGroup = mainMetricsGroup;
    this.metrics = Maps.newHashMap();
  }

  SinkWriterMetricGroup mainMetricsGroup() {
    return this.mainMetricsGroup;
  }

  public void updateFlushResult(String fullTableName, WriteResult result) {
    writerMetrics(fullTableName).updateFlushResult(result);

    long bytesOutTotal = sum(result.dataFiles()) + sum(result.deleteFiles());
    this.mainMetricsGroup.getNumBytesSendCounter().inc(bytesOutTotal);
  }

  public void flushDuration(String fullTableName, long flushDurationMs) {
    writerMetrics(fullTableName).flushDuration(flushDurationMs);
  }

  IcebergStreamWriterMetrics writerMetrics(String fullTableName) {
    return metrics.computeIfAbsent(
        fullTableName, tableName -> new IcebergStreamWriterMetrics(mainMetricsGroup, tableName));
  }

  private static long sum(DataFile[] files) {
    return sum(files, DataFile::fileSizeInBytes);
  }

  private static long sum(DeleteFile[] files) {
    return sum(files, ScanTaskUtil::contentSizeInBytes);
  }

  private static <T extends ContentFile<T>> long sum(T[] files, ToLongFunction<T> sizeExtractor) {
    return Arrays.stream(files).mapToLong(sizeExtractor).sum();
  }
}
