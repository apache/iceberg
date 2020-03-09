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

package org.apache.iceberg.flink.connector.sink;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;

public final class FileWriter implements Serializable {
  private static final long serialVersionUID = 1L;

  private final FileFormat format;
  private final Path path;
  private final ProcessingTimeService timeService;
  private final long creationTime;
  private final Partitioner<IndexedRecord> partitioner;
  private final FileAppender<IndexedRecord> appender;
  private final org.apache.hadoop.conf.Configuration hadoopConfig;
  private final PartitionSpec spec;
  private final IcebergWriterTaskMetrics icebergWriterTaskMetrics;
  private final IcebergWriterSubtaskMetrics icebergWriterSubtaskMetrics;
  private final WatermarkTimeExtractor watermarkTimeExtractor;

  private long lastWrittenToTime;
  private long lowWatermark = Long.MAX_VALUE;
  private long highWatermark = Long.MIN_VALUE;

  @SuppressWarnings("checkstyle:HiddenField")
  public static class Builder<T> {
    private FileFormat format;
    private Path path;
    private ProcessingTimeService timeService;
    private Partitioner<T> partitioner;
    private FileAppender<T> appender;
    private org.apache.hadoop.conf.Configuration hadoopConfig;
    private PartitionSpec spec;
    private IcebergWriterTaskMetrics icebergWriterTaskMetrics;
    private IcebergWriterSubtaskMetrics icebergWriterSubtaskMetrics;
    private Schema avroSchema;
    private String vttsTimestampField;
    private TimeUnit vttsTimestampUnit;

    private Builder() {
    }

    public Builder withFileFormat(final FileFormat format) {
      this.format = format;
      return this;
    }

    public Builder withPath(final Path path) {
      this.path = path;
      return this;
    }

    public Builder withProcessingTimeService(final ProcessingTimeService timeService) {
      this.timeService = timeService;
      return this;
    }

    public Builder withPartitioner(final Partitioner<T> partitioner) {
      this.partitioner = partitioner;
      return this;
    }

    public Builder withAppender(final FileAppender appender) {
      this.appender = appender;
      return this;
    }

    public Builder withHadooopConfig(final org.apache.hadoop.conf.Configuration hadoopConfig) {
      this.hadoopConfig = hadoopConfig;
      return this;
    }

    public Builder withSpec(final PartitionSpec spec) {
      this.spec = spec;
      return this;
    }

    public Builder withMetrics(final IcebergWriterTaskMetrics icebergWriterTaskMetrics) {
      this.icebergWriterTaskMetrics = icebergWriterTaskMetrics;
      return this;
    }

    public Builder withSubtaskMetrics(final IcebergWriterSubtaskMetrics icebergWriterSubtaskMetrics) {
      this.icebergWriterSubtaskMetrics = icebergWriterSubtaskMetrics;
      return this;
    }

    public Builder withSchema(final Schema avroSchema) {
      this.avroSchema = avroSchema;
      return this;
    }

    public Builder withVttsTimestampField(final String vttsTimestampField) {
      this.vttsTimestampField = vttsTimestampField;
      return this;
    }

    public Builder withVttsTimestampUnit(final TimeUnit vttsTimestampUnit) {
      this.vttsTimestampUnit = vttsTimestampUnit;
      return this;
    }

    public FileWriter build() {
      Preconditions.checkArgument(this.format != null, "File format is required");
      Preconditions.checkArgument(this.path != null, "File path is required");
      Preconditions.checkArgument(this.timeService != null, "ProcessingTimeService is required");
      Preconditions.checkArgument(this.partitioner != null, "Partitioner is required");
      Preconditions.checkArgument(this.appender != null, "File appender is required");
      Preconditions.checkArgument(this.hadoopConfig != null, "Hadoop config is required");
      Preconditions.checkArgument(this.spec != null, "Partition spec is required");
      Preconditions.checkArgument(this.icebergWriterTaskMetrics != null, "icebergWriterTaskMetrics is required");
      Preconditions.checkArgument(this.avroSchema != null, "avroSchema is required");
      Preconditions.checkArgument(this.vttsTimestampField != null, "vttsTimestampField is required");
      Preconditions.checkArgument(this.vttsTimestampUnit != null, "vttsTimestampUnit is required");
      return new FileWriter(this);
    }
  }

  private FileWriter(Builder builder) {
    format = builder.format;
    path = builder.path;
    timeService = builder.timeService;
    creationTime = timeService.getCurrentProcessingTime();
    partitioner = builder.partitioner;
    appender = builder.appender;
    hadoopConfig = builder.hadoopConfig;
    spec = builder.spec;
    icebergWriterTaskMetrics = builder.icebergWriterTaskMetrics;
    icebergWriterSubtaskMetrics = builder.icebergWriterSubtaskMetrics;
    watermarkTimeExtractor = new WatermarkTimeExtractor(
        builder.avroSchema, builder.vttsTimestampField, builder.vttsTimestampUnit);
  }

  public static Builder builder() {
    return new Builder();
  }

  public long write(IndexedRecord record) {
    // We want to distinguish (1) IOException v.s. (2)type/schema exception
    // For type exception, we want to page table owner.
    try {
      appender.add(record);
    } catch (RuntimeIOException e) {
      // 1. IOException (file or S3 write failure)
      icebergWriterSubtaskMetrics.incrementIcebergAppendRecordIOFailures();
      throw e;
    } catch (Exception e) {
      // 2. schema/type mismatch
      // Ideally, we would like Iceberg/Parquet throw a unified exception
      // (e.g. IcebergTypeException) for type/schema error.
      // Unfortunately, that is not the case and not an simple change.
      // For now, we just assume all non-schema errors are
      // RuntimeIOException based on discussion with Ryan.
      icebergWriterSubtaskMetrics.incrementIcebergAppendRecordTypeFailures();
      throw e;
    }
    lastWrittenToTime = timeService.getCurrentProcessingTime();
    Long timeMs = watermarkTimeExtractor.getWatermarkTimeMs(record);
    if (null != timeMs) {
      if (timeMs < lowWatermark) {
        lowWatermark = timeMs;
      }
      if (timeMs > highWatermark) {
        highWatermark = timeMs;
      }
    }
    return appender.length();
  }

  public FlinkDataFile close() throws IOException {
    final long start = icebergWriterTaskMetrics.getRegistry().clock().monotonicTime();
    try {
      appender.close();
    } finally {
      final long end = icebergWriterTaskMetrics.getRegistry().clock().monotonicTime();
      icebergWriterTaskMetrics.recordS3UploadLatency(end - start, TimeUnit.NANOSECONDS);
    }

    // metrics are only valid after the appender is closed
    Metrics metrics = appender.metrics();
    InputFile inputFile = HadoopInputFile.fromPath(path, hadoopConfig);
    DataFile dataFile = DataFiles.builder(spec)
        .withFormat(format)
        .withInputFile(inputFile)
        .withPartition(partitioner)
        .withMetrics(metrics)
        .build();
    return new FlinkDataFile(lowWatermark, highWatermark, dataFile);
  }

  public Path abort() throws IOException {
    // TODO: need an abort API from Iceberg
    appender.close();
    return path;
  }

  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return String.format("path @ %s, created @ %d, last written @ %d, " +
            "low watermark @ %d, high watermark @ %d",
        path.toString(), creationTime, lastWrittenToTime, lowWatermark, highWatermark);
  }
}
