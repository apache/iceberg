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
package org.apache.iceberg.parquet;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

class ParquetWriter<T> implements FileAppender<T>, Closeable {

  private static final Metrics EMPTY_METRICS = new Metrics(0L, null, null, null, null);

  private final long targetRowGroupSize;
  private final Map<String, String> metadata;
  private final ParquetProperties props;
  private final CodecFactory.BytesCompressor compressor;
  private final MessageType parquetSchema;
  private final ParquetValueWriter<T> model;
  private final MetricsConfig metricsConfig;
  private final int columnIndexTruncateLength;
  private final ParquetFileWriter.Mode writeMode;
  private final OutputFile output;
  private final Configuration conf;
  private final InternalFileEncryptor fileEncryptor;

  private ColumnChunkPageWriteStore pageStore = null;
  private ColumnWriteStore writeStore;
  private long recordCount = 0;
  private long nextCheckRecordCount = 10;
  private boolean closed;
  private ParquetFileWriter writer;
  private int rowGroupOrdinal;

  private static final String COLUMN_INDEX_TRUNCATE_LENGTH = "parquet.columnindex.truncate.length";
  private static final int DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH = 64;

  @SuppressWarnings("unchecked")
  ParquetWriter(
      Configuration conf,
      OutputFile output,
      Schema schema,
      long rowGroupSize,
      Map<String, String> metadata,
      Function<MessageType, ParquetValueWriter<?>> createWriterFunc,
      CompressionCodecName codec,
      ParquetProperties properties,
      MetricsConfig metricsConfig,
      ParquetFileWriter.Mode writeMode,
      FileEncryptionProperties encryptionProperties) {
    this.targetRowGroupSize = rowGroupSize;
    this.props = properties;
    this.metadata = ImmutableMap.copyOf(metadata);
    this.compressor =
        new ParquetCodecFactory(conf, props.getPageSizeThreshold()).getCompressor(codec);
    this.parquetSchema = ParquetSchemaUtil.convert(schema, "table");
    this.model = (ParquetValueWriter<T>) createWriterFunc.apply(parquetSchema);
    this.metricsConfig = metricsConfig;
    this.columnIndexTruncateLength =
        conf.getInt(COLUMN_INDEX_TRUNCATE_LENGTH, DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH);
    this.writeMode = writeMode;
    this.output = output;
    this.conf = conf;
    this.rowGroupOrdinal = 0;
    this.fileEncryptor =
        (encryptionProperties == null ? null : new InternalFileEncryptor(encryptionProperties));

    startRowGroup();
  }

  private void ensureWriterInitialized() {
    if (writer == null) {
      try {
        this.writer =
            new ParquetFileWriter(
                ParquetIO.file(output, conf),
                parquetSchema,
                writeMode,
                targetRowGroupSize,
                0,
                columnIndexTruncateLength,
                ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH,
                ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED,
                fileEncryptor);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to create Parquet file", e);
      }

      try {
        writer.start();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to start Parquet file writer", e);
      }
    }
  }

  @Override
  public void add(T value) {
    recordCount += 1;
    model.write(0, value);
    writeStore.endRecord();
    checkSize();
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkState(closed, "Cannot return metrics for unclosed writer");
    if (writer != null) {
      return ParquetUtil.footerMetrics(writer.getFooter(), model.metrics(), metricsConfig);
    }
    return EMPTY_METRICS;
  }

  /**
   * Returns the approximate length of the output file produced by this writer.
   *
   * <p>Prior to calling {@link ParquetWriter#close}, the result is approximate. After calling
   * close, the length is exact.
   *
   * @return the approximate length of the output file produced by this writer or the exact length
   *     if this writer is closed.
   */
  @Override
  public long length() {
    try {
      long length = 0L;

      if (writer != null) {
        length += writer.getPos();
      }

      if (!closed && recordCount > 0) {
        // recordCount > 0 when there are records in the write store that have not been flushed to
        // the Parquet file
        length += writeStore.getBufferedSize();
      }

      return length;

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to get file length", e);
    }
  }

  @Override
  public List<Long> splitOffsets() {
    if (writer != null) {
      return ParquetUtil.getSplitOffsets(writer.getFooter());
    }
    return null;
  }

  private void checkSize() {
    if (recordCount >= nextCheckRecordCount) {
      long bufferedSize = writeStore.getBufferedSize();
      double avgRecordSize = ((double) bufferedSize) / recordCount;

      if (bufferedSize > (targetRowGroupSize - 2 * avgRecordSize)) {
        flushRowGroup(false);
      } else {
        long remainingSpace = targetRowGroupSize - bufferedSize;
        long remainingRecords = (long) (remainingSpace / avgRecordSize);
        this.nextCheckRecordCount =
            recordCount
                + Math.min(
                    Math.max(remainingRecords / 2, props.getMinRowCountForPageSizeCheck()),
                    props.getMaxRowCountForPageSizeCheck());
      }
    }
  }

  private void flushRowGroup(boolean finished) {
    try {
      if (recordCount > 0) {
        ensureWriterInitialized();
        writer.startBlock(recordCount);
        writeStore.flush();
        pageStore.flushToFileWriter(writer);
        writer.endBlock();
        if (!finished) {
          writeStore.close();
          startRowGroup();
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to flush row group", e);
    }
  }

  private void startRowGroup() {
    Preconditions.checkState(!closed, "Writer is closed");

    this.nextCheckRecordCount =
        Math.min(
            Math.max(recordCount / 2, props.getMinRowCountForPageSizeCheck()),
            props.getMaxRowCountForPageSizeCheck());
    this.recordCount = 0;

    this.pageStore =
        new ColumnChunkPageWriteStore(
            compressor,
            parquetSchema,
            props.getAllocator(),
            this.columnIndexTruncateLength,
            ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED,
            fileEncryptor,
            rowGroupOrdinal);
    this.rowGroupOrdinal++;

    this.writeStore = props.newColumnWriteStore(parquetSchema, pageStore, pageStore);

    model.setColumnStore(writeStore);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      this.closed = true;
      flushRowGroup(true);
      writeStore.close();
      if (writer != null) {
        writer.end(metadata);
      }
      if (compressor != null) {
        compressor.release();
      }
    }
  }
}
