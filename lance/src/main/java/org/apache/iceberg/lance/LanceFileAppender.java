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
package org.apache.iceberg.lance;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * A {@link FileAppender} implementation that writes data in Lance format.
 *
 * <p>In the current implementation, this writes data as a JSON-like text format to the output file.
 * In a production implementation, this would use the Lance JNI bridge to write native Lance format
 * files with Arrow columnar encoding.
 *
 * @param <D> the type of data records to write (typically GenericRecord)
 */
public class LanceFileAppender<D> implements FileAppender<D> {
  private final OutputFile outputFile;
  private final Schema schema;
  private final LanceMetrics.MetricsCollector metricsCollector;
  private final List<D> bufferedRecords;
  private boolean closed = false;
  private long fileLength = 0;

  /**
   * Create a new LanceFileAppender.
   *
   * @param outputFile the output file to write to
   * @param schema the Iceberg schema for this file
   */
  public LanceFileAppender(OutputFile outputFile, Schema schema) {
    Preconditions.checkNotNull(outputFile, "Output file cannot be null");
    Preconditions.checkNotNull(schema, "Schema cannot be null");
    this.outputFile = outputFile;
    this.schema = schema;
    this.metricsCollector = new LanceMetrics.MetricsCollector();
    this.bufferedRecords = Lists.newArrayList();
  }

  @Override
  public void add(D datum) {
    Preconditions.checkState(!closed, "Cannot add to a closed appender");
    bufferedRecords.add(datum);
    metricsCollector.incrementRowCount();

    // Collect per-column metrics if the datum is a GenericRecord
    if (datum instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) datum;
      collectRecordMetrics(record);
    }
  }

  /** Collect per-column metrics from a GenericRecord. */
  private void collectRecordMetrics(GenericRecord record) {
    for (Types.NestedField field : schema.columns()) {
      int fieldId = field.fieldId();
      Object value = record.getField(field.name());

      metricsCollector.incrementValueCount(fieldId);

      if (value == null) {
        metricsCollector.incrementNullCount(fieldId);
      } else {
        // Estimate column size (simplified)
        metricsCollector.addColumnSize(fieldId, estimateSize(value));

        // Update bounds for primitive types
        if (field.type().isPrimitiveType() && value instanceof Comparable) {
          metricsCollector.updateLowerBound(fieldId, value);
          metricsCollector.updateUpperBound(fieldId, value);
        }
      }
    }
  }

  /** Estimate the serialized size of a value in bytes. */
  private long estimateSize(Object value) {
    if (value instanceof String || value instanceof CharSequence) {
      return value.toString().length();
    } else if (value instanceof byte[]) {
      return ((byte[]) value).length;
    } else if (value instanceof Number) {
      if (value instanceof Integer || value instanceof Float) {
        return 4;
      } else if (value instanceof Long || value instanceof Double) {
        return 8;
      }
      return 8;
    } else if (value instanceof Boolean) {
      return 1;
    }
    return 8; // default estimate
  }

  @Override
  public Metrics metrics() {
    Preconditions.checkState(closed, "Metrics are only valid after the file is closed");
    return metricsCollector.toMetrics(schema);
  }

  @Override
  public long length() {
    return fileLength;
  }

  @Override
  public List<Long> splitOffsets() {
    // Return null for now; future Lance implementation would return Fragment offsets
    return null;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      flush();
      closed = true;
    }
  }

  /**
   * Flush buffered records to the output file.
   *
   * <p>In the current implementation, this writes a simple text-based format. In a production
   * implementation, this would use the Lance JNI bridge to write native columnar data.
   */
  private void flush() throws IOException {
    try (OutputStream out = outputFile.createOrOverwrite()) {
      StringBuilder sb = new StringBuilder();
      sb.append("LANCE_V1\n");
      sb.append("schema:").append(schema.toString()).append("\n");
      sb.append("records:").append(bufferedRecords.size()).append("\n");
      sb.append("---\n");

      for (D record : bufferedRecords) {
        if (record instanceof GenericRecord) {
          GenericRecord gr = (GenericRecord) record;
          for (int i = 0; i < schema.columns().size(); i++) {
            if (i > 0) {
              sb.append("|");
            }
            Object val = gr.get(i);
            sb.append(val != null ? val.toString() : "null");
          }
          sb.append("\n");
        } else {
          sb.append(record.toString()).append("\n");
        }
      }

      byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
      out.write(data);
      fileLength = data.length;
    }
    bufferedRecords.clear();
  }

  /**
   * Get the buffered records (for testing).
   *
   * @return the list of buffered records
   */
  List<D> bufferedRecords() {
    return bufferedRecords;
  }
}
