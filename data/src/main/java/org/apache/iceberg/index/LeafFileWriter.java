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
package org.apache.iceberg.index;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

/**
 * Writes a SCALAR index leaf file (Parquet), following the schema defined by {@link
 * LeafFileEntry#schema(Types.NestedField)}.
 *
 * <p>Entries must be added in non-decreasing {@code (transform_value, key_value)} order —
 * required for the row-group-level statistics pruning {@link LeafFileReader} relies on to work at
 * all. This class does not sort; callers (the index build job, e.g. Spark's {@code
 * sortWithinPartitions}) are responsible for producing entries in that order. What this class
 * does do is validate that order as entries are added, and fail fast with {@link
 * IllegalStateException} on the first violation, rather than silently writing an unsorted leaf
 * file that would still return correct results but with row-group pruning providing little to no
 * benefit. Equal consecutive keys are allowed, since a key value is not required to be unique
 * across rows.
 */
public class LeafFileWriter implements AutoCloseable {

  private final FileAppender<Record> appender;
  private final Schema schema;
  private final Types.NestedField keyField;

  private boolean hasWritten = false;
  private long lastTransformValue;
  private Object lastKeyValue;

  public LeafFileWriter(OutputFile outputFile, Types.NestedField keyField) {
    Preconditions.checkNotNull(outputFile, "outputFile is required");
    this.keyField = Preconditions.checkNotNull(keyField, "keyField is required");
    this.schema = LeafFileEntry.schema(keyField);
    try {
      this.appender =
          Parquet.write(outputFile)
              .schema(schema)
              .createWriterFunc(GenericParquetWriter::create)
              .build();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create leaf file writer", e);
    }
  }

  /**
   * Write a single leaf file entry.
   *
   * @throws IllegalStateException if {@code entry} is out of order relative to the previously
   *     written entry
   */
  public void add(LeafFileEntry entry) {
    Preconditions.checkNotNull(entry, "entry is required");
    checkOrder(entry);

    Record record = GenericRecord.create(schema);
    record.setField(keyField.name(), entry.keyValue());
    record.setField(LeafFileEntry.TRANSFORM_VALUE_FIELD_NAME, entry.transformValue());
    record.setField(LeafFileEntry.FILE_PATH_FIELD_NAME, entry.filePath());
    record.setField(LeafFileEntry.POSITION_FIELD_NAME, entry.position());
    appender.add(record);
  }

  /** Write all entries from a list, in the given order. */
  public void addAll(List<LeafFileEntry> entries) {
    entries.forEach(this::add);
  }

  private void checkOrder(LeafFileEntry entry) {
    if (hasWritten) {
      int cmp = Long.compare(entry.transformValue(), lastTransformValue);
      if (cmp == 0) {
        cmp = compareKeys(entry.keyValue(), lastKeyValue);
      }
      if (cmp < 0) {
        throw new IllegalStateException(
            String.format(
                Locale.ROOT,
                "Leaf file entries must be added in non-decreasing (transform_value, key_value) "
                    + "order: entry (%s, %s) is out of order after (%s, %s)",
                entry.transformValue(), entry.keyValue(), lastTransformValue, lastKeyValue));
      }
    }
    this.hasWritten = true;
    this.lastTransformValue = entry.transformValue();
    this.lastKeyValue = entry.keyValue();
  }

  @SuppressWarnings("unchecked")
  private static int compareKeys(Object a, Object b) {
    return ((Comparable<Object>) a).compareTo(b);
  }

  @Override
  public void close() {
    try {
      appender.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close leaf file writer", e);
    }
  }
}

