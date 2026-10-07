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
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

/**
 * Writes a SCALAR index leaf file (Parquet), following the schema defined by {@link
 * LeafFileEntry#schema(Types.NestedField)}.
 *
 * <p>Entries must be added in non-decreasing {@code (transform_value, key_value)} order — required
 * for the row-group-level statistics pruning {@link LeafFileReader} relies on to work at all. This
 * class does not sort; callers (the index build job, e.g. Spark's {@code sortWithinPartitions}) are
 * responsible for producing entries in that order. What this class does do is validate that order
 * as entries are added, and fail fast with {@link IllegalStateException} on the first violation,
 * rather than silently writing an unsorted leaf file that would still return correct results but
 * with row-group pruning providing little to no benefit. Equal consecutive keys are allowed, since
 * a key value is not required to be unique across rows.
 */
public class LeafFileWriter implements AutoCloseable {

  /**
   * Default Parquet row-group size for leaf files (1 MB), far smaller than Iceberg's 128 MB
   * default. Leaf files are sorted by {@code (transform_value, key_value)} and read via point/range
   * lookups, so small row groups let {@link LeafFileReader}'s statistics filter skip to a tight
   * slice rather than decode a whole large row group.
   */
  public static final long DEFAULT_ROW_GROUP_SIZE_BYTES = 1024 * 1024;

  private final FileAppender<Record> appender;
  private final Schema schema;
  private final List<Types.NestedField> keyFields;

  private boolean hasWritten = false;
  private long lastTransformValue;
  private List<Object> lastKeyValues;

  public LeafFileWriter(OutputFile outputFile, Types.NestedField keyField) {
    this(outputFile, keyField, DEFAULT_ROW_GROUP_SIZE_BYTES);
  }

  public LeafFileWriter(OutputFile outputFile, Types.NestedField keyField, long rowGroupSizeBytes) {
    this(outputFile, ImmutableList.of(keyField), rowGroupSizeBytes);
  }

  public LeafFileWriter(
      OutputFile outputFile, List<Types.NestedField> keyFields, long rowGroupSizeBytes) {
    Preconditions.checkNotNull(outputFile, "outputFile is required");
    Preconditions.checkArgument(
        keyFields != null && !keyFields.isEmpty(), "at least one key field is required");
    this.keyFields = ImmutableList.copyOf(keyFields);
    this.schema = LeafFileEntry.schema(this.keyFields);
    try {
      this.appender =
          Parquet.write(outputFile)
              .schema(schema)
              .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(rowGroupSizeBytes))
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
    List<Object> keyValues = entry.keyValues();
    Preconditions.checkArgument(
        keyValues.size() == keyFields.size(),
        "entry has %s key value(s) but writer expects %s",
        keyValues.size(),
        keyFields.size());
    checkOrder(entry);

    Record record = GenericRecord.create(schema);
    for (int i = 0; i < keyFields.size(); i++) {
      record.setField(keyFields.get(i).name(), keyValues.get(i));
    }
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
        cmp = compareKeyValues(entry.keyValues(), lastKeyValues);
      }
      if (cmp < 0) {
        throw new IllegalStateException(
            String.format(
                Locale.ROOT,
                "Leaf file entries must be added in non-decreasing (transform_value, key values) "
                    + "order: entry (%s, %s) is out of order after (%s, %s)",
                entry.transformValue(),
                entry.keyValues(),
                lastTransformValue,
                lastKeyValues));
      }
    }
    this.hasWritten = true;
    this.lastTransformValue = entry.transformValue();
    this.lastKeyValues = entry.keyValues();
  }

  /** Lexicographic comparison of key values, element by element. */
  private static int compareKeyValues(List<Object> left, List<Object> right) {
    int size = Math.min(left.size(), right.size());
    for (int i = 0; i < size; i++) {
      int cmp = compareKeys(left.get(i), right.get(i));
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(left.size(), right.size());
  }

  @SuppressWarnings("unchecked")
  private static int compareKeys(Object left, Object right) {
    return ((Comparable<Object>) left).compareTo(right);
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
