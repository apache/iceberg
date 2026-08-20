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
 * <p>Rows must be added in {@code (transform_value, key_value)} sorted order — this class does
 * not sort; callers (the index build job) are responsible for sorting before writing, since that
 * ordering is what leaf-file consumers rely on for page-level pruning.
 */
public class LeafFileWriter implements AutoCloseable {

  private final FileAppender<Record> appender;
  private final Schema schema;

  public LeafFileWriter(OutputFile outputFile, Types.NestedField keyField) {
    Preconditions.checkNotNull(outputFile, "outputFile is required");
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

  /** Write a single leaf file entry. */
  public void add(LeafFileEntry entry) {
    Preconditions.checkNotNull(entry, "entry is required");
    Record record = GenericRecord.create(schema);
    Types.NestedField keyField = schema.columns().get(0);
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

  @Override
  public void close() {
    try {
      appender.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close leaf file writer", e);
    }
  }
}
