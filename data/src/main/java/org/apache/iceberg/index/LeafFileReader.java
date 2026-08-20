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
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * Reads a SCALAR index leaf file written by {@link LeafFileWriter}.
 *
 * <p>{@link #readMatching} pushes the lookup predicate down to Parquet via {@code
 * ReadBuilder#filter}, which skips whole row groups whose statistics rule out a match — but the
 * {@link org.apache.iceberg.parquet.ParquetReader} used here (via {@code createReaderFunc}) does
 * *not* filter individual records within a row group that wasn't skipped, unlike the older
 * {@code readSupport}-based read path. So every row group that survives statistics-based skipping
 * still needs an exact per-record check, done here with an {@link Evaluator} bound to the leaf
 * file's schema. There is no hand-rolled search beyond that — Parquet's own row-group statistics
 * do the coarse pruning; the {@link Evaluator} does the exact match.
 */
public class LeafFileReader {

  private LeafFileReader() {}

  /** Read all entries from a leaf file, in file order. */
  public static List<LeafFileEntry> readAll(InputFile inputFile, Types.NestedField keyField) {
    return read(inputFile, keyField, null);
  }

  /**
   * Read only the entries matching {@code predicate}, e.g. {@code Expressions.equal(keyField
   * .name(), value)} for a HASH point lookup, or a range predicate on the key column for an
   * IDENTITY range scan.
   */
  public static List<LeafFileEntry> readMatching(
      InputFile inputFile, Types.NestedField keyField, Expression predicate) {
    Preconditions.checkNotNull(predicate, "predicate is required");
    return read(inputFile, keyField, predicate);
  }

  private static List<LeafFileEntry> read(
      InputFile inputFile, Types.NestedField keyField, Expression predicate) {
    Preconditions.checkNotNull(inputFile, "inputFile is required");
    Preconditions.checkNotNull(keyField, "keyField is required");
    Schema schema = LeafFileEntry.schema(keyField);
    Evaluator evaluator =
        predicate != null ? new Evaluator(schema.asStruct(), predicate) : null;

    Parquet.ReadBuilder builder =
        Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema));
    builder = builder.filter(predicate != null ? predicate : Expressions.alwaysTrue());

    List<LeafFileEntry> entries = Lists.newArrayList();
    try (CloseableIterable<Record> records = builder.build()) {
      for (Record record : records) {
        // ReadBuilder.filter() only skips whole row groups by statistics; it does not filter
        // individual records here, so every candidate record still needs an exact check.
        if (evaluator == null || evaluator.eval(record)) {
          entries.add(fromRecord(record, keyField));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read leaf file: " + inputFile.location(), e);
    }
    return entries;
  }

  private static LeafFileEntry fromRecord(Record record, Types.NestedField keyField) {
    return LeafFileEntry.builder()
        .keyValue(record.getField(keyField.name()))
        .transformValue((Long) record.getField(LeafFileEntry.TRANSFORM_VALUE_FIELD_NAME))
        .filePath((String) record.getField(LeafFileEntry.FILE_PATH_FIELD_NAME))
        .position((Long) record.getField(LeafFileEntry.POSITION_FIELD_NAME))
        .build();
  }
}
