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
package org.apache.iceberg.spark.source;

import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

abstract class BaseRowReader<T extends ScanTask> extends BaseReader<InternalRow, T> {
  private final StructType engineReadSchema;

  BaseRowReader(
      Table table,
      FileIO fileIO,
      ScanTaskGroup<T> taskGroup,
      Schema expectedSchema,
      StructType engineReadSchema,
      boolean caseSensitive,
      boolean cacheDeleteFilesOnExecutors) {
    super(table, fileIO, taskGroup, expectedSchema, caseSensitive, cacheDeleteFilesOnExecutors);
    this.engineReadSchema = engineReadSchema;
  }

  BaseRowReader(
      Table table,
      FileIO fileIO,
      ScanTaskGroup<T> taskGroup,
      Schema expectedSchema,
      boolean caseSensitive,
      boolean cacheDeleteFilesOnExecutors) {
    this(
        table, fileIO, taskGroup, expectedSchema, null, caseSensitive, cacheDeleteFilesOnExecutors);
  }

  protected StructType engineReadSchema() {
    return engineReadSchema;
  }

  protected CloseableIterable<InternalRow> newIterable(
      InputFile file,
      FileFormat format,
      long start,
      long length,
      Expression residual,
      Schema projection,
      Map<Integer, ?> idToConstant) {
    // FormatModelRegistry always returns a ReadBuilder parameterised for InternalRow; the
    // wildcard on the engine-schema type is erased at runtime so this cast is safe.
    @SuppressWarnings("unchecked")
    ReadBuilder<InternalRow, Object> reader =
        (ReadBuilder<InternalRow, Object>)
            FormatModelRegistry.readBuilder(format, InternalRow.class, file);
    reader = reader.project(projection).idToConstant(idToConstant);
    if (engineReadSchema != null) {
      reader = reader.engineProjection(engineReadSchema);
    }

    return reader
        .reuseContainers()
        .split(start, length)
        .caseSensitive(caseSensitive())
        .filter(residual)
        .withNameMapping(nameMapping())
        .build();
  }
}
