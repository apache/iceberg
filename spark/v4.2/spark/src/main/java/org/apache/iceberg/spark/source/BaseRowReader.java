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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.DataFileReadBuilder;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;

abstract class BaseRowReader<T extends ScanTask> extends BaseReader<InternalRow, T> {
  BaseRowReader(
      Table table,
      FileIO fileIO,
      ScanTaskGroup<T> taskGroup,
      Schema expectedSchema,
      boolean caseSensitive,
      boolean cacheDeleteFilesOnExecutors) {
    super(table, fileIO, taskGroup, expectedSchema, caseSensitive, cacheDeleteFilesOnExecutors);
  }

  protected CloseableIterable<InternalRow> newIterable(
      DataFile file,
      long start,
      long length,
      Expression residual,
      Schema projection,
      Map<Integer, ?> idToConstant) {
    ReadBuilder<InternalRow, ?> reader =
        DataFileReadBuilder.read(file, InternalRow.class, this::getInputFile);
    return configureReader(reader, start, length, residual, projection, idToConstant);
  }

  protected CloseableIterable<InternalRow> newIterable(
      DeleteFile file,
      long start,
      long length,
      Expression residual,
      Schema projection,
      Map<Integer, ?> idToConstant) {
    InputFile inputFile = getInputFile(file.location());
    Preconditions.checkArgument(
        inputFile != null, "Cannot find input file for location: %s", file.location());
    ReadBuilder<InternalRow, ?> reader =
        FormatModelRegistry.readBuilder(file.format(), InternalRow.class, inputFile);
    return configureReader(reader, start, length, residual, projection, idToConstant);
  }

  private CloseableIterable<InternalRow> configureReader(
      ReadBuilder<InternalRow, ?> reader,
      long start,
      long length,
      Expression residual,
      Schema projection,
      Map<Integer, ?> idToConstant) {
    return reader
        .project(projection)
        .idToConstant(idToConstant)
        .reuseContainers()
        .split(start, length)
        .caseSensitive(caseSensitive())
        .filter(residual)
        .withNameMapping(nameMapping())
        .build();
  }
}
