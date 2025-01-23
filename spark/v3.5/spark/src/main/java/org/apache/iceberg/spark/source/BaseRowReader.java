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
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFileFormats;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.SparkOrcReader;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.spark.data.SparkPlannedAvroReader;
import org.apache.spark.sql.catalyst.InternalRow;

abstract class BaseRowReader<T extends ScanTask> extends BaseReader<InternalRow, T> {

  static {
    DataFileFormats.register(
        FileFormat.PARQUET,
        InternalRow.class,
        (inputFile, task, readSchema, table, deleteFilter) ->
            Parquet.read(inputFile)
                .project(readSchema)
                .createReaderFunc(
                    fileSchema ->
                        SparkParquetReaders.buildReader(
                            readSchema, fileSchema, constantsMap(task, readSchema, table))));

    DataFileFormats.register(
        FileFormat.ORC,
        InternalRow.class,
        (inputFile, task, readSchema, table, deleteFilter) -> {
          Map<Integer, ?> idToConstant = constantsMap(task, readSchema, table);
          return ORC.read(inputFile)
              .project(ORC.schemaWithoutConstantAndMetadataFields(readSchema, idToConstant))
              .createReaderFunc(
                  readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema, idToConstant));
        });

    DataFileFormats.register(
        FileFormat.AVRO,
        InternalRow.class,
        (inputFile, task, readSchema, table, counter) -> {
          Map<Integer, ?> idToConstant = constantsMap(task, readSchema, table);
          return Avro.read(inputFile)
              .project(readSchema)
              .createResolvingReader(
                  fileSchema -> SparkPlannedAvroReader.create(fileSchema, idToConstant));
        });
  }

  BaseRowReader(
      Table table,
      ScanTaskGroup<T> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive) {
    super(table, taskGroup, tableSchema, expectedSchema, caseSensitive);
  }

  protected CloseableIterable<InternalRow> newIterable(
      InputFile file, ContentScanTask<?> task, Expression residual, Schema projection) {
    return DataFileFormats.read(
            task.file().format(), InternalRow.class, file, task, projection, table(), null)
        .reuseContainers()
        .split(task.start(), task.length())
        .filter(residual)
        .caseSensitive(caseSensitive())
        .withNameMapping(nameMapping())
        .build();
  }
}
