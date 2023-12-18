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
import org.apache.iceberg.MetadataColumns;
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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.data.SparkAvroReader;
import org.apache.iceberg.spark.data.SparkOrcReader;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.catalyst.InternalRow;

abstract class BaseRowReader<T extends ScanTask> extends BaseReader<InternalRow, T> {
  BaseRowReader(
      Table table,
      ScanTaskGroup<T> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive) {
    super(table, taskGroup, tableSchema, expectedSchema, caseSensitive);
  }

  protected CloseableIterable<InternalRow> newIterable(
      InputFile file,
      FileFormat format,
      long start,
      long length,
      Expression residual,
      Schema projection,
      Map<Integer, ?> idToConstant) {
    switch (format) {
      case PARQUET:
        return newParquetIterable(file, start, length, residual, projection, idToConstant);

      case AVRO:
        return newAvroIterable(file, start, length, projection, idToConstant);

      case ORC:
        return newOrcIterable(file, start, length, residual, projection, idToConstant);

      default:
        throw new UnsupportedOperationException("Cannot read unknown format: " + format);
    }
  }

  private CloseableIterable<InternalRow> newAvroIterable(
      InputFile file, long start, long length, Schema projection, Map<Integer, ?> idToConstant) {
    return Avro.read(file)
        .reuseContainers()
        .project(projection)
        .split(start, length)
        .createReaderFunc(readSchema -> new SparkAvroReader(projection, readSchema, idToConstant))
        .withNameMapping(nameMapping())
        .build();
  }

  private CloseableIterable<InternalRow> newParquetIterable(
      InputFile file,
      long start,
      long length,
      Expression residual,
      Schema readSchema,
      Map<Integer, ?> idToConstant) {
    return Parquet.read(file)
        .reuseContainers()
        .split(start, length)
        .project(readSchema)
        .createReaderFunc(
            fileSchema -> SparkParquetReaders.buildReader(readSchema, fileSchema, idToConstant))
        .filter(residual)
        .caseSensitive(caseSensitive())
        .withNameMapping(nameMapping())
        .build();
  }

  private CloseableIterable<InternalRow> newOrcIterable(
      InputFile file,
      long start,
      long length,
      Expression residual,
      Schema readSchema,
      Map<Integer, ?> idToConstant) {
    Schema readSchemaWithoutConstantAndMetadataFields =
        TypeUtil.selectNot(
            readSchema, Sets.union(idToConstant.keySet(), MetadataColumns.metadataFieldIds()));

    return ORC.read(file)
        .project(readSchemaWithoutConstantAndMetadataFields)
        .split(start, length)
        .createReaderFunc(
            readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema, idToConstant))
        .filter(residual)
        .caseSensitive(caseSensitive())
        .withNameMapping(nameMapping())
        .build();
  }
}
