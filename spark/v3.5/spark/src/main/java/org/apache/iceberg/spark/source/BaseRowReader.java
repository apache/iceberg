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
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.datafile.DataFileServiceRegistry;
import org.apache.iceberg.io.datafile.DeleteFilter;
import org.apache.iceberg.io.datafile.ReaderBuilder;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.SparkOrcReader;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.spark.data.SparkPlannedAvroReader;
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
    return DataFileServiceRegistry.readerBuilder(
            format, InternalRow.class.getName(), file, projection, idToConstant)
        .reuseContainers()
        .split(start, length)
        .filter(residual)
        .caseSensitive(caseSensitive())
        .withNameMapping(nameMapping())
        .build();
  }

  public static class ParquetReaderService implements DataFileServiceRegistry.ReaderService {
    @Override
    public DataFileServiceRegistry.Key key() {
      return new DataFileServiceRegistry.Key(FileFormat.PARQUET, InternalRow.class.getName());
    }

    @Override
    public ReaderBuilder builder(
        InputFile inputFile,
        Schema readSchema,
        Map<Integer, ?> idToConstant,
        DeleteFilter<?> deleteFilter) {
      return Parquet.read(inputFile)
          .project(readSchema)
          .createReaderFunc(
              fileSchema -> SparkParquetReaders.buildReader(readSchema, fileSchema, idToConstant));
    }
  }

  public static class ORCReaderService implements DataFileServiceRegistry.ReaderService {
    @Override
    public DataFileServiceRegistry.Key key() {
      return new DataFileServiceRegistry.Key(FileFormat.ORC, InternalRow.class.getName());
    }

    @Override
    public ReaderBuilder builder(
        InputFile inputFile,
        Schema readSchema,
        Map<Integer, ?> idToConstant,
        DeleteFilter<?> deleteFilter) {
      return ORC.read(inputFile)
          .project(ORC.schemaWithoutConstantAndMetadataFields(readSchema, idToConstant))
          .createReaderFunc(
              readOrcSchema -> new SparkOrcReader(readSchema, readOrcSchema, idToConstant));
    }
  }

  public static class AvroReaderService implements DataFileServiceRegistry.ReaderService {
    @Override
    public DataFileServiceRegistry.Key key() {
      return new DataFileServiceRegistry.Key(FileFormat.AVRO, InternalRow.class.getName());
    }

    @Override
    public ReaderBuilder builder(
        InputFile inputFile,
        Schema readSchema,
        Map<Integer, ?> idToConstant,
        DeleteFilter<?> deleteFilter) {
      return Avro.read(inputFile)
          .project(readSchema)
          .createResolvingReader(schema -> SparkPlannedAvroReader.create(schema, idToConstant));
    }
  }
}
