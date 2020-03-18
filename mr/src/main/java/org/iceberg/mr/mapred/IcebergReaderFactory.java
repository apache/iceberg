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
package org.iceberg.mr.mapred;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;

class IcebergReaderFactory {

  IcebergReaderFactory() {
  }

  public CloseableIterable<Record> createReader(DataFile file, FileScanTask currentTask, InputFile inputFile, Schema tableSchema, boolean reuseContainers) {
    switch (file.format()) {
      case AVRO:
        return buildAvroReader(currentTask, inputFile, tableSchema, reuseContainers);
      case ORC:
        return buildOrcReader(currentTask, inputFile, tableSchema, reuseContainers);
      case PARQUET:
        return buildParquetReader(currentTask, inputFile, tableSchema, reuseContainers);

      default:
        throw new UnsupportedOperationException(String.format("Cannot read %s file: %s", file.format().name(), file.path()));
    }
  }

  // FIXME: use generic reader function
  private CloseableIterable buildAvroReader(FileScanTask task, InputFile file, Schema schema, boolean reuseContainers) {
    Avro.ReadBuilder builder = Avro.read(file)
        .createReaderFunc(DataReader::create)
        .project(schema)
        .split(task.start(), task.length());

    if (reuseContainers) {
      builder.reuseContainers();
    }

    return builder.build();
  }

  // FIXME: use generic reader function
  private CloseableIterable buildOrcReader(FileScanTask task, InputFile file, Schema schema, boolean reuseContainers) {
    ORC.ReadBuilder builder = ORC.read(file)
//            .createReaderFunc() // FIXME: implement
        .schema(schema)
        .split(task.start(), task.length());

    return builder.build();
  }

  // FIXME: use generic reader function
  private CloseableIterable buildParquetReader(FileScanTask task, InputFile file, Schema schema, boolean reuseContainers) {
    Parquet.ReadBuilder builder = Parquet.read(file)
        .createReaderFunc(messageType -> GenericParquetReaders.buildReader(schema, messageType))
        .project(schema)
        .split(task.start(), task.length());

    if (reuseContainers) {
      builder.reuseContainers();
    }

    return builder.build();
  }
}
