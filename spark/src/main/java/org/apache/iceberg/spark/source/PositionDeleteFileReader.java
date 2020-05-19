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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.SparkAvroReader;
import org.apache.iceberg.spark.data.SparkOrcReader;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;

import static org.apache.iceberg.types.Types.NestedField.required;

class PositionDeleteFileReader {
  private final InputFile inputFile;
  private final FileFormat format;

  private static final Schema deleteSchema = new Schema(
      required(1, "file_path", Types.StringType.get()),
      required(2, "position", Types.LongType.get())
  );

  PositionDeleteFileReader(InputFile inputFile, FileFormat format) {
    this.inputFile = inputFile;
    this.format = format;
  }

  public CloseableIterable<InternalRow> open(long start, long length) {
    switch (format) {
      case PARQUET:
        return Parquet.read(inputFile)
            .project(deleteSchema)
            .split(start, length)
            .createReaderFunc(fileSchema -> SparkParquetReaders.buildReader(deleteSchema, fileSchema))
            .build();

      case AVRO:
        return Avro.read(inputFile)
            .reuseContainers()
            .project(deleteSchema)
            .split(start, length)
            .createReaderFunc(SparkAvroReader::new)
            .build();

      case ORC:
        return ORC.read(inputFile)
            .project(deleteSchema)
            .split(start, length)
            .createReaderFunc(SparkOrcReader::new)
            .build();

      default:
        throw new UnsupportedOperationException("Cannot read unknown format: " + format);
    }
  }
}
