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
package org.apache.iceberg.beam;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.beam.writers.GenericAvroParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

public class BeamAppenderFactory<T extends GenericRecord> implements FileAppenderFactory<T> {
  private final PartitionSpec spec;
  private final Table table;

  BeamAppenderFactory(Table table, PartitionSpec spec) {
    this.spec = spec;
    this.table = table;
  }

  @Override
  public FileAppender<T> newAppender(OutputFile file, FileFormat fileFormat) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      switch (fileFormat) {
        case PARQUET:
          return Parquet.write(file)
              .createWriterFunc(GenericAvroParquetWriter::buildWriter)
              .setAll(table.properties())
              .metricsConfig(metricsConfig)
              .schema(table.schema())
              .overwrite()
              .build();
        default:
          throw new UnsupportedOperationException("Cannot write unknown format: " + fileFormat);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public DataWriter<T> newDataWriter(EncryptedOutputFile file, FileFormat format, StructLike partition) {
    return new DataWriter<>(newAppender(file.encryptingOutputFile(), format), format,
        file.encryptingOutputFile().location(), spec, partition, file.keyMetadata());
  }

  @Override
  public EqualityDeleteWriter<T> newEqDeleteWriter(EncryptedOutputFile file, FileFormat format, StructLike partition) {
    return null;
  }

  @Override
  public PositionDeleteWriter<T> newPosDeleteWriter(EncryptedOutputFile file, FileFormat format, StructLike partition) {
    return null;
  }
}