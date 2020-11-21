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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.SparkAvroWriter;
import org.apache.iceberg.spark.data.SparkOrcWriter;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

class SparkAppenderFactory implements FileAppenderFactory<InternalRow> {
  private final Map<String, String> properties;
  private final Schema writeSchema;
  private final StructType dsSchema;
  private final PartitionSpec spec;
  private final int[] equalityFieldIds;

  SparkAppenderFactory(Map<String, String> properties, Schema writeSchema, StructType dsSchema) {
    this.properties = properties;
    this.writeSchema = writeSchema;
    this.dsSchema = dsSchema;
    // TODO: set these for real
    this.spec = PartitionSpec.unpartitioned();
    this.equalityFieldIds = new int[] { 0 };
  }

  @Override
  public FileAppender<InternalRow> newAppender(OutputFile file, FileFormat fileFormat) {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(properties);
    try {
      switch (fileFormat) {
        case PARQUET:
          return Parquet.write(file)
              .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(dsSchema, msgType))
              .setAll(properties)
              .metricsConfig(metricsConfig)
              .schema(writeSchema)
              .overwrite()
              .build();

        case AVRO:
          return Avro.write(file)
              .createWriterFunc(ignored -> new SparkAvroWriter(dsSchema))
              .setAll(properties)
              .schema(writeSchema)
              .overwrite()
              .build();

        case ORC:
          return ORC.write(file)
              .createWriterFunc(SparkOrcWriter::new)
              .setAll(properties)
              .metricsConfig(metricsConfig)
              .schema(writeSchema)
              .overwrite()
              .build();

        default:
          throw new UnsupportedOperationException("Cannot write unknown format: " + fileFormat);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  public DataWriter<InternalRow> newDataWriter(EncryptedOutputFile file, FileFormat fileFormat, StructLike partition) {
    return new DataWriter<>(
        newAppender(file.encryptingOutputFile(), fileFormat), fileFormat,
        file.encryptingOutputFile().location(), spec, partition, file.keyMetadata());
  }

  @Override
  public EqualityDeleteWriter<InternalRow> newEqDeleteWriter(EncryptedOutputFile outputFile, FileFormat format,
                                                             StructLike partition) {
    try {
      switch (format) {
        case PARQUET:
          return Parquet.writeDeletes(outputFile.encryptingOutputFile())
              .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(dsSchema, msgType))
              .overwrite()
              .rowSchema(writeSchema)
              .withSpec(spec)
              .withPartition(partition)
              .equalityFieldIds(equalityFieldIds)
              .withKeyMetadata(outputFile.keyMetadata())
              .buildEqualityWriter();

        case AVRO:
          return Avro.writeDeletes(outputFile.encryptingOutputFile())
              .createWriterFunc(ignored -> new SparkAvroWriter(dsSchema))
              .overwrite()
              .rowSchema(writeSchema)
              .withSpec(spec)
              .withPartition(partition)
              .equalityFieldIds(equalityFieldIds)
              .withKeyMetadata(outputFile.keyMetadata())
              .buildEqualityWriter();

        default:
          throw new UnsupportedOperationException("Cannot write unsupported format: " + format);
      }

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create new equality delete writer", e);
    }
  }

  @Override
  public PositionDeleteWriter<InternalRow> newPosDeleteWriter(EncryptedOutputFile outputFile, FileFormat format,
                                                              StructLike partition) {
    try {
      switch (format) {
        case PARQUET:
          return Parquet.writeDeletes(outputFile.encryptingOutputFile())
              .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(dsSchema, msgType))
              .overwrite()
              .rowSchema(writeSchema)
              .withSpec(spec)
              .withPartition(partition)
              .withKeyMetadata(outputFile.keyMetadata())
              .buildPositionWriter();

        case AVRO:
          return Avro.writeDeletes(outputFile.encryptingOutputFile())
              .createWriterFunc(ignored -> new SparkAvroWriter(dsSchema))
              .overwrite()
              .rowSchema(writeSchema)
              .withSpec(spec)
              .withPartition(partition)
              .withKeyMetadata(outputFile.keyMetadata())
              .buildPositionWriter();

        default:
          throw new UnsupportedOperationException("Cannot write unsupported format: " + format);
      }

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create new equality delete writer", e);
    }
  }
}
