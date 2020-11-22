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

package org.apache.iceberg.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Factory to create a new {@link FileAppender} to write {@link Record}s.
 */
public class GenericAppenderFactory implements FileAppenderFactory<Record> {

  private final Schema schema;
  private final PartitionSpec spec;
  private final int[] equalityFieldIds;
  private final Map<String, String> config = Maps.newHashMap();

  public GenericAppenderFactory(Schema schema) {
    this.schema = schema;
    // TODO set spec.
    this.spec = PartitionSpec.unpartitioned();
    this.equalityFieldIds = new int[] {0};
  }

  public GenericAppenderFactory set(String property, String value) {
    config.put(property, value);
    return this;
  }

  public GenericAppenderFactory setAll(Map<String, String> properties) {
    config.putAll(properties);
    return this;
  }

  @Override
  public FileAppender<Record> newAppender(OutputFile outputFile, FileFormat fileFormat) {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(config);
    try {
      switch (fileFormat) {
        case AVRO:
          return Avro.write(outputFile)
              .schema(schema)
              .createWriterFunc(DataWriter::create)
              .setAll(config)
              .overwrite()
              .build();

        case PARQUET:
          return Parquet.write(outputFile)
              .schema(schema)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .setAll(config)
              .metricsConfig(metricsConfig)
              .overwrite()
              .build();

        case ORC:
          return ORC.write(outputFile)
              .schema(schema)
              .createWriterFunc(GenericOrcWriter::buildWriter)
              .setAll(config)
              .overwrite()
              .build();

        default:
          throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public org.apache.iceberg.io.DataWriter<Record> newDataWriter(EncryptedOutputFile file, FileFormat format,
                                                                StructLike partition) {
    return new org.apache.iceberg.io.DataWriter<>(
        newAppender(file.encryptingOutputFile(), format), format,
        file.encryptingOutputFile().location(), spec, partition, file.keyMetadata());
  }

  @Override
  public EqualityDeleteWriter<Record> newEqDeleteWriter(EncryptedOutputFile outputFile, FileFormat format,
                                                        StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(config);
    try {
      switch (format) {
        case AVRO:
          return Avro.writeDeletes(outputFile.encryptingOutputFile())
              .createWriterFunc(DataWriter::create)
              .withPartition(partition)
              .overwrite()
              .setAll(config)
              .rowSchema(schema)
              .withSpec(spec)
              .withKeyMetadata(outputFile.keyMetadata())
              .equalityFieldIds(equalityFieldIds)
              .buildEqualityWriter();

        case PARQUET:
          return Parquet.writeDeletes(outputFile.encryptingOutputFile())
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .withPartition(partition)
              .overwrite()
              .setAll(config)
              .metricsConfig(metricsConfig)
              .rowSchema(schema)
              .withSpec(spec)
              .withKeyMetadata(outputFile.keyMetadata())
              .equalityFieldIds(equalityFieldIds)
              .buildEqualityWriter();

        default:
          throw new UnsupportedOperationException("Cannot write unknown file format: " + format);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public PositionDeleteWriter<Record> newPosDeleteWriter(EncryptedOutputFile outputFile, FileFormat format,
                                                         StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(config);
    try {
      switch (format) {
        case AVRO:
          return Avro.writeDeletes(outputFile.encryptingOutputFile())
              .createWriterFunc(DataWriter::create)
              .withPartition(partition)
              .overwrite()
              .setAll(config)
              .rowSchema(schema) // it's a nullable field.
              .withSpec(spec)
              .withKeyMetadata(outputFile.keyMetadata())
              .buildPositionWriter();

        case PARQUET:
          return Parquet.writeDeletes(outputFile.encryptingOutputFile())
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .withPartition(partition)
              .overwrite()
              .setAll(config)
              .metricsConfig(metricsConfig)
              .rowSchema(schema) // it's a nullable field.
              .withSpec(spec)
              .withKeyMetadata(outputFile.keyMetadata())
              .buildPositionWriter();

        default:
          throw new UnsupportedOperationException("Cannot write unknown file format: " + format);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
