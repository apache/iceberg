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
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
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

  SparkAppenderFactory(Map<String, String> properties, Schema writeSchema, StructType dsSchema) {
    this.properties = properties;
    this.writeSchema = writeSchema;
    this.dsSchema = dsSchema;
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
}
