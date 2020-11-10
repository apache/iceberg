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

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.flink.data.FlinkAvroWriter;
import org.apache.iceberg.flink.data.FlinkOrcWriter;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;

public class FlinkFileAppenderFactory implements FileAppenderFactory<RowData>, Serializable {

  private final Schema schema;
  private final RowType flinkSchema;
  private final Map<String, String> props;

  public FlinkFileAppenderFactory(Schema schema, RowType flinkSchema, Map<String, String> props) {
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.props = props;
  }

  @Override
  public FileAppender<RowData> newAppender(OutputFile outputFile, FileFormat format) {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(props);
    try {
      switch (format) {
        case AVRO:
          return Avro.write(outputFile)
              .createWriterFunc(ignore -> new FlinkAvroWriter(flinkSchema))
              .setAll(props)
              .schema(schema)
              .overwrite()
              .build();

        case ORC:
          return ORC.write(outputFile)
              .createWriterFunc((iSchema, typDesc) -> FlinkOrcWriter.buildWriter(flinkSchema, iSchema))
              .setAll(props)
              .schema(schema)
              .overwrite()
              .build();

        case PARQUET:
          return Parquet.write(outputFile)
              .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(flinkSchema, msgType))
              .setAll(props)
              .metricsConfig(metricsConfig)
              .schema(schema)
              .overwrite()
              .build();

        default:
          throw new UnsupportedOperationException("Cannot write unknown file format: " + format);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
