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
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.ContentFileWriter;
import org.apache.iceberg.ContentFileWriterFactory;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.data.FlinkAvroWriter;
import org.apache.iceberg.flink.data.FlinkParquetWriters;
import org.apache.iceberg.parquet.Parquet;

public class FlinkEqualityDeleterFactory implements ContentFileWriterFactory<DeleteFile, RowData>, Serializable {
  private final Schema schema;
  private final RowType flinkSchema;
  private final PartitionSpec spec;
  private final List<Integer> equalityFieldIds;
  private final Map<String, String> props;

  public FlinkEqualityDeleterFactory(Schema schema,
                                     RowType flinkSchema,
                                     PartitionSpec spec,
                                     List<Integer> equalityFieldIds,
                                     Map<String, String> props) {
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.spec = spec;
    this.equalityFieldIds = equalityFieldIds;
    this.props = props;
  }

  @Override
  public ContentFileWriter<DeleteFile, RowData> createWriter(PartitionKey partitionKey,
                                                             EncryptedOutputFile outputFile,
                                                             FileFormat fileFormat) {
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(props);
    try {
      switch (fileFormat) {
        case AVRO:
          return Avro.writeDeletes(outputFile.encryptingOutputFile())
              .createWriterFunc(ignore -> new FlinkAvroWriter(flinkSchema))
              .withPartition(partitionKey)
              .overwrite()
              .setAll(props)
              .rowSchema(schema)
              .withSpec(spec)
              .equalityFieldIds(equalityFieldIds)
              .buildEqualityWriter();

        case PARQUET:
          return Parquet.writeDeletes(outputFile.encryptingOutputFile())
              .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(flinkSchema, msgType))
              .withPartition(partitionKey)
              .overwrite()
              .setAll(props)
              .metricsConfig(metricsConfig)
              .rowSchema(schema)
              .withSpec(spec)
              .equalityFieldIds(equalityFieldIds)
              .buildEqualityWriter();

        case ORC:
        default:
          throw new UnsupportedOperationException("Cannot write unknown file format: " + fileFormat);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
