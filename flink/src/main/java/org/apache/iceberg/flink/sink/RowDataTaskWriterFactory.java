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

import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class RowDataTaskWriterFactory implements TaskWriterFactory<RowData> {
  private final Schema schema;
  private final RowType flinkSchema;
  private final PartitionSpec spec;
  private final LocationProvider locations;
  private final FileIO io;
  private final EncryptionManager encryptionManager;
  private final long targetFileSizeBytes;
  private final FileFormat format;
  private final FileAppenderFactory<RowData> appenderFactory;

  private transient OutputFileFactory outputFileFactory;

  public RowDataTaskWriterFactory(Schema schema,
                                  RowType flinkSchema,
                                  PartitionSpec spec,
                                  LocationProvider locations,
                                  FileIO io,
                                  EncryptionManager encryptionManager,
                                  long targetFileSizeBytes,
                                  FileFormat format,
                                  Map<String, String> tableProperties,
                                  List<Integer> equalityFieldIds) {
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.spec = spec;
    this.locations = locations;
    this.io = io;
    this.encryptionManager = encryptionManager;
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.format = format;
    this.appenderFactory = new FlinkFileAppenderFactory(schema, flinkSchema, tableProperties, spec, equalityFieldIds);
  }

  @Override
  public void initialize(int taskId, int attemptId) {
    this.outputFileFactory = new OutputFileFactory(spec, format, locations, io, encryptionManager, taskId, attemptId);
  }

  @Override
  public TaskWriter<RowData> create() {
    Preconditions.checkNotNull(outputFileFactory,
        "The outputFileFactory shouldn't be null if we have invoked the initialize().");

    List<Integer> equalityFieldIds = Lists.newArrayList();
    return new RowDataTaskWriter(spec, format, appenderFactory, outputFileFactory, io, targetFileSizeBytes, schema,
        flinkSchema, equalityFieldIds);
  }
}
