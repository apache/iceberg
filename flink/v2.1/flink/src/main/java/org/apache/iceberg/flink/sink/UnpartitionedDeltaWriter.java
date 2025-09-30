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
import java.util.Set;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningDVWriter;

class UnpartitionedDeltaWriter extends BaseDeltaTaskWriter {
  private final RowDataDeltaWriter writer;
  private PartitioningDVWriter dvFileWriter;

  UnpartitionedDeltaWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<RowData> fileWriterFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      RowType flinkSchema,
      Set<Integer> equalityFieldIds,
      boolean upsert,
      boolean userDv) {
    super(
        spec,
        format,
        fileWriterFactory,
        fileFactory,
        io,
        targetFileSize,
        schema,
        flinkSchema,
        equalityFieldIds,
        upsert,
        userDv);
    this.dvFileWriter = new PartitioningDVWriter<>(fileFactory, p -> null);
    this.writer = new RowDataDeltaWriter(null, dvFileWriter);
  }

  @Override
  RowDataDeltaWriter route(RowData row) {
    return writer;
  }

  @Override
  public void close() throws IOException {
    writer.close();
    if (dvFileWriter != null) {
      try {
        // complete will call close
        dvFileWriter.close();
        DeleteWriteResult result = dvFileWriter.result();
        addCompletedDeleteFiles(result.deleteFiles());
        addReferencedDataFiles(result.referencedDataFiles());
      } finally {
        dvFileWriter = null;
      }
    }
  }
}
