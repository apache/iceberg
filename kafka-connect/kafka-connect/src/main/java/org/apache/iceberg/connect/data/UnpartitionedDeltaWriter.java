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
package org.apache.iceberg.connect.data;

import java.io.IOException;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;

public class UnpartitionedDeltaWriter extends BaseDeltaWriter {
  private final RowDataDeltaWriter writer;

  UnpartitionedDeltaWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<Record> fileWriterFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      Set<Integer> identifierFieldIds,
      boolean upsert,
      boolean useDv,
      String cdcField) {
    super(
        spec,
        format,
        fileWriterFactory,
        fileFactory,
        io,
        targetFileSize,
        schema,
        identifierFieldIds,
        upsert,
        useDv,
        cdcField);
    this.writer = new RowDataDeltaWriter(null);
  }

  @Override
  RowDataDeltaWriter route(Record row) {
    return this.writer;
  }

  @Override
  public void close() throws IOException {
    writer.close();
    super.close();
  }
}
