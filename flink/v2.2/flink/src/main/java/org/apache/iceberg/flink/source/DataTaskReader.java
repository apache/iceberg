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
package org.apache.iceberg.flink.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.flink.data.StructRowData;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

@Internal
public class DataTaskReader implements FileScanTaskReader<RowData> {

  private final Schema readSchema;

  public DataTaskReader(Schema readSchema) {
    this.readSchema = readSchema;
  }

  @Override
  public CloseableIterator<RowData> open(
      FileScanTask task, InputFilesDecryptor inputFilesDecryptor) {
    StructRowData row = new StructRowData(readSchema.asStruct());
    CloseableIterable<RowData> iterable =
        CloseableIterable.transform(task.asDataTask().rows(), row::setStruct);
    return iterable.iterator();
  }
}
