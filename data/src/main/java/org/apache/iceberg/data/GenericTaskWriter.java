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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.UnpartitionedWriter;

public class GenericTaskWriter<T extends StructLike> extends BaseTaskWriter<T> {
  private final BaseTaskWriter<T> taskWriter;

  public GenericTaskWriter(
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<T> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    if (spec.isPartitioned()) {
      final InternalRecordWrapper recordWrapper =
          new InternalRecordWrapper(spec.schema().asStruct());
      final PartitionKey partitionKey = new PartitionKey(spec, spec.schema());

      taskWriter =
          new PartitionedFanoutWriter<T>(
              spec, format, appenderFactory, fileFactory, io, targetFileSize) {
            @Override
            protected PartitionKey partition(T record) {
              partitionKey.partition(recordWrapper.wrap(record));
              return partitionKey;
            }
          };
    } else {
      taskWriter =
          new UnpartitionedWriter<T>(
              spec, format, appenderFactory, fileFactory, io, targetFileSize);
    }
  }

  @Override
  public void write(T t) throws IOException {
    taskWriter.write(t);
  }

  @Override
  public void close() throws IOException {
    if (taskWriter != null) {
      taskWriter.close();
    }
  }

  @Override
  public DataFile[] dataFiles() throws IOException {
    return taskWriter.dataFiles();
  }
}
