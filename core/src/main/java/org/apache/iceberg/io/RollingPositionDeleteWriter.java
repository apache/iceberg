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

package org.apache.iceberg.io;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;

/**
 * A rolling position delete writer that splits incoming deletes into multiple files within one spec/partition.
 */
public class RollingPositionDeleteWriter<T> extends RollingDeleteWriter<PositionDelete<T>, PositionDeleteWriter<T>> {

  private final WriterFactory<T> writerFactory;

  public RollingPositionDeleteWriter(WriterFactory<T> writerFactory, OutputFileFactory fileFactory,
                                     FileIO io, FileFormat fileFormat, long targetFileSizeInBytes,
                                     PartitionSpec spec, StructLike partition) {
    super(fileFactory, io, fileFormat, targetFileSizeInBytes, spec, partition);
    this.writerFactory = writerFactory;
    openCurrent();
  }

  @Override
  protected PositionDeleteWriter<T> newWriter(EncryptedOutputFile file) {
    return writerFactory.newPositionDeleteWriter(file, spec(), partition());
  }

  @Override
  protected long length(PositionDeleteWriter<T> writer) {
    return writer.length();
  }
}
