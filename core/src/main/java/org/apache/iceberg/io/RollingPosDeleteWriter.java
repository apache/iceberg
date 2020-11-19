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

import java.util.Set;
import org.apache.iceberg.ContentFileWriter;
import org.apache.iceberg.ContentFileWriterFactory;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class RollingPosDeleteWriter<T> extends RollingContentFileWriter<DeleteFile, PositionDelete<T>> {
  private final Set<CharSequence> referencedDataFiles;

  public RollingPosDeleteWriter(PartitionKey partitionKey, FileFormat format,
                                OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                                ContentFileWriterFactory<DeleteFile, PositionDelete<T>> writerFactory) {
    super(partitionKey, format, fileFactory, io, targetFileSize, writerFactory);

    this.referencedDataFiles = Sets.newHashSet();
  }

  @Override
  protected void beforeClose(ContentFileWriter<DeleteFile, PositionDelete<T>> writer) {
    PositionDeleteWriter<T> positionDeleteWriter = (PositionDeleteWriter<T>) writer;
    referencedDataFiles.addAll(positionDeleteWriter.referencedDataFiles());
  }

  public Set<CharSequence> referencedDataFiles() {
    return referencedDataFiles;
  }
}
