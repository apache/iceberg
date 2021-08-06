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

import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceSet;

abstract class RollingDeleteWriter<T, W extends FileWriter<T, DeleteWriteResult>>
    extends RollingFileWriter<T, W, DeleteWriteResult> {

  private final List<DeleteFile> deleteFiles;
  private final CharSequenceSet referencedDataFiles;

  protected RollingDeleteWriter(OutputFileFactory fileFactory, FileIO io, FileFormat fileFormat,
                                long targetFileSizeInBytes, PartitionSpec spec, StructLike partition) {
    super(fileFactory, io, fileFormat, targetFileSizeInBytes, spec, partition);
    this.deleteFiles = Lists.newArrayList();
    this.referencedDataFiles = CharSequenceSet.empty();
  }

  @Override
  protected void addResult(DeleteWriteResult result) {
    deleteFiles.addAll(result.deleteFiles());
    referencedDataFiles.addAll(result.referencedDataFiles());
  }

  @Override
  protected DeleteWriteResult aggregatedResult() {
    return new DeleteWriteResult(deleteFiles, referencedDataFiles);
  }
}
