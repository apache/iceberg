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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.util.CharSequenceSet;

public class BaseDeltaTaskWriteResult implements DeltaTaskWriter.Result {

  private final DataFile[] dataFiles;
  private final DeleteFile[] deleteFiles;
  private final CharSequenceSet referencedDataFiles;

  public BaseDeltaTaskWriteResult(List<DataFile> dataFiles, List<DeleteFile> deleteFiles,
                                  CharSequenceSet referencedDataFiles) {
    this.dataFiles = dataFiles.toArray(new DataFile[0]);
    this.deleteFiles = deleteFiles.toArray(new DeleteFile[0]);
    this.referencedDataFiles = referencedDataFiles;
  }

  @Override
  public DataFile[] dataFiles() {
    return dataFiles;
  }

  @Override
  public DeleteFile[] deleteFiles() {
    return deleteFiles;
  }

  @Override
  public CharSequenceSet referencedDataFiles() {
    return referencedDataFiles;
  }
}
