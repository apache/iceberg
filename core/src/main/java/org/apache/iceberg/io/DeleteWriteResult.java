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

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.util.CharSequenceSet;

/**
 * A result of writing delete files.
 *
 * <p>Note that objects of this class are NOT meant to be serialized. Task or delta writers will
 * wrap these results into their own serializable results that can be sent back to query engines.
 */
public class DeleteWriteResult {
  private final List<DeleteFile> deleteFiles;
  private final CharSequenceSet referencedDataFiles;

  public DeleteWriteResult(DeleteFile deleteFile) {
    this.deleteFiles = Collections.singletonList(deleteFile);
    this.referencedDataFiles = CharSequenceSet.empty();
  }

  public DeleteWriteResult(DeleteFile deleteFile, CharSequenceSet referencedDataFiles) {
    this.deleteFiles = Collections.singletonList(deleteFile);
    this.referencedDataFiles = referencedDataFiles;
  }

  public DeleteWriteResult(List<DeleteFile> deleteFiles) {
    this.deleteFiles = deleteFiles;
    this.referencedDataFiles = CharSequenceSet.empty();
  }

  public DeleteWriteResult(List<DeleteFile> deleteFiles, CharSequenceSet referencedDataFiles) {
    this.deleteFiles = deleteFiles;
    this.referencedDataFiles = referencedDataFiles;
  }

  public List<DeleteFile> deleteFiles() {
    return deleteFiles;
  }

  public CharSequenceSet referencedDataFiles() {
    return referencedDataFiles;
  }

  public boolean referencesDataFiles() {
    return referencedDataFiles != null && !referencedDataFiles.isEmpty();
  }
}
