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
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class TaskWriterResult {
  private static final TaskWriterResult EMPTY = new TaskWriterResult(ImmutableList.of(), ImmutableList.of());

  private DataFile[] dataFiles;
  private DeleteFile[] deleteFiles;

  TaskWriterResult(DataFile[] dataFiles, DeleteFile[] deleteFiles) {
    this.dataFiles = dataFiles;
    this.deleteFiles = deleteFiles;
  }

  TaskWriterResult(List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
    this.dataFiles = dataFiles.toArray(new DataFile[0]);
    this.deleteFiles = deleteFiles.toArray(new DeleteFile[0]);
  }

  static TaskWriterResult empty() {
    return EMPTY;
  }

  public DataFile[] dataFiles() {
    return dataFiles;
  }

  public DeleteFile[] deleteFiles() {
    return deleteFiles;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final List<DataFile> dataFiles;
    private final List<DeleteFile> deleteFiles;

    private Builder() {
      this.dataFiles = Lists.newArrayList();
      this.deleteFiles = Lists.newArrayList();
    }

    public <T> void add(ContentFile<T> contentFile) {
      Preconditions.checkNotNull(contentFile, "Content file shouldn't be null.");
      switch (contentFile.content()) {
        case DATA:
          this.dataFiles.add((DataFile) contentFile);
          break;

        case EQUALITY_DELETES:
        case POSITION_DELETES:
          this.deleteFiles.add((DeleteFile) contentFile);
          break;

        default:
          throw new UnsupportedOperationException("Unknown file: " + contentFile.content());
      }
    }

    public TaskWriterResult build() {
      return new TaskWriterResult(dataFiles, deleteFiles);
    }
  }
}
