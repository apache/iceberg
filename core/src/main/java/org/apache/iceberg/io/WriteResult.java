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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class WriteResult implements Serializable {
  private DataFile[] dataFiles;
  private DeleteFile[] deleteFiles;

  private WriteResult(List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
    this.dataFiles = dataFiles.toArray(new DataFile[0]);
    this.deleteFiles = deleteFiles.toArray(new DeleteFile[0]);
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

    public Builder add(WriteResult result) {
      addDataFiles(result.dataFiles);
      addDeleteFiles(result.deleteFiles);

      return this;
    }

    public Builder addDataFiles(DataFile... files) {
      Collections.addAll(dataFiles, files);
      return this;
    }

    public Builder addDataFiles(List<DataFile> files) {
      dataFiles.addAll(files);
      return this;
    }

    public Builder addDeleteFiles(DeleteFile... files) {
      Collections.addAll(deleteFiles, files);
      return this;
    }

    public Builder addDeleteFiles(List<DeleteFile> files) {
      deleteFiles.addAll(files);
      return this;
    }

    public WriteResult build() {
      return new WriteResult(dataFiles, deleteFiles);
    }
  }
}
