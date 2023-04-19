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
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceSet;

public class WriteResult implements Serializable {
  private DataFile[] dataFiles;
  private DeleteFile[] deleteFiles;
  private CharSequence[] referencedDataFiles;

  private WriteResult(
      List<DataFile> dataFiles, List<DeleteFile> deleteFiles, CharSequenceSet referencedDataFiles) {
    this.dataFiles = dataFiles.toArray(new DataFile[0]);
    this.deleteFiles = deleteFiles.toArray(new DeleteFile[0]);
    this.referencedDataFiles = referencedDataFiles.toArray(new CharSequence[0]);
  }

  public DataFile[] dataFiles() {
    return dataFiles;
  }

  public DeleteFile[] deleteFiles() {
    return deleteFiles;
  }

  public CharSequence[] referencedDataFiles() {
    return referencedDataFiles;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final List<DataFile> dataFiles;
    private final List<DeleteFile> deleteFiles;
    private final CharSequenceSet referencedDataFiles;

    private Builder() {
      this.dataFiles = Lists.newArrayList();
      this.deleteFiles = Lists.newArrayList();
      this.referencedDataFiles = CharSequenceSet.empty();
    }

    public Builder add(WriteResult result) {
      addDataFiles(result.dataFiles);
      addDeleteFiles(result.deleteFiles);
      addReferencedDataFiles(result.referencedDataFiles);

      return this;
    }

    public Builder addAll(Iterable<WriteResult> results) {
      results.forEach(this::add);
      return this;
    }

    public Builder addDataFiles(DataFile... files) {
      Collections.addAll(dataFiles, files);
      return this;
    }

    public Builder addDataFiles(Iterable<DataFile> files) {
      Iterables.addAll(dataFiles, files);
      return this;
    }

    public Builder addDeleteFiles(DeleteFile... files) {
      Collections.addAll(deleteFiles, files);
      return this;
    }

    public Builder addDeleteFiles(Iterable<DeleteFile> files) {
      Iterables.addAll(deleteFiles, files);
      return this;
    }

    public Builder addReferencedDataFiles(CharSequence... files) {
      Collections.addAll(referencedDataFiles, files);
      return this;
    }

    public Builder addReferencedDataFiles(Iterable<CharSequence> files) {
      Iterables.addAll(referencedDataFiles, files);
      return this;
    }

    public WriteResult build() {
      return new WriteResult(dataFiles, deleteFiles, referencedDataFiles);
    }
  }
}
