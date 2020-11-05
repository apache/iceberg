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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class WriterResult {
  private static final WriterResult EMPTY = new WriterResult(ImmutableList.of(), ImmutableList.of());

  private DataFile[] dataFiles;
  private DeleteFile[] deleteFiles;

  WriterResult(DataFile[] dataFiles, DeleteFile[] deleteFiles) {
    this.dataFiles = dataFiles;
    this.deleteFiles = deleteFiles;
  }

  WriterResult(List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
    this.dataFiles = dataFiles.toArray(new DataFile[0]);
    this.deleteFiles = deleteFiles.toArray(new DeleteFile[0]);
  }

  static WriterResult empty() {
    return EMPTY;
  }

  public DataFile[] dataFiles() {
    return dataFiles;
  }

  public DeleteFile[] deleteFiles() {
    return deleteFiles;
  }

  public Iterable<ContentFile<?>> contentFiles() {
    return () -> new Iterator<ContentFile<?>>() {
      private int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex < dataFiles.length + deleteFiles.length;
      }

      @Override
      public ContentFile<?> next() {
        ContentFile<?> contentFile;
        if (currentIndex < dataFiles.length) {
          contentFile = dataFiles[currentIndex];
        } else if (currentIndex < dataFiles.length + deleteFiles.length) {
          contentFile = deleteFiles[currentIndex - dataFiles.length];
        } else {
          throw new NoSuchElementException();
        }
        currentIndex += 1;
        return contentFile;
      }
    };
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

    public void add(WriterResult result) {
      addAll(result.contentFiles());
    }

    public void addAll(Iterable<ContentFile<?>> iterable) {
      for (ContentFile<?> contentFile : iterable) {
        add(contentFile);
      }
    }

    public void add(ContentFile<?> contentFile) {
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

    public WriterResult build() {
      return new WriterResult(dataFiles, deleteFiles);
    }
  }
}
