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
package org.apache.iceberg.util;

import java.util.Objects;
import org.apache.iceberg.DeleteFile;

/**
 * Wrapper class to adapt DeleteFile for use in maps and sets.
 *
 * <p>Delete files are identified by location and content range rather than location alone, as a
 * single Puffin file can hold a deletion vector for each of several data files.
 */
public class DeleteFileWrapper implements WrapperSet.Wrapper<DeleteFile> {
  public static DeleteFileWrapper wrap(DeleteFile deleteFile) {
    return new DeleteFileWrapper(deleteFile);
  }

  private DeleteFile file;

  private DeleteFileWrapper(DeleteFile file) {
    this.file = file;
  }

  @Override
  public DeleteFile get() {
    return file;
  }

  @Override
  public DeleteFileWrapper set(DeleteFile deleteFile) {
    this.file = deleteFile;
    return this;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof DeleteFileWrapper)) {
      return false;
    }

    DeleteFileWrapper that = (DeleteFileWrapper) other;
    return Objects.equals(file.location(), that.file.location())
        && Objects.equals(file.contentOffset(), that.file.contentOffset())
        && Objects.equals(file.contentSizeInBytes(), that.file.contentSizeInBytes());
  }

  @Override
  public int hashCode() {
    return Objects.hash(file.location(), file.contentOffset(), file.contentSizeInBytes());
  }

  @Override
  public String toString() {
    return file.location();
  }
}
