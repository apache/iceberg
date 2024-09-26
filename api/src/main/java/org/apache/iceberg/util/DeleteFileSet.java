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
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;

public class DeleteFileSet extends WrapperSet<DeleteFile> {
  private static final ThreadLocal<DeleteFileWrapper> WRAPPERS =
      ThreadLocal.withInitial(() -> DeleteFileWrapper.wrap(null));

  protected DeleteFileSet(Iterable<Wrapper<DeleteFile>> wrappers) {
    super(wrappers);
  }

  public static DeleteFileSet empty() {
    return new DeleteFileSet(Sets.newLinkedHashSet());
  }

  public static DeleteFileSet of(Iterable<DeleteFile> iterable) {
    return new DeleteFileSet(
        Sets.newLinkedHashSet(Iterables.transform(iterable, DeleteFileWrapper::wrap)));
  }

  @Override
  protected Wrapper<DeleteFile> wrapper() {
    return WRAPPERS.get();
  }

  @Override
  protected Wrapper<DeleteFile> wrap(DeleteFile deleteFile) {
    return DeleteFileWrapper.wrap(deleteFile);
  }

  @Override
  protected boolean isInstance(Object obj) {
    return obj instanceof DeleteFile;
  }

  private static class DeleteFileWrapper implements Wrapper<DeleteFile> {
    private DeleteFile file;

    private DeleteFileWrapper(DeleteFile file) {
      this.file = file;
    }

    private static DeleteFileWrapper wrap(DeleteFile deleteFile) {
      return new DeleteFileWrapper(deleteFile);
    }

    @Override
    public DeleteFile get() {
      return file;
    }

    @Override
    public Wrapper<DeleteFile> set(DeleteFile deleteFile) {
      this.file = deleteFile;
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof DeleteFileWrapper)) {
        return false;
      }

      DeleteFileWrapper that = (DeleteFileWrapper) o;
      if (null == file && null == that.file) {
        return true;
      }

      if (null == file || null == that.file) {
        return false;
      }

      // this needs to be updated once deletion vector support is added
      return 0 == Comparators.charSequences().compare(file.location(), that.file.location());
    }

    @Override
    public int hashCode() {
      return null == file ? 0 : Objects.hashCode(file.location());
    }

    @Override
    public String toString() {
      return null == file ? "null" : file.location();
    }
  }
}
