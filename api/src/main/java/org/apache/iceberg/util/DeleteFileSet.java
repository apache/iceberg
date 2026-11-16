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

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

public class DeleteFileSet extends WrapperSet<DeleteFile> {
  private static final ThreadLocal<DeleteFileWrapper> WRAPPERS =
      ThreadLocal.withInitial(() -> DeleteFileWrapper.wrap(null));

  private DeleteFileSet() {
    // needed for serialization/deserialization
  }

  private DeleteFileSet(Iterable<Wrapper<DeleteFile>> wrappers) {
    super(wrappers);
  }

  public static DeleteFileSet create() {
    return new DeleteFileSet();
  }

  public static DeleteFileSet of(Iterable<? extends DeleteFile> iterable) {
    return new DeleteFileSet(
        Iterables.transform(
            iterable,
            obj -> {
              Preconditions.checkNotNull(obj, "Invalid object: null");
              return DeleteFileWrapper.wrap(obj);
            }));
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
  protected Class<DeleteFile> elementClass() {
    return DeleteFile.class;
  }
}
