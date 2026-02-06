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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

public class DataFileSet extends WrapperSet<DataFile> {
  private static final ThreadLocal<DataFileWrapper> WRAPPERS =
      ThreadLocal.withInitial(() -> DataFileWrapper.wrap(null));

  private DataFileSet() {
    // needed for serialization/deserialization
  }

  private DataFileSet(Iterable<Wrapper<DataFile>> wrappers) {
    super(wrappers);
  }

  public static DataFileSet create() {
    return new DataFileSet();
  }

  public static DataFileSet of(Iterable<? extends DataFile> iterable) {
    return new DataFileSet(
        Iterables.transform(
            iterable,
            obj -> {
              Preconditions.checkNotNull(obj, "Invalid object: null");
              return DataFileWrapper.wrap(obj);
            }));
  }

  @Override
  protected Wrapper<DataFile> wrapper() {
    return WRAPPERS.get();
  }

  @Override
  protected Wrapper<DataFile> wrap(DataFile dataFile) {
    return DataFileWrapper.wrap(dataFile);
  }

  @Override
  protected Class<DataFile> elementClass() {
    return DataFile.class;
  }

  private static class DataFileWrapper implements Wrapper<DataFile> {
    private DataFile file;

    private DataFileWrapper(DataFile file) {
      this.file = file;
    }

    private static DataFileWrapper wrap(DataFile dataFile) {
      return new DataFileWrapper(dataFile);
    }

    @Override
    public DataFile get() {
      return file;
    }

    @Override
    public Wrapper<DataFile> set(DataFile dataFile) {
      this.file = dataFile;
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof DataFileWrapper)) {
        return false;
      }

      DataFileWrapper that = (DataFileWrapper) o;
      return Objects.equals(file.location(), that.file.location());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(file.location());
    }

    @Override
    public String toString() {
      return file.location();
    }
  }
}
