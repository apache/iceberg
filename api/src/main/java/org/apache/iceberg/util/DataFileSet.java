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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

public class DataFileSet extends WrapperSet<DataFile> implements Serializable {
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
        Iterables.transform(Iterables.filter(iterable, Objects::nonNull), DataFileWrapper::wrap));
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

  /**
   * Since {@link WrapperSet} itself isn't {@link Serializable}, this requires custom logic to write
   * {@link DataFileSet} to the given stream.
   *
   * @param out The output stream to write to
   * @throws IOException in case the object can't be written
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeInt(set().size());
    for (Wrapper<DataFile> wrapper : set()) {
      out.writeObject(wrapper);
    }
  }

  /**
   * Since {@link WrapperSet} itself isn't {@link Serializable}, this requires custom logic to read
   * {@link DataFileSet} from the given stream.
   *
   * @param in The input stream to read from
   * @throws IOException in case the object can't be read
   * @throws ClassNotFoundException in case the class of the serialized object can't be found
   */
  @SuppressWarnings("unchecked")
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      set().add((Wrapper<DataFile>) in.readObject());
    }
  }

  private static class DataFileWrapper implements Wrapper<DataFile>, Serializable {
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
      if (null == file && null == that.file) {
        return true;
      }

      if (null == file || null == that.file) {
        return false;
      }

      return Objects.equals(file.location(), that.file.location());
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
