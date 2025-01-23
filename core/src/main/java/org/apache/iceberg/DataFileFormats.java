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
package org.apache.iceberg;

import java.util.Map;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.io.FileFormatReadBuilder;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Maintains the available reader and writer factories for the supported {@link FileFormat}s. {@link
 * BuilderFactory} needs to be registered for {@link FileFormatReadBuilder} with the supported
 * return type.
 */
public class DataFileFormats {
  private static final Map<Key, BuilderFactory> READ_BUILDERS = Maps.newConcurrentMap();

  private DataFileFormats() {}

  /**
   * Registers a new reader factory which could be used to read the given {@link FileFormat} and
   * returns an object with a given returnType for every row.
   *
   * @param format which could be read by the factory
   * @param returnType of the reader created by the factory
   * @param factory used for reading the given file format and returning the give type
   */
  public static void register(FileFormat format, Class<?> returnType, BuilderFactory factory) {
    READ_BUILDERS.put(new Key(format, returnType), factory);
  }

  /**
   * Provides a reader for the given {@link InputFile} which returns objects with a given
   * returnType.
   */
  public static FileFormatReadBuilder<?> read(
      FileFormat format, Class<?> returnType, InputFile inputFile, Schema readSchema) {
    return read(format, returnType, inputFile, null, readSchema, null, null);
  }

  /**
   * Provides a reader for the given {@link ContentScanTask} which returns objects with a given
   * returnType.
   */
  public static FileFormatReadBuilder<?> read(
      FileFormat format,
      Class<?> returnType,
      InputFile inputFile,
      ContentScanTask<?> task,
      Schema readSchema) {
    return read(format, returnType, inputFile, task, readSchema, null, null);
  }

  /**
   * Provides a reader for the given input file which returns objects with a given returnType.
   *
   * @param format of the file to read
   * @param returnType returned by the reader
   * @param inputFile to read
   * @param task to provide the values for metadata columns (_file_path, _spec_id, _partition)
   * @param readSchema to use when reading the data file
   * @param table to provide old partition specifications. Used for calculating values for
   *     _partition column after specification changes
   * @param deleteFilter is used when the delete record filtering is pushed down to the reader
   * @return {@link FileFormatReadBuilder} for building the actual reader
   */
  public static FileFormatReadBuilder<?> read(
      FileFormat format,
      Class<?> returnType,
      InputFile inputFile,
      ContentScanTask<?> task,
      Schema readSchema,
      Table table,
      DeleteFilter<?> deleteFilter) {
    BuilderFactory factory = READ_BUILDERS.get(new Key(format, returnType));
    if (factory != null) {
      return factory.readBuilder(inputFile, task, readSchema, table, deleteFilter);
    } else {
      throw new UnsupportedOperationException(
          String.format("Cannot find factory for format: %s and type: %s", format, returnType));
    }
  }

  public interface BuilderFactory {
    FileFormatReadBuilder<?> readBuilder(
        InputFile inputFile,
        ContentScanTask<?> task,
        Schema readSchema,
        Table table,
        DeleteFilter<?> deleteFilter);
  }

  private static class Key {
    private final FileFormat format;
    private final Class<?> returnType;

    private Key(FileFormat format, Class<?> returnType) {
      this.format = format;
      this.returnType = returnType;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("format", format)
          .add("returnType", returnType)
          .toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof Key)) {
        return false;
      }

      Key other = (Key) o;
      return Objects.equal(other.format, format) && Objects.equal(other.returnType, returnType);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(format, returnType);
    }
  }
}
