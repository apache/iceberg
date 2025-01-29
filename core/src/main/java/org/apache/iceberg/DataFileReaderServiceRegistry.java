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
import java.util.ServiceLoader;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.io.FileFormatReadBuilder;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry which maintains the available {@link DataFileReaderService} implementations. Based on
 * the file format and the required return type the registry returns the correct {@link
 * FileFormatReadBuilder} implementations which could be used to generate the readers.
 */
public class DataFileReaderServiceRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileReaderServiceRegistry.class);
  private static final Map<Key, DataFileReaderService> READ_BUILDERS = Maps.newConcurrentMap();

  static {
    ServiceLoader<DataFileReaderService> loader = ServiceLoader.load(DataFileReaderService.class);
    for (DataFileReaderService service : loader) {
      Key key = new Key(service.format(), service.returnType());
      if (READ_BUILDERS.containsKey(key)) {
        throw new IllegalArgumentException(
            String.format(
                "Service %s clashes with %s. Both serves %s",
                service.getClass(), READ_BUILDERS.get(key), key));
      }

      READ_BUILDERS.putIfAbsent(key, service);
    }

    LOG.info("DataFileReaderServices found: {}", READ_BUILDERS);
  }

  private DataFileReaderServiceRegistry() {}

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
    return READ_BUILDERS
        .get(new Key(format, returnType))
        .builder(inputFile, task, readSchema, table, deleteFilter);
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
