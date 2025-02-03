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
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileFormatAppenderBuilder;
import org.apache.iceberg.io.FileFormatDataWriterBuilder;
import org.apache.iceberg.io.FileFormatEqualityDeleteWriterBuilder;
import org.apache.iceberg.io.FileFormatPositionDeleteWriterBuilder;
import org.apache.iceberg.io.FileFormatReadBuilder;
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
public class DataFileWriterServiceRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileWriterServiceRegistry.class);
  private static final Map<Key, DataFileWriterService> WRITE_BUILDERS = Maps.newConcurrentMap();

  static {
    ServiceLoader<DataFileWriterService> loader = ServiceLoader.load(DataFileWriterService.class);
    for (DataFileWriterService service : loader) {
      Key key = new Key(service.format(), service.returnType());
      if (WRITE_BUILDERS.containsKey(key)) {
        throw new IllegalArgumentException(
            String.format(
                "Service %s clashes with %s. Both serves %s",
                service.getClass(), WRITE_BUILDERS.get(key), key));
      }

      WRITE_BUILDERS.putIfAbsent(key, service);
    }

    LOG.info("DataFileWriterServices found: {}", WRITE_BUILDERS);
  }

  private DataFileWriterServiceRegistry() {}

  /**
   * Provides a reader for the given input file which returns objects with a given returnType.
   *
   * @param format of the file to write
   * @param inputType of the rows
   * @param outputFile to write
   * @return {@link FileFormatAppenderBuilder} for building the actual writer
   */
  public static <S> FileFormatAppenderBuilder<?> appenderBuilder(
      FileFormat format, Class<?> inputType, EncryptedOutputFile outputFile, S rowType) {
    return WRITE_BUILDERS.get(new Key(format, inputType)).appenderBuilder(outputFile, rowType);
  }

  public static <S> FileFormatDataWriterBuilder<?> dataWriterBuilder(
      FileFormat format, Class<?> inputType, EncryptedOutputFile outputFile, S rowType) {
    return WRITE_BUILDERS.get(new Key(format, inputType)).dataWriterBuilder(outputFile, rowType);
  }

  public static <S> FileFormatEqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(
      FileFormat format, Class<?> inputType, EncryptedOutputFile outputFile, S rowType) {
    return WRITE_BUILDERS
        .get(new Key(format, inputType))
        .equalityDeleteWriterBuilder(outputFile, rowType);
  }

  public static <S> FileFormatPositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(
      FileFormat format, Class<?> inputType, EncryptedOutputFile outputFile, S rowType) {
    return WRITE_BUILDERS
        .get(new Key(format, inputType))
        .positionDeleteWriterBuilder(outputFile, rowType);
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
