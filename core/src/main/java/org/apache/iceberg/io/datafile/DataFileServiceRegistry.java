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
package org.apache.iceberg.io.datafile;

import java.util.Map;
import java.util.ServiceLoader;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry which maintains the available {@link ReaderService} and {@link WriterService}
 * implementations. Based on the `file format`, the required `data type` and the reader/writer
 * `builderType` the registry returns the correct reader and writer service implementations. These
 * services could be used to generate the correct reader and writer builders.
 */
public final class DataFileServiceRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileServiceRegistry.class);
  private static final Map<Key, ReaderService> READ_BUILDERS = Maps.newConcurrentMap();
  private static final Map<Key, WriterService<?>> WRITE_BUILDERS = Maps.newConcurrentMap();

  static {
    ServiceLoader<ReaderService> readerServices = ServiceLoader.load(ReaderService.class);
    for (ReaderService service : readerServices) {
      Key key = new Key(service.format(), service.dataType(), service.serviceType());
      if (READ_BUILDERS.containsKey(key)) {
        throw new IllegalArgumentException(
            String.format(
                "Read service %s clashes with %s. Both serves %s",
                service.getClass(), READ_BUILDERS.get(key), key));
      }

      READ_BUILDERS.putIfAbsent(key, service);
    }

    ServiceLoader<WriterService> writerServices = ServiceLoader.load(WriterService.class);
    for (WriterService<?> service : writerServices) {
      Key key = new Key(service.format(), service.dataType(), service.serviceType());
      if (WRITE_BUILDERS.containsKey(key)) {
        throw new IllegalArgumentException(
            String.format(
                "Write service %s clashes with %s. Both serves %s",
                service.getClass(), WRITE_BUILDERS.get(key), key));
      }

      WRITE_BUILDERS.putIfAbsent(key, service);
    }

    LOG.info("DataFileServices found: readers={}, writers={}", READ_BUILDERS, WRITE_BUILDERS);
  }

  private DataFileServiceRegistry() {}

  /**
   * Provides a reader for the given {@link InputFile} which returns objects with a given
   * returnType.
   */
  public static ReaderBuilder<?> read(
      FileFormat format, String returnType, InputFile inputFile, Schema readSchema) {
    return read(format, returnType, null, inputFile, null, readSchema, null, null);
  }

  /**
   * Provides a reader for the given {@link ContentScanTask} which returns objects with a given
   * returnType.
   */
  public static ReaderBuilder<?> read(
      FileFormat format,
      String returnType,
      InputFile inputFile,
      ContentScanTask<?> task,
      Schema readSchema) {
    return read(format, returnType, null, inputFile, task, readSchema, null, null);
  }

  /**
   * Provides a reader for the given input file which returns objects with a given returnType.
   *
   * @param format of the file to read
   * @param returnType returned by the reader
   * @param builderType selects the builder when there are multiple builders for the same format and
   *     return type
   * @param inputFile to read
   * @param task to provide the values for metadata columns (_file_path, _spec_id, _partition)
   * @param readSchema to use when reading the data file
   * @param table to provide old partition specifications. Used for calculating values for
   *     _partition column after specification changes
   * @param deleteFilter is used when the delete record filtering is pushed down to the reader
   * @return {@link ReaderBuilder} for building the actual reader
   */
  public static ReaderBuilder<?> read(
      FileFormat format,
      String returnType,
      String builderType,
      InputFile inputFile,
      ContentScanTask<?> task,
      Schema readSchema,
      Table table,
      DeleteFilter<?> deleteFilter) {
    return READ_BUILDERS
        .get(new Key(format, returnType, builderType))
        .builder(inputFile, task, readSchema, table, deleteFilter);
  }

  /**
   * Provides an appender builder for the given input file which writes objects with a given
   * inputType.
   *
   * @param format of the file to write
   * @param inputType of the rows
   * @param builderType selects the builder when there are multiple builders for the same format and
   *     input type
   * @param outputFile to write
   * @param rowType of the native input data
   * @return {@link AppenderBuilder} for building the actual writer
   */
  public static <S> AppenderBuilder<?> appenderBuilder(
      FileFormat format,
      String inputType,
      String builderType,
      EncryptedOutputFile outputFile,
      S rowType) {
    return ((WriterService<S>) WRITE_BUILDERS.get(new Key(format, inputType, builderType)))
        .appenderBuilder(outputFile, rowType);
  }

  /**
   * Provides a data writer builder for the given input file which writes objects with a given
   * inputType.
   *
   * @param format of the file to write
   * @param inputType of the rows
   * @param builderType selects the builder when there are multiple builders for the same format and
   *     input type
   * @param outputFile to write
   * @param rowType of the native input data
   * @return {@link AppenderBuilder} for building the actual writer
   */
  public static <S> DataWriterBuilder<?> dataWriterBuilder(
      FileFormat format,
      String inputType,
      String builderType,
      EncryptedOutputFile outputFile,
      S rowType) {
    return ((WriterService<S>) WRITE_BUILDERS.get(new Key(format, inputType, builderType)))
        .dataWriterBuilder(outputFile, rowType);
  }

  /**
   * Provides an equality delete writer builder for the given input file which writes objects with a
   * given inputType.
   *
   * @param format of the file to write
   * @param inputType of the rows
   * @param builderType selects the builder when there are multiple builders for the same format and
   *     input type
   * @param outputFile to write
   * @param rowType of the native input data
   * @return {@link AppenderBuilder} for building the actual writer
   */
  public static <S> EqualityDeleteWriterBuilder<?> equalityDeleteWriterBuilder(
      FileFormat format,
      String inputType,
      String builderType,
      EncryptedOutputFile outputFile,
      S rowType) {
    return ((WriterService<S>) WRITE_BUILDERS.get(new Key(format, inputType, builderType)))
        .equalityDeleteWriterBuilder(outputFile, rowType);
  }

  /**
   * Provides an positional delete writer builder for the given input file which writes objects with
   * a given inputType.
   *
   * @param format of the file to write
   * @param inputType of the rows
   * @param builderType selects the builder when there are multiple builders for the same format and
   *     input type
   * @param outputFile to write
   * @param rowType of the native input data
   * @return {@link AppenderBuilder} for building the actual writer
   */
  public static <S> PositionDeleteWriterBuilder<?> positionDeleteWriterBuilder(
      FileFormat format,
      String inputType,
      String builderType,
      EncryptedOutputFile outputFile,
      S rowType) {
    return ((WriterService<S>) WRITE_BUILDERS.get(new Key(format, inputType, builderType)))
        .positionDeleteWriterBuilder(outputFile, rowType);
  }

  interface TypedService {
    /** Returns the file format which is handled by the service. */
    FileFormat format();

    /** Returns the fully qualified data type which is handled by the service. */
    String dataType();

    /**
     * Returns the service type which allows selecting the actual service if multiple services are
     * available for the same format and data type.
     */
    String serviceType();
  }

  private static class Key {
    private final FileFormat format;
    private final String dataType;
    private final String builderType;

    private Key(FileFormat format, String dataType, String builderType) {
      this.format = format;
      this.dataType = dataType;
      this.builderType = builderType;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("format", format)
          .add("dataType", dataType)
          .add("builderType", builderType)
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
      return Objects.equal(other.format, format)
          && Objects.equal(other.dataType, dataType)
          && java.util.Objects.equals(other.builderType, builderType);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(format, dataType, builderType);
    }
  }
}
