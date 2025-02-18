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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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
    for (ReaderService service : ServiceLoader.load(ReaderService.class)) {
      if (READ_BUILDERS.containsKey(service.key())) {
        throw new IllegalArgumentException(
            String.format(
                "Read service %s clashes with %s. Both serves %s",
                service.getClass(), READ_BUILDERS.get(service.key()), service.key()));
      }

      READ_BUILDERS.putIfAbsent(service.key(), service);
    }

    for (WriterService<?> service : ServiceLoader.load(WriterService.class)) {
      if (WRITE_BUILDERS.containsKey(service.key())) {
        throw new IllegalArgumentException(
            String.format(
                "Write service %s clashes with %s. Both serves %s",
                service.getClass(), WRITE_BUILDERS.get(service.key()), service.key()));
      }

      WRITE_BUILDERS.putIfAbsent(service.key(), service);
    }

    LOG.info("DataFileServices found: readers={}, writers={}", READ_BUILDERS, WRITE_BUILDERS);
  }

  private DataFileServiceRegistry() {}

  /**
   * Provides a reader for the given input file which returns objects with a given returnType.
   *
   * @param format of the file to read
   * @param returnType returned by the reader
   * @param inputFile to read
   * @param readSchema to use when reading the data file
   * @return {@link ReaderBuilder} for building the actual reader
   */
  public static ReaderBuilder read(
      FileFormat format, String returnType, InputFile inputFile, Schema readSchema) {
    return read(format, returnType, null, inputFile, readSchema, ImmutableMap.of(), null);
  }

  /**
   * Provides a reader for the given input file which returns objects with a given returnType.
   *
   * @param format of the file to read
   * @param returnType returned by the reader
   * @param inputFile to read
   * @param readSchema to use when reading the data file
   * @param idToConstant to use for getting value for constant fields
   * @return {@link ReaderBuilder} for building the actual reader
   */
  public static ReaderBuilder read(
      FileFormat format,
      String returnType,
      InputFile inputFile,
      Schema readSchema,
      Map<Integer, ?> idToConstant) {
    return read(format, returnType, null, inputFile, readSchema, idToConstant, null);
  }

  /**
   * Provides a reader for the given input file which returns objects with a given returnType.
   *
   * @param format of the file to read
   * @param returnType returned by the reader
   * @param builderType selects the builder when there are multiple builders for the same format and
   *     return type
   * @param inputFile to read
   * @param readSchema to use when reading the data file
   * @param idToConstant to use for getting value for constant fields
   * @param deleteFilter is used when the delete record filtering is pushed down to the reader
   * @return {@link ReaderBuilder} for building the actual reader
   */
  public static ReaderBuilder read(
      FileFormat format,
      String returnType,
      String builderType,
      InputFile inputFile,
      Schema readSchema,
      Map<Integer, ?> idToConstant,
      DeleteFilter<?> deleteFilter) {
    return READ_BUILDERS
        .get(new Key(format, returnType, builderType))
        .builder(inputFile, readSchema, idToConstant, deleteFilter);
  }

  /**
   * Provides an appender builder for the given input file which writes objects with a given
   * inputType.
   *
   * @param format of the file to write
   * @param inputType of the rows
   * @param outputFile to write
   * @param rowType of the native input data
   * @return {@link AppenderBuilder} for building the actual appender
   */
  public static <S> AppenderBuilder appenderBuilder(
      FileFormat format, String inputType, EncryptedOutputFile outputFile, S rowType) {
    return appenderBuilder(format, inputType, null, outputFile, rowType);
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
   * @return {@link AppenderBuilder} for building the actual appender
   */
  public static <S> AppenderBuilder appenderBuilder(
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
   * @param outputFile to write
   * @param rowType of the native input data
   * @return {@link DataWriterBuilder} for building the actual writer
   */
  public static <S> DataWriterBuilder dataWriterBuilder(
      FileFormat format, String inputType, EncryptedOutputFile outputFile, S rowType) {
    return dataWriterBuilder(format, inputType, null, outputFile, rowType);
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
   * @return {@link DataWriterBuilder} for building the actual writer
   */
  public static <S> DataWriterBuilder dataWriterBuilder(
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
   * @param outputFile to write
   * @param rowType of the native input data
   * @return {@link AppenderBuilder} for building the actual writer
   */
  public static <S, B extends EqualityDeleteWriterBuilder<B>>
      EqualityDeleteWriterBuilder<B> equalityDeleteWriterBuilder(
          FileFormat format, String inputType, EncryptedOutputFile outputFile, S rowType) {
    return equalityDeleteWriterBuilder(format, inputType, null, outputFile, rowType);
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
  public static <S, B extends EqualityDeleteWriterBuilder<B>>
      EqualityDeleteWriterBuilder<B> equalityDeleteWriterBuilder(
          FileFormat format,
          String inputType,
          String builderType,
          EncryptedOutputFile outputFile,
          S rowType) {
    return ((WriterService<S>) WRITE_BUILDERS.get(new Key(format, inputType, builderType)))
        .equalityDeleteWriterBuilder(outputFile, rowType);
  }

  /**
   * Provides a positional delete writer builder for the given input file which writes objects with
   * a given inputType.
   *
   * @param format of the file to write
   * @param inputType of the rows
   * @param outputFile to write
   * @param rowType of the native input data
   * @return {@link AppenderBuilder} for building the actual writer
   */
  public static <S, B extends PositionDeleteWriterBuilder<B>>
      PositionDeleteWriterBuilder<B> positionDeleteWriterBuilder(
          FileFormat format, String inputType, EncryptedOutputFile outputFile, S rowType) {
    return positionDeleteWriterBuilder(format, inputType, null, outputFile, rowType);
  }

  /**
   * Provides a positional delete writer builder for the given input file which writes objects with
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
  public static <S, B extends PositionDeleteWriterBuilder<B>>
      PositionDeleteWriterBuilder<B> positionDeleteWriterBuilder(
          FileFormat format,
          String inputType,
          String builderType,
          EncryptedOutputFile outputFile,
          S rowType) {
    return ((WriterService<S>) WRITE_BUILDERS.get(new Key(format, inputType, builderType)))
        .positionDeleteWriterBuilder(outputFile, rowType);
  }

  /**
   * Service building readers. Implementations should be registered through the {@link
   * java.util.ServiceLoader}. {@link DataFileServiceRegistry} is used to collect and serve the
   * reader implementations.
   */
  public interface ReaderService {
    /** Returns the file format which is handled by the service. */
    Key key();

    /**
     * Provides a reader for the given input file which returns objects with a given type.
     *
     * @param inputFile to read
     * @param readSchema to use when reading the data file
     * @param idToConstant used to generate constant values
     * @param deleteFilter is used when the delete record filtering is pushed down to the reader
     * @return {@link ReaderBuilder} for building the actual reader
     */
    ReaderBuilder builder(
        InputFile inputFile,
        Schema readSchema,
        Map<Integer, ?> idToConstant,
        DeleteFilter<?> deleteFilter);
  }

  /**
   * Service building writers. Implementations should be registered through the {@link
   * java.util.ServiceLoader}. {@link DataFileServiceRegistry} is used to collect and serve the
   * writer implementations.
   */
  public interface WriterService<S> {
    /** Returns the file format which is handled by the service. */
    Key key();

    /**
     * Provides an appender builder for the given output file which writes objects with a given
     * input type.
     *
     * @param outputFile to write to
     * @param rowType of the input records
     */
    AppenderBuilder appenderBuilder(EncryptedOutputFile outputFile, S rowType);

    /**
     * Provides a data writer builder for the given output file which writes objects with a given
     * input type.
     *
     * @param outputFile to write to
     * @param rowType of the input records
     */
    DataWriterBuilder dataWriterBuilder(EncryptedOutputFile outputFile, S rowType);

    /**
     * Provides an equality delete writer builder for the given output file which writes objects
     * with a given input type.
     *
     * @param outputFile to write to
     * @param rowType of the input records
     */
    <T extends EqualityDeleteWriterBuilder<T>>
        EqualityDeleteWriterBuilder<T> equalityDeleteWriterBuilder(
            EncryptedOutputFile outputFile, S rowType);

    /**
     * Provides a positional delete writer builder for the given output file which writes objects
     * with a given input type.
     *
     * @param outputFile to write to
     * @param rowType of the input records
     */
    <T extends PositionDeleteWriterBuilder<T>>
        PositionDeleteWriterBuilder<T> positionDeleteWriterBuilder(
            EncryptedOutputFile outputFile, S rowType);
  }

  /** Key used to identify readers and writers in the {@link DataFileServiceRegistry}. */
  public static class Key {
    private final FileFormat fileFormat;
    private final String dataType;
    private final String builderType;

    /**
     * Create the key when there are no multiple builder types available.
     *
     * @param fileFormat the service handles
     * @param dataType the service handles
     */
    public Key(FileFormat fileFormat, String dataType) {
      this.fileFormat = fileFormat;
      this.dataType = dataType;
      this.builderType = null;
    }

    /**
     * Create the key when there are multiple builder types available.
     *
     * @param fileFormat the service handles
     * @param dataType the service handles
     * @param builderType identifier of the builder type when multiple readers/writers are available
     */
    public Key(FileFormat fileFormat, String dataType, String builderType) {
      this.fileFormat = fileFormat;
      this.dataType = dataType;
      this.builderType = builderType;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("fileFormat", fileFormat)
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
      return Objects.equal(other.fileFormat, fileFormat)
          && Objects.equal(other.dataType, dataType)
          && java.util.Objects.equals(other.builderType, builderType);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(fileFormat, dataType, builderType);
    }
  }
}
