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
import java.util.function.Function;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry which maintains the available {@link ReadBuilder}s and {@link WriterService}
 * implementations. Based on the `file format`, the required `data type` and the reader/writer
 * `builderType` the registry returns the correct reader and writer service implementations. These
 * services could be used to generate the correct reader and writer builders.
 */
public final class DataFileServiceRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileServiceRegistry.class);
  private static final String[] CLASSES_TO_REGISTER =
      new String[] {
        "org.apache.iceberg.parquet.Parquet",
        "org.apache.iceberg.orc.ORC",
        "org.apache.iceberg.arrow.vectorized.ArrowReader",
        "org.apache.iceberg.spark.source.Readers",
        "org.apache.iceberg.flink.source.RowDataFileScanTaskReader"
      };

  private static final Map<Key, Function<OutputFile, WriteBuilder<?>>> WRITE_BUILDERS =
      Maps.newConcurrentMap();
  private static final Map<Key, Function<InputFile, ReadBuilder<?, ?>>> READ_BUILDERS =
      Maps.newConcurrentMap();

  static void registerWrite(
      FileFormat format, String inputType, Function<OutputFile, WriteBuilder<?>> writeBuilder) {
    registerWrite(format, inputType, null, writeBuilder);
  }

  public static void registerRead(
      FileFormat format, String inputType, Function<InputFile, ReadBuilder<?, ?>> readBuilder) {
    registerRead(format, inputType, null, readBuilder);
  }

  static void registerWrite(
      FileFormat format,
      String inputType,
      String writerType,
      Function<OutputFile, WriteBuilder<?>> writeBuilder) {
    WRITE_BUILDERS.put(new Key(format, inputType, writerType), writeBuilder);
  }

  public static void registerRead(
      FileFormat format,
      String inputType,
      String readerType,
      Function<InputFile, ReadBuilder<?, ?>> readBuilder) {
    READ_BUILDERS.put(new Key(format, inputType, readerType), readBuilder);
  }

  @SuppressWarnings("CatchBlockLogException")
  private static void registerSupportedFormats() {
    registerRead(
        FileFormat.AVRO,
        Record.class.getName(),
        inputFile ->
            new Avro.DataReadBuilder<>(inputFile).readerFunction(PlannedDataReader::create));

    for (String s : CLASSES_TO_REGISTER) {
      try {
        DynMethods.StaticMethod register =
            DynMethods.builder("register").impl(s).buildStaticChecked();

        register.invoke();

      } catch (NoSuchMethodException e) {
        // failing to register readers/writers is normal and does not require a stack trace
        LOG.info("Unable to register {} for data files: {}", s, e.getMessage());
      }
    }
  }

  static {
    registerSupportedFormats();
  }

  private DataFileServiceRegistry() {}

  /**
   * Provides a reader for the given input file which returns objects with a given returnType.
   *
   * @param format of the file to read
   * @param returnType returned by the reader
   * @param inputFile to read
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <D, F> ReadBuilder<D, F> readerBuilder(
      FileFormat format, String returnType, InputFile inputFile) {
    return readerBuilder(format, returnType, null, inputFile);
  }

  /**
   * Provides a reader for the given input file which returns objects with a given returnType.
   *
   * @param format of the file to read
   * @param returnType returned by the reader
   * @param builderType of the reader builder
   * @param inputFile to read
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <D, F> ReadBuilder<D, F> readerBuilder(
      FileFormat format, String returnType, String builderType, InputFile inputFile) {
    return (ReadBuilder<D, F>)
        READ_BUILDERS.get(new Key(format, returnType, builderType)).apply(inputFile);
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
    return Registry.writeBuilderFor(format, inputType, builderType)
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
    return Registry.writeBuilderFor(format, inputType, builderType)
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
    return Registry.writeBuilderFor(format, inputType, builderType)
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
    return Registry.writeBuilderFor(format, inputType, builderType)
        .positionDeleteWriterBuilder(outputFile, rowType);
  }

  /**
   * Internal class providing the actual registry. This is a separate class to avoid class loader
   * issues.
   */
  private static final class Registry {
    private static final Map<Key, WriterService<?>> WRITE_BUILDERS = Maps.newConcurrentMap();

    static {
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

    private static <S> WriterService<S> writeBuilderFor(
        FileFormat format, String inputType, String builderType) {
      Key key = new Key(format, inputType, builderType);
      WriterService<S> service = (WriterService<S>) WRITE_BUILDERS.get(key);
      if (service == null) {
        throw new IllegalArgumentException(
            String.format("No writer builder registered for key %s", key));
      }

      return service;
    }
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
