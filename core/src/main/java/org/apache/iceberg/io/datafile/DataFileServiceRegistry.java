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
import java.util.function.Function;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry which maintains the available {@link ReadBuilder}s and {@link WriteBuilder}s. Based on
 * the `file format`, the required `data type` and the reader/writer `builderType` the registry
 * returns the correct reader and writer builders. These services could be used to generate the
 * readers and writers.
 */
public final class DataFileServiceRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileServiceRegistry.class);
  // The list of classes which are used for registering the reader and writer builders
  private static final String[] CLASSES_TO_REGISTER =
      new String[] {
        "org.apache.iceberg.parquet.Parquet",
        "org.apache.iceberg.orc.ORC",
        "org.apache.iceberg.arrow.vectorized.ArrowReader",
        "org.apache.iceberg.flink.source.RowDataFileScanTaskReader",
        "org.apache.iceberg.flink.sink.FlinkAppenderFactory",
        "org.apache.iceberg.spark.source.DataFileServices"
      };

  private static final Map<Key, AppenderData<?>> APPENDER_BUILDERS = Maps.newConcurrentMap();
  private static final Map<Key, Function<InputFile, ReadBuilder<?, ?>>> READ_BUILDERS =
      Maps.newConcurrentMap();

  /** Registers a new appender builder for the given format/input type. */
  public static <A extends AppenderBuilder<A>> void registerAppender(
      FileFormat format,
      String inputType,
      Function<EncryptedOutputFile, AppenderBuilder<A>> appenderBuilder,
      AppenderBuilder.Initializer initializer) {
    Key key = new Key(format, inputType, null);
    if (APPENDER_BUILDERS.containsKey(key)) {
      throw new IllegalArgumentException(
          String.format(
              "Appender builder %s clashes with %s. Both serves %s",
              appenderBuilder.getClass(), APPENDER_BUILDERS.get(key), key));
    }

    APPENDER_BUILDERS.putIfAbsent(key, new AppenderData<>(appenderBuilder, initializer));
  }

  /** Registers a new reader builder for the given format/input type. */
  public static void registerReader(
      FileFormat format, String outputType, Function<InputFile, ReadBuilder<?, ?>> readBuilder) {
    registerReader(format, outputType, null, readBuilder);
  }

  /** Registers a new reader builder for the given format/input type/reader type. */
  public static void registerReader(
      FileFormat format,
      String outputType,
      String readerType,
      Function<InputFile, ReadBuilder<?, ?>> readBuilder) {
    Key key = new Key(format, outputType, readerType);
    if (READ_BUILDERS.containsKey(key)) {
      throw new IllegalArgumentException(
          String.format(
              "Read builder %s clashes with %s. Both serves %s",
              readBuilder.getClass(), READ_BUILDERS.get(key), key));
    }

    READ_BUILDERS.put(new Key(format, outputType, readerType), readBuilder);
  }

  @SuppressWarnings("CatchBlockLogException")
  private static void registerSupportedFormats() {
    Avro.register();

    // Uses dynamic methods to call the `register` for the listed classes
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
  public static <D, F> ReadBuilder<D, F> readBuilder(
      FileFormat format, String returnType, InputFile inputFile) {
    return readBuilder(format, returnType, null, inputFile);
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
  public static <D, F> ReadBuilder<D, F> readBuilder(
      FileFormat format, String returnType, String builderType, InputFile inputFile) {
    return (ReadBuilder<D, F>)
        READ_BUILDERS.get(new Key(format, returnType, builderType)).apply(inputFile);
  }

  /**
   * Provides a writer for the given output file which writes objects with a given inputType.
   *
   * @param format of the file to read
   * @param inputType accepted by the writer
   * @param outputFile to write
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <A extends AppenderBuilder<A>, T> WriteBuilder<A, T> writeBuilder(
      FileFormat format, String inputType, EncryptedOutputFile outputFile) {
    AppenderData<A> appenderData =
        (AppenderData<A>) APPENDER_BUILDERS.get(new Key(format, inputType, null));

    return new WriteBuilder<>(
        appenderData.appenderBuilder.apply(outputFile),
        appenderData.initializer,
        outputFile.encryptingOutputFile().location(),
        format);
  }

  /** Key used to identify readers and writers in the {@link DataFileServiceRegistry}. */
  private static class Key {
    private final FileFormat fileFormat;
    private final String dataType;
    private final String builderType;

    private Key(FileFormat fileFormat, String dataType, String builderType) {
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

  /**
   * Object to store a pair of {@link AppenderBuilder} and {@link AppenderBuilder.Initializer}. Used
   * to store them in the registry.
   */
  private static class AppenderData<A extends AppenderBuilder<A>> {
    private final Function<EncryptedOutputFile, AppenderBuilder<A>> appenderBuilder;
    private final AppenderBuilder.Initializer initializer;

    private AppenderData(
        Function<EncryptedOutputFile, AppenderBuilder<A>> appenderBuilder,
        AppenderBuilder.Initializer initializer) {
      this.appenderBuilder = appenderBuilder;
      this.initializer = initializer;
    }
  }
}
