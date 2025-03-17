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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry which provides the available {@link ReadBuilder}s and {@link WriteBuilder}s. Based on
 * the `file format`, the required `object model name` and the reader/writer `builderType` the
 * registry returns the correct reader and writer builders. These builders could be used to generate
 * the readers and writers.
 *
 * <p>File formats has to register the {@link ReadBuilder}s and the {@link DataFileAppenderBuilder}s
 * which will be used to create the readers and the writers. The readers returned directly, the
 * appenders are wrapped into a {@link WriteBuilder} and the {@link
 * DataFileAppenderBuilder#build(DataFileAppenderBuilder.WriteMode)} method is used to finalize the
 * appender configuration for the specific writer use-cases. The following inputs should be handled
 * by the appender in the following cases:
 *
 * <ul>
 *   <li>The appender's native input type - {@link DataFileAppenderBuilder.WriteMode#APPENDER},
 *       {@link DataFileAppenderBuilder.WriteMode#DATA_WRITER}, {@link
 *       DataFileAppenderBuilder.WriteMode#EQUALITY_DELETE_WRITER}
 *   <li>{@link org.apache.iceberg.deletes.PositionDelete} where the row is the appender's native
 *       input type - {@link DataFileAppenderBuilder.WriteMode#POSITION_DELETE_WRITER}, {@link
 *       DataFileAppenderBuilder.WriteMode#POSITION_DELETE_WITH_ROW_WRITER}
 * </ul>
 */
public final class DataFileToObjectModelRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileToObjectModelRegistry.class);
  // The list of classes which are used for registering the reader and writer builders
  private static final List<String> CLASSES_TO_REGISTER =
      ImmutableList.of(
          "org.apache.iceberg.parquet.Parquet",
          "org.apache.iceberg.orc.ORC",
          "org.apache.iceberg.arrow.vectorized.ArrowReader",
          "org.apache.iceberg.flink.data.FlinkObjectModels",
          "org.apache.iceberg.spark.source.SparkObjectModels");

  private static final Map<Key, Function<EncryptedOutputFile, DataFileAppenderBuilder<?, ?>>>
      APPENDER_BUILDERS = Maps.newConcurrentMap();
  private static final Map<Key, Function<InputFile, ReadBuilder<?>>> READ_BUILDERS =
      Maps.newConcurrentMap();

  /** Registers a new appender builder for the given format/object model name. */
  public static void registerAppender(
      FileFormat format,
      String objectModelName,
      Function<EncryptedOutputFile, DataFileAppenderBuilder<?, ?>> appenderBuilder) {
    Key key = new Key(format, objectModelName, null);
    if (APPENDER_BUILDERS.containsKey(key)) {
      throw new IllegalArgumentException(
          String.format(
              "Appender builder %s clashes with %s. Both serves %s",
              appenderBuilder.getClass(), APPENDER_BUILDERS.get(key), key));
    }

    APPENDER_BUILDERS.put(key, appenderBuilder);
  }

  /** Registers a new reader builder for the given format/object model name. */
  public static void registerReader(
      FileFormat format, String objectModelName, Function<InputFile, ReadBuilder<?>> readBuilder) {
    registerReader(format, objectModelName, null, readBuilder);
  }

  /** Registers a new reader builder for the given format/object model name/reader type. */
  public static void registerReader(
      FileFormat format,
      String objectModelName,
      String readerType,
      Function<InputFile, ReadBuilder<?>> readBuilder) {
    Key key = new Key(format, objectModelName, readerType);
    if (READ_BUILDERS.containsKey(key)) {
      throw new IllegalArgumentException(
          String.format(
              "Read builder %s clashes with %s. Both serves %s",
              readBuilder.getClass(), READ_BUILDERS.get(key), key));
    }

    READ_BUILDERS.put(new Key(format, objectModelName, readerType), readBuilder);
  }

  @SuppressWarnings("CatchBlockLogException")
  private static void registerSupportedFormats() {
    Avro.register();

    // Uses dynamic methods to call the `register` for the listed classes
    for (String classToRegister : CLASSES_TO_REGISTER) {
      try {
        DynMethods.StaticMethod register =
            DynMethods.builder("register").impl(classToRegister).buildStaticChecked();

        register.invoke();

      } catch (NoSuchMethodException e) {
        // failing to register readers/writers is normal and does not require a stack trace
        LOG.info("Unable to register {} for data files: {}", classToRegister, e.getMessage());
      }
    }
  }

  static {
    registerSupportedFormats();
  }

  private DataFileToObjectModelRegistry() {}

  /**
   * Provides a reader builder for the given input file which returns objects with a given object
   * model name.
   *
   * @param format of the file to read
   * @param objectModelName returned by the reader
   * @param inputFile to read
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static ReadBuilder<?> readBuilder(
      FileFormat format, String objectModelName, InputFile inputFile) {
    return readBuilder(format, objectModelName, null, inputFile);
  }

  /**
   * Provides a reader builder for the given input file which returns objects with a given object
   * model name.
   *
   * @param format of the file to read
   * @param objectModelName returned by the reader
   * @param builderType of the reader builder
   * @param inputFile to read
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static ReadBuilder<?> readBuilder(
      FileFormat format, String objectModelName, String builderType, InputFile inputFile) {
    return READ_BUILDERS.get(new Key(format, objectModelName, builderType)).apply(inputFile);
  }

  /**
   * Provides an appender builder for the given output file which writes objects with a given object
   * model name.
   *
   * @param format of the file to read
   * @param objectModelName accepted by the writer
   * @param outputFile to write
   * @param <E> type for the engine specific schema used by the builder
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <E> AppenderBuilder<?, E> appenderBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return writerFor(format, objectModelName, outputFile);
  }

  /**
   * Provides a data writer builder for the given output file which writes objects with a given
   * object model name.
   *
   * @param format of the file to read
   * @param objectModelName accepted by the writer
   * @param outputFile to write
   * @param <E> type for the engine specific schema used by the builder
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <E> DataWriterBuilder<?, E> writerBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return writerFor(format, objectModelName, outputFile);
  }

  /**
   * Provides a equality delete writer builder for the given output file which writes objects with a
   * given object model name.
   *
   * @param format of the file to read
   * @param objectModelName accepted by the writer
   * @param outputFile to write
   * @param <E> type for the engine specific schema used by the builder
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <E> EqualityDeleteWriterBuilder<?, E> equalityDeleteWriterBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return writerFor(format, objectModelName, outputFile);
  }

  /**
   * Provides a position delete writer builder for the given output file which writes objects with a
   * given object model name.
   *
   * @param format of the file to read
   * @param objectModelName accepted by the writer
   * @param outputFile to write
   * @param <E> type for the engine specific schema used by the builder
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <E> PositionDeleteWriterBuilder<?, E> positionDeleteWriterBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return writerFor(format, objectModelName, outputFile);
  }

  private static <E> WriteBuilder<?, ?, E> writerFor(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return new WriteBuilder<>(
        (DataFileAppenderBuilder<?, E>)
            APPENDER_BUILDERS.get(new Key(format, objectModelName, null)).apply(outputFile),
        outputFile.encryptingOutputFile().location(),
        format);
  }

  /** Key used to identify readers and writers in the {@link DataFileToObjectModelRegistry}. */
  private static class Key {
    private final FileFormat fileFormat;
    private final String objectModelName;
    private final String builderType;

    private Key(FileFormat fileFormat, String objectModelName, String builderType) {
      this.fileFormat = fileFormat;
      this.objectModelName = objectModelName;
      this.builderType = builderType;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("fileFormat", fileFormat)
          .add("objectModelName", objectModelName)
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
          && Objects.equal(other.objectModelName, objectModelName)
          && java.util.Objects.equals(other.builderType, builderType);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(fileFormat, objectModelName, builderType);
    }
  }
}
