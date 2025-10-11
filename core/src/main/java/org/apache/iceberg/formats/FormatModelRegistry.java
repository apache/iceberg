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
package org.apache.iceberg.formats;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A registry that manages file-format-specific readers and writers through a unified object model
 * factory interface.
 *
 * <p>This registry provides access to {@link ReadBuilder}s for data consumption and various writer
 * builders:
 *
 * <ul>
 *   <li>{@link WriteBuilder} for basic file writing,
 *   <li>{@link DataWriteBuilder} for data files,
 *   <li>{@link EqualityDeleteWriteBuilder} for equality deletes,
 *   <li>{@link PositionDeleteWriteBuilder} for position deletes.
 * </ul>
 *
 * The appropriate builder is selected based on {@link FileFormat} and object model name.
 *
 * <p>{@link FormatModel} objects are registered through {@link #register(FormatModel)} and used for
 * creating readers and writers. Read builders are returned directly from the factory. Write
 * builders may be wrapped in specialized content file writer implementations depending on the
 * requested builder type.
 */
public final class FormatModelRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(FormatModelRegistry.class);
  // The list of classes which are used for registering the reader and writer builders
  private static final List<String> CLASSES_TO_REGISTER = ImmutableList.of();

  private static final Map<Pair<FileFormat, Class<?>>, FormatModel<?, ?>> MODELS =
      Maps.newConcurrentMap();

  private FormatModelRegistry() {}

  /**
   * Registers an {@link FormatModel} in this registry.
   *
   * <p>The {@link FormatModel} creates readers and writers for a specific combinations of file
   * format (Parquet, ORC, Avro) and object model (for example: "generic", "spark", "flink", etc.).
   * Registering custom factories allows integration of new data processing engines for the
   * supported file formats with Iceberg's file access mechanisms.
   *
   * <p>Each factory must be uniquely identified by its combination of file format and object model
   * name. This uniqueness constraint prevents ambiguity when selecting factories for read and write
   * operations.
   *
   * @param formatModel the factory implementation to register
   * @throws IllegalArgumentException if a factory is already registered for the combination of
   *     {@link FormatModel#format()} and {@link FormatModel#type()}
   */
  public static void register(FormatModel<?, ?> formatModel) {
    Pair<FileFormat, Class<?>> key = Pair.of(formatModel.format(), formatModel.type());

    FormatModel<?, ?> existing = MODELS.get(key);
    Preconditions.checkArgument(
        existing == null || checkFormatModelEquals(existing, formatModel),
        "Cannot register %s: %s is registered for format=%s type=%s schemaType=%s",
        formatModel.getClass(),
        existing == null ? null : existing.getClass(),
        key.first(),
        key.second(),
        existing == null ? null : existing.schemaType());

    MODELS.put(key, formatModel);
  }

  private static boolean checkFormatModelEquals(
      FormatModel<?, ?> model1, FormatModel<?, ?> model2) {
    return Objects.equals(model1.getClass().getName(), model2.getClass().getName())
        && Objects.equals(model1.type().getName(), model2.type().getName())
        && Objects.equals(
            model1.schemaType() == null ? null : model1.schemaType().getName(),
            model2.schemaType() == null ? null : model2.schemaType().getName());
  }

  @SuppressWarnings("CatchBlockLogException")
  private static void registerSupportedFormats() {
    // Uses dynamic methods to call the `register` for the listed classes
    for (String classToRegister : CLASSES_TO_REGISTER) {
      try {
        DynMethods.builder("register").impl(classToRegister).buildStaticChecked().invoke();
      } catch (NoSuchMethodException e) {
        // failing to register a factory is normal and does not require a stack trace
        LOG.info("Unable to register {}: {}", classToRegister, e.getMessage());
      }
    }
  }

  static {
    registerSupportedFormats();
  }

  /**
   * Returns a reader builder for the specified file format and object model.
   *
   * <p>The returned {@link ReadBuilder} provides a fluent interface for configuring how data is
   * read from the input file and converted to the output objects. The builder supports
   * configuration options like schema projection, predicate pushdown, batch size and encryption.
   *
   * @param format the file format (Parquet, Avro, ORC) that determines the parsing implementation
   * @param type the output type
   * @param inputFile source file to read data from
   * @param <D> the type of data records the reader will produce
   * @return a configured reader builder for the specified format and object model
   */
  public static <D, S> ReadBuilder readBuilder(
      FileFormat format, Class<D> type, InputFile inputFile) {
    FormatModel<D, S> factory = factoryFor(format, type);
    return factory.readBuilder(inputFile);
  }

  /**
   * Returns a writer builder for appending data to the specified output file.
   *
   * <p>The returned builder produces a {@link FileAppender} that accepts records defined by the
   * specified object model and persists them using the given file format. Data is written to the
   * output file, but this basic writer does not collect or return {@link ContentFile} metadata.
   *
   * @param format the file format used for writing
   * @param type the input type
   * @param outputFile destination for the written data
   * @param <D> the type of data records the writer will accept
   * @return a configured writer builder for creating the appender
   */
  public static <D> WriteBuilder writeBuilder(
      FileFormat format, Class<D> type, EncryptedOutputFile outputFile) {
    FormatModel<D, ?> factory = factoryFor(format, type);
    return factory.writeBuilder(outputFile.encryptingOutputFile()).content(FileContent.DATA);
  }

  /**
   * Returns a writer builder for generating a {@link DataFile}.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the provided file format. Unlike basic writers, this writer
   * collects file metadata during the writing process and generates a {@link DataFile} that can be
   * used for table operations.
   *
   * @param format the file format used for writing
   * @param type the input type
   * @param outputFile destination for the written data
   * @param <D> the type of data records the writer will accept
   * @param <S> the type of the input schema for the writer
   * @return a configured data write builder for creating a {@link DataWriter}
   */
  public static <D, S> DataWriteBuilder<D, S> dataWriteBuilder(
      FileFormat format, Class<D> type, EncryptedOutputFile outputFile) {
    FormatModel<D, S> factory = factoryFor(format, type);
    WriteBuilder writeBuilder =
        factory.writeBuilder(outputFile.encryptingOutputFile()).content(FileContent.DATA);
    return ContentFileWriteBuilderImpl.forDataFile(
        writeBuilder, outputFile.encryptingOutputFile().location(), format);
  }

  /**
   * Creates a writer builder for generating a {@link DeleteFile} with equality deletes.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the given file format. The writer persists equality delete
   * records that identify rows to be deleted based on the configured equality fields, producing a
   * {@link DeleteFile} that can be used for table operations.
   *
   * @param format the file format used for writing
   * @param type the input type
   * @param outputFile destination for the written data
   * @param <D> the type of data records the writer will accept
   * @param <S> the type of the input schema for the writer
   * @return a configured delete write builder for creating an {@link EqualityDeleteWriter}
   */
  public static <D, S> EqualityDeleteWriteBuilder<D, S> equalityDeleteWriteBuilder(
      FileFormat format, Class<D> type, EncryptedOutputFile outputFile) {
    FormatModel<D, S> factory = factoryFor(format, type);
    WriteBuilder writeBuilder =
        factory
            .writeBuilder(outputFile.encryptingOutputFile())
            .content(FileContent.EQUALITY_DELETES);
    return ContentFileWriteBuilderImpl.forEqualityDelete(
        writeBuilder, outputFile.encryptingOutputFile().location(), format);
  }

  /**
   * Creates a writer builder for generating a {@link DeleteFile} with position-based deletes.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the given file format. The writer accepts {@link PositionDelete}
   * records that identify rows to be deleted by file path and position, producing a {@link
   * DeleteFile} that can be used for table operations.
   *
   * @param format the file format used for writing
   * @param outputFile destination for the written data
   * @return a configured delete write builder for creating a {@link PositionDeleteWriter}
   */
  public static PositionDeleteWriteBuilder positionDeleteWriteBuilder(
      FileFormat format, EncryptedOutputFile outputFile) {
    FormatModel<PositionDelete<?>, ?> factory = factoryForPositionDelete(format);
    WriteBuilder writeBuilder =
        factory
            .writeBuilder(outputFile.encryptingOutputFile())
            .content(FileContent.POSITION_DELETES);
    return ContentFileWriteBuilderImpl.forPositionDelete(
        writeBuilder, outputFile.encryptingOutputFile().location(), format);
  }

  @VisibleForTesting
  static Map<Pair<FileFormat, Class<?>>, FormatModel<?, ?>> models() {
    return MODELS;
  }

  @SuppressWarnings("unchecked")
  private static <D, S> FormatModel<D, S> factoryFor(FileFormat format, Class<D> type) {
    FormatModel<D, S> model = ((FormatModel<D, S>) MODELS.get(Pair.of(format, type)));
    Preconditions.checkNotNull(model, "Format model is not registered for %s and %s", format, type);
    return model;
  }

  @SuppressWarnings("unchecked")
  private static FormatModel<PositionDelete<?>, ?> factoryForPositionDelete(FileFormat format) {
    return (FormatModel<PositionDelete<?>, ?>) MODELS.get(Pair.of(format, PositionDelete.class));
  }
}
