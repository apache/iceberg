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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
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

  // Format models indexed by file format and object model class
  private static final Map<Pair<FileFormat, Class<?>>, FormatModel<?, ?>> MODELS =
      Maps.newConcurrentMap();

  static {
    registerSupportedFormats();
  }

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
  public static synchronized void register(FormatModel<?, ?> formatModel) {
    Pair<FileFormat, Class<?>> key = Pair.of(formatModel.format(), formatModel.type());

    FormatModel<?, ?> existing = MODELS.get(key);
    Preconditions.checkArgument(
        existing == null,
        "Cannot register %s: %s is registered for format=%s type=%s schemaType=%s",
        formatModel.getClass(),
        existing == null ? null : existing.getClass(),
        key.first(),
        key.second(),
        existing == null ? null : existing.schemaType());

    MODELS.put(key, formatModel);
  }

  /**
   * Returns a reader builder for the specified file format and object model.
   *
   * <p>The returned {@link ReadBuilder} provides a fluent interface for configuring how data is
   * read from the input file and converted to the output objects.
   *
   * @param format the file format (Parquet, Avro, ORC) that determines the parsing implementation
   * @param type the output type
   * @param inputFile source file to read data from
   * @param <D> the type of data records the reader will produce
   * @param <S> the type of the output schema for the reader
   * @return a configured reader builder for the specified format and object model
   */
  public static <D, S> ReadBuilder<D, S> readBuilder(
      FileFormat format, Class<D> type, InputFile inputFile) {
    FormatModel<D, S> factory = factoryFor(format, type);
    return factory.readBuilder(inputFile);
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
    return CommonWriteBuilderImpl.forDataFile(
        factory.writeBuilder(outputFile), outputFile.encryptingOutputFile().location(), format);
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
    return CommonWriteBuilderImpl.forEqualityDelete(
        factory.writeBuilder(outputFile), outputFile.encryptingOutputFile().location(), format);
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
  @SuppressWarnings("rawtypes")
  public static PositionDeleteWriteBuilder positionDeleteWriteBuilder(
      FileFormat format, EncryptedOutputFile outputFile) {
    FormatModel<PositionDelete, ?> factory = factoryFor(format, PositionDelete.class);
    return CommonWriteBuilderImpl.forPositionDelete(
        factory.writeBuilder(outputFile), outputFile.encryptingOutputFile().location(), format);
  }

  @VisibleForTesting
  static Map<Pair<FileFormat, Class<?>>, FormatModel<?, ?>> models() {
    return MODELS;
  }

  @SuppressWarnings("unchecked")
  private static <D, S> FormatModel<D, S> factoryFor(FileFormat format, Class<D> type) {
    FormatModel<D, S> model = (FormatModel<D, S>) MODELS.get(Pair.of(format, type));
    Preconditions.checkArgument(
        model != null, "Format model is not registered for format %s and type %s", format, type);
    return model;
  }

  @SuppressWarnings("CatchBlockLogException")
  private static void registerSupportedFormats() {
    // Uses dynamic methods to call the `register` for the listed classes
    for (String classToRegister : CLASSES_TO_REGISTER) {
      try {
        DynMethods.builder("register").impl(classToRegister).buildStaticChecked().invoke();
      } catch (NoSuchMethodException e) {
        // failing to register a factory is normal and does not require a stack trace
        LOG.info(
            "Skip registration of {}. Likely the jar is not in the classpath", classToRegister);
      }
    }
  }

  private FormatModelRegistry() {}
}
