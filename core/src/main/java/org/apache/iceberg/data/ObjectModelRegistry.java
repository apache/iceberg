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
package org.apache.iceberg.data;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ObjectModelFactory;
import org.apache.iceberg.io.ReadBuilder;
import org.apache.iceberg.io.WriteBuilder;
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
 * <p>{@link ObjectModelFactory} objects are registered through {@link
 * #registerObjectModelFactory(ObjectModelFactory)} and used for creating readers and writers. Read
 * builders are returned directly from the factory. Write builders may be wrapped in specialized
 * content file writer implementations depending on the requested builder type.
 */
public final class ObjectModelRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectModelRegistry.class);
  // The list of classes which are used for registering the reader and writer builders
  private static final List<String> CLASSES_TO_REGISTER =
      ImmutableList.of(
          "org.apache.iceberg.data.GenericObjectModels",
          "org.apache.iceberg.arrow.vectorized.ArrowReader",
          "org.apache.iceberg.flink.data.FlinkObjectModels",
          "org.apache.iceberg.spark.source.SparkObjectModels");

  private static final Map<Pair<FileFormat, String>, ObjectModelFactory<?, ?>>
      OBJECT_MODEL_FACTORIES = Maps.newConcurrentMap();

  /**
   * Registers an {@link ObjectModelFactory} in this registry.
   *
   * <p>The {@link ObjectModelFactory} creates readers and writers for a specific combinations of
   * file format (Parquet, ORC, Avro) and object model (for example: "generic", "spark", "flink",
   * etc.). Registering custom factories allows integration of new data processing engines for the
   * supported file formats with Iceberg's file access mechanisms.
   *
   * <p>Each factory must be uniquely identified by its combination of file format and object model
   * name. This uniqueness constraint prevents ambiguity when selecting factories for read and write
   * operations.
   *
   * @param objectModelFactory the factory implementation to register
   * @throws IllegalArgumentException if a factory is already registered for the combination of
   *     {@link ObjectModelFactory#format()} and {@link ObjectModelFactory#objectModelName()}
   */
  @SuppressWarnings("CatchBlockLogException")
  public static void registerObjectModelFactory(ObjectModelFactory<?, ?> objectModelFactory) {
    Pair<FileFormat, String> key =
        Pair.of(objectModelFactory.format(), objectModelFactory.objectModelName());
    if (OBJECT_MODEL_FACTORIES.containsKey(key)) {
      throw new IllegalArgumentException(
          String.format(
              "Object model factory %s clashes with %s. Both serves %s",
              objectModelFactory.getClass(), OBJECT_MODEL_FACTORIES.get(key), key));
    }

    OBJECT_MODEL_FACTORIES.put(key, objectModelFactory);
  }

  @SuppressWarnings("CatchBlockLogException")
  private static void registerSupportedFormats() {
    // Uses dynamic methods to call the `register` for the listed classes
    for (String classToRegister : CLASSES_TO_REGISTER) {
      try {
        DynMethods.builder("register").impl(classToRegister).buildStaticChecked().invoke();
      } catch (NoSuchMethodException e) {
        // failing to register a factory is normal and does not require a stack trace
        LOG.info("Unable to register {} for data files: {}", classToRegister, e.getMessage());
      }
    }
  }

  static {
    registerSupportedFormats();
  }

  private ObjectModelRegistry() {}

  /**
   * Returns a reader builder for the specified file format and object model.
   *
   * <p>The returned {@link ReadBuilder} provides a fluent interface for configuring how data is
   * read from the input file and converted to the output objects. The builder supports
   * configuration options like schema projection, predicate pushdown, batch size and encryption.
   *
   * @param format the file format (Parquet, Avro, ORC) that determines the parsing implementation
   * @param objectModelName identifier for the expected output data representation (generic, spark,
   *     flink, etc.)
   * @param inputFile source file to read data from
   * @param <D> the type of data records the reader will produce
   * @return a configured reader builder for the specified format and object model
   */
  public static <D> ReadBuilder<?, D> readBuilder(
      FileFormat format, String objectModelName, InputFile inputFile) {
    ObjectModelFactory<?, D> factory = factoryFor(format, objectModelName);
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
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> input schema type required by the writer for data conversion
   * @param <D> the type of data records the writer will accept
   * @return a configured writer builder for creating the appender
   */
  public static <E, D> WriteBuilder<?, E, D> writeBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    ObjectModelFactory<E, D> factory = factoryFor(format, objectModelName);
    return factory.dataWriteBuilder(outputFile.encryptingOutputFile());
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
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> input schema type required by the writer for data conversion
   * @param <D> the type of data records the writer will accept
   * @return a configured data write builder for creating a {@link DataWriter}
   */
  public static <E, D> DataWriteBuilder<?, E, D> dataWriteBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    ObjectModelFactory<E, D> factory = factoryFor(format, objectModelName);
    WriteBuilder<?, E, D> writeBuilder =
        factory.equalityDeleteWriteBuilder(outputFile.encryptingOutputFile());
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
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> input schema type required by the writer for data conversion
   * @param <D> the type of data records the writer will accept
   * @return a configured delete write builder for creating an {@link EqualityDeleteWriter}
   */
  public static <E, D> EqualityDeleteWriteBuilder<?, E, D> equalityDeleteWriteBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    ObjectModelFactory<E, D> factory = factoryFor(format, objectModelName);
    WriteBuilder<?, E, D> writeBuilder =
        factory.equalityDeleteWriteBuilder(outputFile.encryptingOutputFile());
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
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> input schema type required by the writer for data conversion
   * @param <D> the type of data records contained in the {@link PositionDelete} that the writer
   *     will accept
   * @return a configured delete write builder for creating a {@link PositionDeleteWriter}
   */
  public static <E, D> PositionDeleteWriteBuilder<?, E, D> positionDeleteWriteBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    ObjectModelFactory<E, D> factory = factoryFor(format, objectModelName);
    WriteBuilder<?, E, PositionDelete<D>> writeBuilder =
        factory.positionDeleteWriteBuilder(outputFile.encryptingOutputFile());
    return ContentFileWriteBuilderImpl.forPositionDelete(
        writeBuilder, outputFile.encryptingOutputFile().location(), format);
  }

  @SuppressWarnings("unchecked")
  private static <E, D> ObjectModelFactory<E, D> factoryFor(
      FileFormat format, String objectModelName) {
    return ((ObjectModelFactory<E, D>)
        OBJECT_MODEL_FACTORIES.get(Pair.of(format, objectModelName)));
  }
}
