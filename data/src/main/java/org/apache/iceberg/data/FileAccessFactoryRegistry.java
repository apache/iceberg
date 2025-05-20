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
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAccessFactory;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ReadBuilder;
import org.apache.iceberg.io.WriteBuilder;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A registry that manages file-format-specific readers and writers through a unified file access
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
 * <p>File access factories are registered through {@link
 * #registerFileAccessFactory(FileAccessFactory)} and used for creating readers and writers. Read
 * builders are returned directly from the factory, while write builders may be wrapped in
 * specialized content file writer implementations depending on the requested operation type.
 */
public final class FileAccessFactoryRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(FileAccessFactoryRegistry.class);
  // The list of classes which are used for registering the reader and writer builders
  private static final List<String> CLASSES_TO_REGISTER = ImmutableList.of();

  private static final Map<Key, FileAccessFactory<?>> FILE_ACCESS_FACTORIES =
      Maps.newConcurrentMap();

  /**
   * Registers a file access factory with this registry.
   *
   * <p>File access factories create readers and writers for specific combinations of file formats
   * (Parquet, ORC, Avro) and object models ("generic", "spark", "flink", etc.). Registering custom
   * factories allows integration of new data processing engines for the supported file formats with
   * Iceberg's file access mechanisms.
   *
   * <p>Each factory must be uniquely identified by its combination of file format and object model
   * name. This uniqueness constraint prevents ambiguity when selecting factories for read and write
   * operations.
   *
   * @param fileAccessFactory the factory implementation to register
   * @throws IllegalArgumentException if a factory is already registered for the combination of
   *     {@link FileAccessFactory#format()} and {@link FileAccessFactory#objectModeName()}
   */
  @SuppressWarnings("CatchBlockLogException")
  public static void registerFileAccessFactory(FileAccessFactory<?> fileAccessFactory) {
    Key key = new Key(fileAccessFactory.format(), fileAccessFactory.objectModeName());
    if (FILE_ACCESS_FACTORIES.containsKey(key)) {
      throw new IllegalArgumentException(
          String.format(
              "Object model %s clashes with %s. Both serves %s",
              fileAccessFactory.getClass(), FILE_ACCESS_FACTORIES.get(key), key));
    }

    FILE_ACCESS_FACTORIES.put(key, fileAccessFactory);
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

  private FileAccessFactoryRegistry() {}

  /**
   * Returns a reader builder for the specified file format and object model.
   *
   * <p>The returned {@link ReadBuilder} provides a fluent interface for configuring how data is
   * read from the input file and converted to engine-specific objects. The builder supports
   * configuration options like schema projection, predicate pushdown, batch size and encryption.
   *
   * @param format the file format (Parquet, Avro, ORC) that determines the parsing implementation
   * @param objectModelName identifier for the engine-specific data representation (generic, spark,
   *     flink, etc.)
   * @param inputFile source file to read data from
   * @return a configured reader builder for the specified format and object model
   */
  public static ReadBuilder<?> readBuilder(
      FileFormat format, String objectModelName, InputFile inputFile) {
    return FILE_ACCESS_FACTORIES.get(new Key(format, objectModelName)).readBuilder(inputFile);
  }

  /**
   * Returns a writer builder for appending data to the specified output file.
   *
   * <p>The returned builder produces a {@link FileAppender} that accepts records defined by the
   * specified object model and persists them using the given file format. While data is written to
   * the output file, this basic writer does not collect or return {@link ContentFile} metadata.
   *
   * @param format the file format used for writing
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> type of the engine-specific schema expected by the writer
   * @return a configured writer builder for creating the appender
   */
  public static <E> WriteBuilder<?, E> writeBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return ((FileAccessFactory<E>) FILE_ACCESS_FACTORIES.get(new Key(format, objectModelName)))
        .writeBuilder(outputFile.encryptingOutputFile(), FileContent.DATA);
  }

  /**
   * Returns a writer builder for generating a {@link DataFile}.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the provided file format. Unlike basic writers, these writers
   * collect file metadata during the writing process and generate a {@link DataFile} that can be
   * used for table operations.
   *
   * @param format the file format used for writing
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> type of the engine-specific schema expected by the writer
   * @return a configured data write builder for creating a {@link DataWriter}
   */
  public static <E> DataWriteBuilder<?, E> dataWriteBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return writeBuilderFor(format, objectModelName, outputFile, FileContent.DATA);
  }

  /**
   * Creates a writer builder for generating a {@link DeleteFile} with equality deletes.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the given file format. These specialized writers collect equality
   * delete records that identify rows to be deleted based on equality conditions, producing a
   * {@link DeleteFile} that can be used for table operations.
   *
   * @param format the file format used for writing
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> type of the engine-specific schema expected by the writer
   * @return a configured delete write builder for creating an {@link EqualityDeleteWriter}
   */
  public static <E> EqualityDeleteWriteBuilder<?, E> equalityDeleteWriteBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return writeBuilderFor(format, objectModelName, outputFile, FileContent.EQUALITY_DELETES);
  }

  /**
   * Creates a writer builder for generating a {@link DeleteFile} with position-based deletes.
   *
   * <p>The returned builder produces a writer that accepts records defined by the specified object
   * model and persists them using the given file format. These specialized writers collect {@link
   * PositionDelete} records that identify rows to be deleted by file path and position, producing a
   * {@link DeleteFile} that can be used for table operations.
   *
   * @param format the file format used for writing
   * @param objectModelName name of the object model defining the input format
   * @param outputFile destination for the written data
   * @param <E> type of the engine-specific schema expected by the writer
   * @return a configured delete write builder for creating a {@link PositionDeleteWriter}
   */
  public static <E> PositionDeleteWriteBuilder<?, E> positionDeleteWriteBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return writeBuilderFor(format, objectModelName, outputFile, FileContent.POSITION_DELETES);
  }

  @SuppressWarnings("unchecked")
  private static <B extends WriteBuilder<B, E>, E>
      ContentFileWriteBuilderImpl<?, ?, E> writeBuilderFor(
          FileFormat format,
          String objectModelName,
          EncryptedOutputFile outputFile,
          FileContent content) {
    return new ContentFileWriteBuilderImpl<>(
        ((FileAccessFactory<E>) FILE_ACCESS_FACTORIES.get(new Key(format, objectModelName)))
            .<B>writeBuilder(outputFile.encryptingOutputFile(), content),
        outputFile.encryptingOutputFile().location(),
        format);
  }

  /** Key used to identify readers and writers in the {@link FileAccessFactoryRegistry}. */
  private static class Key {
    private final FileFormat fileFormat;
    private final String objectModelName;

    private Key(FileFormat fileFormat, String objectModelName) {
      this.fileFormat = fileFormat;
      this.objectModelName = objectModelName;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("fileFormat", fileFormat)
          .add("objectModelName", objectModelName)
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
          && Objects.equal(other.objectModelName, objectModelName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(fileFormat, objectModelName);
    }
  }
}
