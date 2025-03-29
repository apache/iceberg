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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.AppenderBuilder;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ObjectModel;
import org.apache.iceberg.io.ReadBuilder;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry which provides the available {@link ReadBuilder}s and writer builders ({@link
 * org.apache.iceberg.data.AppenderBuilder}, {@link DataWriterBuilder}, {@link
 * EqualityDeleteWriterBuilder}, {@link PositionDeleteWriterBuilder}). Based on the `file format`
 * and the requested `object model name` the registry returns the correct reader and writer
 * builders. These builders could be used to generate the readers and writers.
 *
 * <p>File formats has to register the {@link ReadBuilder}s and the {@link AppenderBuilder}s which
 * will be used to create the readers and the writers.
 */
public final class ObjectModelRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(ObjectModelRegistry.class);
  // The list of classes which are used for registering the reader and writer builders
  private static final List<String> CLASSES_TO_REGISTER =
      ImmutableList.of(
          "org.apache.iceberg.arrow.vectorized.ArrowReader",
          "org.apache.iceberg.flink.data.FlinkObjectModels",
          "org.apache.iceberg.spark.source.SparkObjectModels");

  private static final Map<Key, ObjectModel<?>> OBJECT_MODELS = Maps.newConcurrentMap();

  /**
   * Registers a new format/object model.
   *
   * @param objectModel the object model
   * @throws IllegalArgumentException if a read builder for the given {@code format} and {@code
   *     objectModelName} combination already exists
   */
  @SuppressWarnings("CatchBlockLogException")
  public static void registerObjectModel(ObjectModel<?> objectModel) {
    try {
      Key key = new Key(objectModel.format(), objectModel.name());
      if (OBJECT_MODELS.containsKey(key)) {
        throw new IllegalArgumentException(
            String.format(
                "Read builder %s clashes with %s. Both serves %s",
                objectModel.getClass(), OBJECT_MODELS.get(key), key));
      }

      OBJECT_MODELS.put(key, objectModel);
    } catch (RuntimeException e) {
      // failing to register readers/writers is normal and does not require a stack trace
      LOG.info(
          "Unable to use register object model {} for data files: {}", objectModel, e.getMessage());
    }
  }

  @SuppressWarnings("CatchBlockLogException")
  private static void registerSupportedFormats() {
    GenericObjectModels.register();

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

  private ObjectModelRegistry() {}

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
    return OBJECT_MODELS.get(new Key(format, objectModelName)).readBuilder(inputFile);
    //    return READ_BUILDERS.get(new Key(format, objectModelName)).apply(inputFile);
  }

  /**
   * Provides an appender builder for the given output file which writes objects with a given object
   * model name.
   *
   * @param format of the file to write
   * @param objectModelName accepted by the writer
   * @param outputFile to write
   * @param <E> type for the engine specific schema used by the builder
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <E> org.apache.iceberg.data.AppenderBuilder<?, E> appenderBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return writerFor(format, objectModelName, outputFile);
  }

  /**
   * Provides a data writer builder for the given output file which writes objects with a given
   * object model name.
   *
   * @param format of the file to write
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
   * Provides an equality delete writer builder for the given output file which writes objects with
   * a given object model name.
   *
   * @param format of the file to write
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
   * @param format of the file to write
   * @param objectModelName accepted by the writer
   * @param outputFile to write
   * @param <E> type for the engine specific schema used by the builder
   * @return {@link ReadBuilder} for building the actual reader
   */
  public static <E> PositionDeleteWriterBuilder<?, E> positionDeleteWriterBuilder(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return writerFor(format, objectModelName, outputFile);
  }

  @SuppressWarnings("unchecked")
  private static <B extends AppenderBuilder<B, E>, E> WriteBuilder<?, ?, E> writerFor(
      FileFormat format, String objectModelName, EncryptedOutputFile outputFile) {
    return new WriteBuilder<>(
        ((ObjectModel<E>) OBJECT_MODELS.get(new Key(format, objectModelName)))
            .<B>appenderBuilder(outputFile.encryptingOutputFile()),
        outputFile.encryptingOutputFile().location(),
        format);
  }

  /** Key used to identify readers and writers in the {@link ObjectModelRegistry}. */
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
