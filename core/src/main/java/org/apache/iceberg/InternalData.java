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
package org.apache.iceberg;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.InternalReader;
import org.apache.iceberg.avro.InternalWriter;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalData {
  private InternalData() {}

  private static final Logger LOG = LoggerFactory.getLogger(InternalData.class);
  private static final Map<FileFormat, Function<OutputFile, WriteBuilder>> WRITE_BUILDERS =
      Maps.newConcurrentMap();
  private static final Map<FileFormat, Function<InputFile, ReadBuilder>> READ_BUILDERS =
      Maps.newConcurrentMap();

  static void register(
      FileFormat format,
      Function<OutputFile, WriteBuilder> writeBuilder,
      Function<InputFile, ReadBuilder> readBuilder) {
    WRITE_BUILDERS.put(format, writeBuilder);
    READ_BUILDERS.put(format, readBuilder);
  }

  @SuppressWarnings("CatchBlockLogException")
  private static void registerSupportedFormats() {
    InternalData.register(
        FileFormat.AVRO,
        outputFile -> Avro.write(outputFile).createWriterFunc(InternalWriter::create),
        inputFile -> Avro.read(inputFile).createResolvingReader(InternalReader::create));

    try {
      DynMethods.StaticMethod registerParquet =
          DynMethods.builder("register")
              .impl("org.apache.iceberg.InternalParquet")
              .buildStaticChecked();

      registerParquet.invoke();

    } catch (NoSuchMethodException e) {
      // failing to load Parquet is normal and does not require a stack trace
      LOG.info("Unable to register Parquet for metadata files: {}", e.getMessage());
    }
  }

  static {
    registerSupportedFormats();
  }

  public static WriteBuilder write(FileFormat format, OutputFile file) {
    Function<OutputFile, WriteBuilder> writeBuilder = WRITE_BUILDERS.get(format);
    if (writeBuilder != null) {
      return writeBuilder.apply(file);
    }

    throw new UnsupportedOperationException(
        "Cannot write using unregistered internal data format: " + format);
  }

  public static ReadBuilder read(FileFormat format, InputFile file) {
    Function<InputFile, ReadBuilder> readBuilder = READ_BUILDERS.get(format);
    if (readBuilder != null) {
      return readBuilder.apply(file);
    }

    throw new UnsupportedOperationException(
        "Cannot read using unregistered internal data format: " + format);
  }

  public interface WriteBuilder {
    /** Set the file schema. */
    WriteBuilder schema(Schema schema);

    /** Set the file schema's root name. */
    WriteBuilder named(String name);

    /**
     * Set a writer configuration property.
     *
     * <p>Write configuration affects writer behavior. To add file metadata properties, use {@link
     * #meta(String, String)}.
     *
     * @param property a writer config property name
     * @param value config value
     * @return this for method chaining
     */
    WriteBuilder set(String property, String value);

    /**
     * Set a writer configuration property.
     *
     * <p>Write configuration affects writer behavior. To add file metadata properties, use {@link
     * #meta(String, String)}.
     *
     * @param config a writer config
     * @return this for method chaining
     */
    default WriteBuilder setAll(Map<String, String> config) {
      config.forEach(this::set);
      return this;
    }

    /**
     * Set a file metadata property.
     *
     * <p>Metadata properties are written into file metadata. To alter a writer configuration
     * property, use {@link #set(String, String)}.
     *
     * @param property a file metadata property name
     * @param value config value
     * @return this for method chaining
     */
    WriteBuilder meta(String property, String value);

    /**
     * Set a file metadata properties from a Map.
     *
     * <p>Metadata properties are written into file metadata. To alter a writer configuration
     * property, use {@link #set(String, String)}.
     *
     * @param properties a map of file metadata properties
     * @return this for method chaining
     */
    default WriteBuilder meta(Map<String, String> properties) {
      properties.forEach(this::meta);
      return this;
    }

    /** Overwrite the file if it already exists. */
    WriteBuilder overwrite();

    /**
     * Set the metrics config
     *
     * @param newMetricsConfig The metrics config to set
     * @return this for method chaining
     */
    default WriteBuilder metricsConfig(MetricsConfig newMetricsConfig) {
      throw new UnsupportedOperationException("metricsConfig() not supported");
    }

    /** Build the configured {@link FileAppender}. */
    <D> FileAppender<D> build() throws IOException;
  }

  public interface ReadBuilder {
    /** Set the projection schema. */
    ReadBuilder project(Schema projectedSchema);

    /** Read only the split that is {@code length} bytes starting at {@code start}. */
    ReadBuilder split(long newStart, long newLength);

    /** Reuse container classes, like structs, lists, and maps. */
    ReadBuilder reuseContainers();

    /** Set a custom class for in-memory objects at the schema root. */
    ReadBuilder setRootType(Class<? extends StructLike> rootClass);

    /** Set a custom class for in-memory objects at the given field ID. */
    ReadBuilder setCustomType(int fieldId, Class<? extends StructLike> structClass);

    /** Build the configured reader. */
    <D> CloseableIterable<D> build();
  }
}
