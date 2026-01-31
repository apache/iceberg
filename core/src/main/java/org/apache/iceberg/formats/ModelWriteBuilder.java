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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;

/**
 * Builder interface for creating file writers across supported data file formats. The {@link
 * FormatModel} implementations provide the appropriate {@link ModelWriteBuilder} instances.
 *
 * <p>The {@link ModelWriteBuilder} follows the builder pattern to configure and create {@link
 * FileAppender} instances that write data to the target output files.
 *
 * <p>This interface is directly exposed to users for parameterizing when only an appender is
 * required.
 *
 * @param <D> the output data type produced by the reader
 * @param <S> the type of the schema for the output data type
 */
public interface ModelWriteBuilder<D, S> {
  /** Set the file schema. */
  ModelWriteBuilder<D, S> schema(Schema schema);

  /**
   * Sets the engine's representation accepted by the writer.
   *
   * <p>Some data types require additional type information from the engine schema that cannot be
   * fully expressed by the Iceberg schema or the data itself. For example, a variant type may use a
   * shredded representation that relies on engine-specific metadata to map back to the Iceberg
   * schema.
   *
   * <p>The engine schema must be aligned with the Iceberg schema, but may include representation
   * details that Iceberg considers equivalent.
   */
  ModelWriteBuilder<D, S> engineSchema(S schema);

  /**
   * Set a writer configuration property which affects the writer behavior. Writer builders should
   * ignore configuration keys not known for them.
   *
   * @param property a writer config property name
   * @param value config value
   * @return this for method chaining
   */
  ModelWriteBuilder<D, S> set(String property, String value);

  /**
   * Sets multiple writer configuration properties that affect the writer behavior. Writer builders
   * should ignore configuration keys not known for them.
   *
   * @param properties writer config properties to set
   * @return this for method chaining
   */
  default ModelWriteBuilder<D, S> setAll(Map<String, String> properties) {
    properties.forEach(this::set);
    return this;
  }

  /**
   * Set a file metadata property in the created file.
   *
   * @param property a file metadata property name
   * @param value config value
   * @return this for method chaining
   */
  ModelWriteBuilder<D, S> meta(String property, String value);

  /**
   * Sets multiple file metadata properties in the created file.
   *
   * @param properties file metadata properties to set
   * @return this for method chaining
   */
  default ModelWriteBuilder<D, S> meta(Map<String, String> properties) {
    properties.forEach(this::meta);
    return this;
  }

  /**
   * Based on the target file content the generated {@link FileAppender} needs different
   * configuration.
   */
  ModelWriteBuilder<D, S> content(FileContent content);

  /** Sets the metrics configuration used for collecting column metrics for the created file. */
  ModelWriteBuilder<D, S> metricsConfig(MetricsConfig metricsConfig);

  /** Overwrite the file if it already exists. By default, overwrite is disabled. */
  ModelWriteBuilder<D, S> overwrite();

  /**
   * Sets the encryption key used for writing the file. If the writer does not support encryption,
   * then an exception should be thrown.
   */
  ModelWriteBuilder<D, S> withFileEncryptionKey(ByteBuffer encryptionKey);

  /**
   * Sets the additional authentication data (AAD) prefix used for writing the file. If the writer
   * does not support encryption, then an exception should be thrown.
   */
  ModelWriteBuilder<D, S> withAADPrefix(ByteBuffer aadPrefix);

  /** Finalizes the configuration and builds the {@link FileAppender}. */
  FileAppender<D> build() throws IOException;
}
