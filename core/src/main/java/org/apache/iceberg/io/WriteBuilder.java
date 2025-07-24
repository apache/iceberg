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
package org.apache.iceberg.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;

/**
 * Builder interface for creating file writers across supported data file formats. The {@link
 * FormatModel} implementations provide the appropriate {@link WriteBuilder} instances.
 *
 * <p>The {@link WriteBuilder} follows the builder pattern to configure and create {@link
 * FileAppender} instances that write data to the target output files.
 *
 * <p>This interface is directly exposed to users for parameterizing when only an appender is
 * required.
 *
 * @param <B> the concrete builder type for method chaining
 * @param <D> the input data type for the writer
 */
public interface WriteBuilder<B extends WriteBuilder<B, D>, D> {
  /** Set the file schema. */
  B fileSchema(Schema fileSchema);

  /**
   * Set a writer configuration property which affects the writer behavior.
   *
   * @param property a writer config property name
   * @param value config value
   * @return this for method chaining
   */
  B set(String property, String value);

  /**
   * Sets multiple writer configuration properties that affect the writer behavior.
   *
   * @param properties writer config properties to set
   * @return this for method chaining
   */
  @SuppressWarnings("unchecked")
  default B set(Map<String, String> properties) {
    properties.forEach(this::set);
    return (B) this;
  }

  /**
   * Set a file metadata property in the created file.
   *
   * @param property a file metadata property name
   * @param value config value
   * @return this for method chaining
   */
  B meta(String property, String value);

  /**
   * Sets multiple file metadata properties in the created file.
   *
   * @param properties file metadata properties to set
   * @return this for method chaining
   */
  @SuppressWarnings("unchecked")
  default B meta(Map<String, String> properties) {
    properties.forEach(this::meta);
    return (B) this;
  }

  /** Sets the metrics configuration used for collecting column metrics for the created file. */
  B metricsConfig(MetricsConfig metricsConfig);

  /** Overwrite the file if it already exists. By default, overwrite is disabled. */
  B overwrite();

  /**
   * Sets the encryption key used for writing the file. If the writer does not support encryption,
   * then an exception should be thrown.
   */
  B fileEncryptionKey(ByteBuffer encryptionKey);

  /**
   * Sets the additional authentication data (AAD) prefix used for writing the file. If the reader
   * does not support encryption, then an exception should be thrown.
   */
  B fileAADPrefix(ByteBuffer aadPrefix);

  /** Finalizes the configuration and builds the {@link FileAppender}. */
  FileAppender<D> build() throws IOException;
}
