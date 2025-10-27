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
 * FormatModel} implementations provide the appropriate {@link WriteBuilder} instances.
 *
 * <p>The {@link WriteBuilder} follows the builder pattern to configure and create {@link
 * FileAppender} instances that write data to the target output files.
 *
 * <p>This interface is directly exposed to users for parameterizing when only an appender is
 * required.
 */
public interface WriteBuilder {
  /** Set the file schema. */
  WriteBuilder schema(Schema schema);

  /**
   * Set a writer configuration property which affects the writer behavior.
   *
   * @param property a writer config property name
   * @param value config value
   * @return this for method chaining
   */
  WriteBuilder set(String property, String value);

  /**
   * Sets multiple writer configuration properties that affect the writer behavior.
   *
   * @param properties writer config properties to set
   * @return this for method chaining
   */
  default WriteBuilder setAll(Map<String, String> properties) {
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
  WriteBuilder meta(String property, String value);

  /**
   * Sets multiple file metadata properties in the created file.
   *
   * @param properties file metadata properties to set
   * @return this for method chaining
   */
  default WriteBuilder meta(Map<String, String> properties) {
    properties.forEach(this::meta);
    return this;
  }

  /**
   * Based on the target file content the generated {@link FileAppender} needs different
   * configuration.
   */
  WriteBuilder content(FileContent content);

  /** Sets the metrics configuration used for collecting column metrics for the created file. */
  WriteBuilder metricsConfig(MetricsConfig metricsConfig);

  /** Overwrite the file if it already exists. By default, overwrite is disabled. */
  WriteBuilder overwrite();

  /**
   * Sets the encryption key used for writing the file. If the writer does not support encryption,
   * then an exception should be thrown.
   */
  WriteBuilder withFileEncryptionKey(ByteBuffer encryptionKey);

  /**
   * Sets the additional authentication data (AAD) prefix used for writing the file. If the reader
   * does not support encryption, then an exception should be thrown.
   */
  WriteBuilder withAADPrefix(ByteBuffer aadPrefix);

  /** Finalizes the configuration and builds the {@link FileAppender}. */
  <D> FileAppender<D> build() throws IOException;
}
