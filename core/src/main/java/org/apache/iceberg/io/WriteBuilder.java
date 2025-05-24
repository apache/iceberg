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
import org.apache.iceberg.FileContent;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;

/**
 * Builder interface for creating file writers across supported data file formats. Each {@link
 * FileAccessFactory} implementation provides appropriate {@link WriteBuilder} instances based on:
 *
 * <ul>
 *   <li>target file format (Parquet, Avro, ORC)
 *   <li>input data object model (spark, flink, generic, etc.)
 *   <li>content type ({@link FileContent#DATA}, {@link FileContent#EQUALITY_DELETES}, {@link
 *       FileContent#POSITION_DELETES})
 * </ul>
 *
 * The {@link WriteBuilder} follows the builder pattern to configure and create {@link FileAppender}
 * instances that write data to the target output files.
 *
 * @param <B> the concrete builder type for method chaining
 * @param <E> schema type for the input data records
 */
public interface WriteBuilder<B extends WriteBuilder<B, E>, E> {
  /** Set the file schema. */
  B fileSchema(Schema newSchema);

  /**
   * Set a writer configuration property which affects the writer behavior.
   *
   * @param property a writer config property name
   * @param value config value
   * @return this for method chaining
   */
  B set(String property, String value);

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

  /** Sets the metrics configuration used for collecting column metrics for the created file. */
  B metricsConfig(MetricsConfig newMetricsConfig);

  /** Overwrite the file if it already exists. By default, overwrite is disabled. */
  B overwrite();

  /**
   * Overwrite the file if it already exists. The default value is <code>false</code>.
   *
   * @deprecated Since 1.10.0, will be removed in 1.11.0. Only provided for backward compatibility.
   *     Use {@link #overwrite()} instead.
   */
  @Deprecated
  B overwrite(boolean enabled);

  /**
   * Sets the encryption key used for writing the file. If the writer does not support encryption,
   * then an exception should be thrown.
   */
  default B fileEncryptionKey(ByteBuffer encryptionKey) {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Sets the additional authentication data (AAD) prefix used for writing the file. If the reader
   * does not support encryption, then an exception should be thrown.
   */
  default B fileAADPrefix(ByteBuffer aadPrefix) {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Sets the schema for the input data records.
   *
   * <p>This method is necessary when the mapping between input types and Iceberg types is not
   * one-to-one. For example, when multiple input types could map to the same Iceberg type, or when
   * schema metadata beyond the structure is needed to properly interpret the data.
   *
   * <p>While the Iceberg schema defines the expected output structure, the input schema provides
   * the exact input format details needed for proper type conversion.
   *
   * @param newDataSchema the native schema representation from the input (Spark, Flink, etc.)
   * @return this builder for method chaining
   */
  B dataSchema(E newDataSchema);

  /** Finalizes the configuration and builds the {@link FileAppender}. */
  <D> FileAppender<D> build() throws IOException;
}
