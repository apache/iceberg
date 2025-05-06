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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.AppenderBuilder;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;

/**
 * Builder for generating one of the following:
 *
 * <ul>
 *   <li>{@link FileAppender}
 *   <li>{@link DataWriter}
 *   <li>{@link EqualityDeleteWriter}
 *   <li>{@link PositionDeleteWriter}
 * </ul>
 *
 * @param <B> type of the builder
 * @param <E> engine specific schema of the input records used for appender initialization
 */
interface WriterBuilderBase<B extends WriterBuilderBase<B, E>, E> {

  /** Set the file schema. */
  B schema(Schema newSchema);

  /**
   * Sets the engine specific schema for the input. Used by the {@link AppenderBuilder#build()} to
   * configure the engine specific converters.
   */
  B engineSchema(E engineSchema);

  /**
   * Set a writer configuration property which affects the writer behavior.
   *
   * @param property a writer config property name
   * @param value config value
   * @return this for method chaining
   */
  B set(String property, String value);

  /**
   * Adds the new properties to the writer configuration.
   *
   * @param properties a map of writer config properties
   * @return this for method chaining
   */
  B set(Map<String, String> properties);

  /**
   * Set a file metadata property in the created file.
   *
   * @param property a file metadata property name
   * @param value config value
   * @return this for method chaining
   */
  B meta(String property, String value);

  /**
   * Add the new properties to file metadata for the created file.
   *
   * @param properties a map of file metadata properties
   * @return this for method chaining
   */
  B meta(Map<String, String> properties);

  /** Sets the metrics configuration used for collecting column metrics for the created file. */
  B metricsConfig(MetricsConfig newMetricsConfig);

  /** Overwrite the file if it already exists. By default, overwrite is disabled. */
  B overwrite();

  /**
   * Sets the encryption key used for writing the file. If encryption is not supported by the writer
   * then an exception should be thrown.
   */
  B fileEncryptionKey(ByteBuffer encryptionKey);

  /**
   * Sets the additional authentication data prefix used for writing the file. If encryption is not
   * supported by the writer then an exception should be thrown.
   */
  B aadPrefix(ByteBuffer aadPrefix);
}
