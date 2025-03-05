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
package org.apache.iceberg.io.datafile;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileAppender;

/**
 * Interface which should be implemented by the data file format implementations.
 *
 * @param <A> type returned by builder API to allow chained calls
 * @param <E> type for the engine specific schema
 */
public interface AppenderBuilder<A extends AppenderBuilder<A, E>, E> {
  /**
   * Sets the appender configurations coming from the table like {@link #schema(Schema)}, {@link
   * #set(String, String)} and {@link #metricsConfig(MetricsConfig)}.
   */
  A forTable(Table table);

  /** Set the file schema. */
  A schema(Schema newSchema);

  /** Set the file schema's root name. */
  default A named(String newName) {
    throw new UnsupportedOperationException("Not supported");
  }

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
  A set(String property, String value);

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
  A meta(String property, String value);

  /** Sets the metrics configuration used for collecting column metrics for the created file. */
  A metricsConfig(MetricsConfig newMetricsConfig);

  /** Overwrite the file if it already exists. The default value is <code>false</code>. */
  A overwrite(boolean enabled);

  /**
   * Sets the encryption key used for writing the file. If encryption is not supported by the reader
   * then an exception should be thrown.
   */
  default A fileEncryptionKey(ByteBuffer encryptionKey) {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Sets the additional authentication data prefix used for writing the file. If encryption is not
   * supported by the reader then an exception should be thrown.
   */
  default A aADPrefix(ByteBuffer aadPrefix) {
    throw new UnsupportedOperationException("Not supported");
  }

  /** Sets the engine native schema for the appender. */
  E engineSchema(E newEngineSchema);

  /**
   * Builds the {@link FileAppender} for the configured {@link WriteMode}. Could change several
   * use-case specific configurations, like:
   *
   * <ul>
   *   <li>Mode specific writer context (typically different for data and delete files).
   *   <li>Writer functions to accept data rows, or {@link
   *       org.apache.iceberg.deletes.PositionDelete}s
   * </ul>
   */
  <D> FileAppender<D> build(WriteMode mode) throws IOException;

  /**
   * Writer modes. Based on the mode {@link #build(WriteMode)} could alter the appender
   * configuration when creating the {@link FileAppender}.
   */
  enum WriteMode {
    APPENDER,
    DATA_WRITER,
    EQUALITY_DELETE_WRITER,
    POSITION_DELETE_WRITER,
    POSITION_DELETE_WITH_ROW_WRITER,
  }
}
