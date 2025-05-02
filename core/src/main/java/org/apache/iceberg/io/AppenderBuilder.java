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
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;

/**
 * Interface which should be implemented by the data file format implementations. The {@link
 * AppenderBuilder} will be parametrized based on the user provided configuration and finally the
 * {@link AppenderBuilder#build(AppenderBuilder.WriteMode)} method is used to generate the appender
 * for the specific writer use-cases. The following input should be handled by the appender in the
 * specific modes:
 *
 * <ul>
 *   <li>The appender's engine specific input type
 *       <ul>
 *         <li>{@link AppenderBuilder.WriteMode#DATA_WRITER}
 *         <li>{@link AppenderBuilder.WriteMode#EQUALITY_DELETE_WRITER}
 *       </ul>
 *   <li>{@link org.apache.iceberg.deletes.PositionDelete} where the type of the row is the
 *       appender's engine specific input type
 *       <ul>
 *         <li>{@link AppenderBuilder.WriteMode#POSITION_DELETE_WRITER}
 *         <li>{@link AppenderBuilder.WriteMode#POSITION_DELETE_WITH_ROW_WRITER}
 *       </ul>
 * </ul>
 *
 * @param <B> type returned by builder API to allow chained calls
 * @param <E> the engine specific schema of the input data
 */
public interface AppenderBuilder<B extends AppenderBuilder<B, E>, E> {
  /** Set the file schema. */
  B schema(Schema newSchema);

  /**
   * Set a writer configuration property which affects the writer behavior.
   *
   * @param property a writer config property name
   * @param value config value
   * @return this for method chaining
   */
  B set(String property, String value);

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
   * Sets the encryption key used for writing the file. If encryption is not supported by the reader
   * then an exception should be thrown.
   */
  default B fileEncryptionKey(ByteBuffer encryptionKey) {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Sets the additional authentication data prefix used for writing the file. If encryption is not
   * supported by the reader then an exception should be thrown.
   */
  default B aadPrefix(ByteBuffer aadPrefix) {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Sets the engine native schema for the input. Defines the input type when there is N to 1
   * mapping between the engine type and the Iceberg type, and providing the Iceberg schema is not
   * enough for the conversion.
   */
  B engineSchema(E newEngineSchema);

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
    /** Mode for writing data files. */
    DATA_WRITER,
    /** Mode for writing equality delete files. */
    EQUALITY_DELETE_WRITER,
    /** Mode for writing position delete files. */
    POSITION_DELETE_WRITER,
    /** Mode for writing position delete files with row data. */
    POSITION_DELETE_WITH_ROW_WRITER
  }
}
