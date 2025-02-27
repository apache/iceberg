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
import java.util.function.BiConsumer;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileAppender;

/**
 * Interface which should be implemented by the data file format implementations.
 *
 * @param <A> type returned by builder API to allow chained calls
 */
public interface AppenderBuilder<A extends AppenderBuilder<A>> {
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

  /** Overwrite the file if it already exists. */
  A overwrite(boolean enabled);

  /** Sets the encryption key used for writing the file. */
  default A fileEncryptionKey(ByteBuffer encryptionKey) {
    throw new UnsupportedOperationException("Not supported");
  }

  /** Sets the additional authentication data prefix used for writing the file. */
  default A aADPrefix(ByteBuffer aadPrefix) {
    throw new UnsupportedOperationException("Not supported");
  }

  /** Build the configured {@link FileAppender}. */
  <I> FileAppender<I> build() throws IOException;

  /**
   * Functional interface to build initializer functions which for the given {@link WriteMode}s. The
   * method is used by the {@link WriteBuilder} to execute engine specific initializations for the
   * appender.
   */
  interface Initializer {
    <A extends AppenderBuilder<A>, T> BiConsumer<AppenderBuilder<A>, T> buildInitializer(
        WriteMode mode);
  }

  enum WriteMode {
    APPENDER,
    DATA_WRITER,
    EQUALITY_DELETE_WRITER,
    POSITION_DELETE_WRITER,
    POSITION_DELETE_WITH_ROW_WRITER,
  }
}
