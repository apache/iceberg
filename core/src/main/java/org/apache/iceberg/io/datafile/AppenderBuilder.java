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
import java.util.Map;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileAppender;

/** Builder API for creating {@link FileAppender}s. */
public interface AppenderBuilder extends InternalData.WriteBuilder {
  /** Sets the {@link Table} specific parameters like schema, spec, etc. all at once. */
  AppenderBuilder forTable(Table table);

  /** Sets the write schema for the writer. */
  @Override
  AppenderBuilder schema(Schema schema);

  /** Sets configuration key/value pairs for the writer. */
  @Override
  AppenderBuilder set(String property, String value);

  AppenderBuilder setAll(Map<String, String> properties);

  /** Sets the file metadata kep/value pairs for the writer which should be written to the file. */
  @Override
  AppenderBuilder meta(String property, String value);

  @Override
  default AppenderBuilder meta(Map<String, String> properties) {
    properties.forEach(this::meta);
    return this;
  }

  /** Enables overwriting previously created files. */
  @Override
  AppenderBuilder overwrite();

  AppenderBuilder overwrite(boolean enabled);

  /**
   * Sets the configuration for collecting file metrics. Writers should provide metrics for metadata
   * based on this configuration.
   */
  AppenderBuilder metricsConfig(MetricsConfig newMetricsConfig);

  /**
   * Sets the name of the generated {@link FileAppender}. Currently only Avro specific for mapping
   * the schema to the table name.
   */
  @Override
  default AppenderBuilder named(String newName) {
    throw new UnsupportedOperationException("Not supported");
  }

  default AppenderBuilder withFileEncryptionKey(ByteBuffer fileEncryptionKey) {
    throw new UnsupportedOperationException("Not supported");
  }

  default AppenderBuilder withAADPrefix(ByteBuffer aadPrefix) {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  <D> FileAppender<D> build() throws IOException;
}
