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

import java.util.Map;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

/**
 * Common builder parameters for writing Iceberg data and delete files.
 *
 * @param <T> the type of the builder for chaining
 */
interface WriterBuilder<T extends WriterBuilder<T>> {
  /** Sets the {@link Table} specific parameters like schema, spec, etc. all at once. */
  T forTable(Table table);

  /** Sets the write schema for the writer. */
  T schema(Schema schema);

  /** Sets configuration key/value pairs for the writer. */
  T set(String property, String value);

  T setAll(Map<String, String> properties);

  /** Sets the file metadata kep/value pairs for the writer which should be written to the file. */
  T meta(String property, String value);

  T meta(Map<String, String> properties);

  /** Enables overwriting previously created files. */
  T overwrite();

  T overwrite(boolean enabled);

  /**
   * Sets the configuration for collecting file metrics. Writers should provide metrics for metadata
   * based on this configuration.
   */
  T metricsConfig(MetricsConfig newMetricsConfig);

  /** Returns the location of the generated file. */
  String location();

  /** Returns the write schema. */
  Schema schema();
}
