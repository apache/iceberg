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
package org.apache.iceberg.gcp.bigquery;

import com.google.cloud.bigquery.BigQueryOptions;
import java.io.Serializable;
import java.util.Map;

/**
 * Factory for creating configured BigQueryOptions (pluggable auth). Implementations create
 * BigQueryOptions from catalog properties.
 */
public interface BigQueryClientFactory extends Serializable {

  /**
   * Initializes the factory with catalog configuration properties.
   *
   * @param properties configuration properties
   * @throws IllegalArgumentException if required properties are missing or invalid
   */
  void initialize(Map<String, String> properties);

  /**
   * Creates BigQuery options with credentials and settings from {@link #initialize(Map)}.
   *
   * @return configured BigQuery options
   * @throws RuntimeException if options cannot be created (e.g., authentication failure)
   */
  BigQueryOptions bigQueryOptions();
}
