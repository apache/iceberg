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
 * Factory interface for creating BigQuery client configurations.
 *
 * <p>Implementations of this interface provide different authentication strategies for accessing
 * BigQuery APIs, such as application default credentials or service account impersonation.
 *
 * <p>This factory pattern allows users to choose their authentication mechanism by specifying the
 * factory implementation class in the catalog configuration:
 *
 * <pre>{@code
 * // Using default credentials
 * properties.put("gcp.bigquery.client.factory",
 *     "org.apache.iceberg.gcp.bigquery.DefaultBigQueryClientFactory");
 *
 * // Using service account impersonation
 * properties.put("gcp.bigquery.client.factory",
 *     "org.apache.iceberg.gcp.bigquery.ImpersonatedBigQueryClientFactory");
 * properties.put("gcp.impersonate.service-account", "target-sa@project.iam.gserviceaccount.com");
 * }</pre>
 *
 * <p><b>Available Implementations:</b>
 *
 * <ul>
 *   <li>{@link DefaultBigQueryClientFactory} - Uses Application Default Credentials
 *   <li>{@link ImpersonatedBigQueryClientFactory} - Uses service account impersonation
 * </ul>
 *
 * @see <a href="https://cloud.google.com/bigquery/docs/authentication">BigQuery Authentication</a>
 * @see <a href="https://cloud.google.com/iam/docs/impersonating-service-accounts">Impersonating
 *     Service Accounts</a>
 */
public interface BigQueryClientFactory extends Serializable {

  /**
   * Initializes the factory with configuration properties.
   *
   * <p>This method is called once during catalog initialization and must validate that all required
   * properties are present. The method should store the necessary configuration for later use when
   * creating BigQuery options.
   *
   * <p>Common properties include:
   *
   * <ul>
   *   <li><code>gcp.bigquery.project-id</code> - The GCP project ID (required)
   *   <li><code>gcp.bigquery.location</code> - The BigQuery location (optional, default: "us")
   *   <li>Additional authentication-specific properties depending on the implementation
   * </ul>
   *
   * @param properties configuration properties from the catalog
   * @throws NullPointerException if properties is null or required properties are missing
   * @throws IllegalArgumentException if properties contain invalid values
   */
  void initialize(Map<String, String> properties);

  /**
   * Creates and returns configured BigQuery options.
   *
   * <p>This method constructs a {@link BigQueryOptions} instance with the appropriate credentials,
   * project ID, location, and other settings based on the configuration provided during {@link
   * #initialize(Map)}.
   *
   * <p>The returned options are used to create BigQuery clients for metadata operations. The method
   * may be called multiple times and should return consistent configurations.
   *
   * <p>Implementations should handle credential refresh and token management as needed for their
   * authentication mechanism.
   *
   * @return configured BigQuery options ready for creating BigQuery clients
   * @throws RuntimeException if BigQuery options cannot be created (e.g., authentication failure)
   */
  BigQueryOptions bigQueryOptions();
}
