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

import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQueryOptions;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Default BigQuery client factory that uses application default credentials.
 *
 * <p>This implementation uses Google's Application Default Credentials (ADC), which automatically
 * discovers credentials from the environment without requiring explicit configuration.
 *
 * @see <a
 *     href="https://cloud.google.com/docs/authentication/application-default-credentials">Application
 *     Default Credentials</a>
 */
public class DefaultBigQueryClientFactory implements BigQueryClientFactory {
  private String projectId;
  private String location;

  private static final String DEFAULT_LOCATION = "us";

  @Override
  public void initialize(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Properties cannot be null");

    this.projectId = properties.get(BigQueryMetastoreCatalog.PROJECT_ID);
    Preconditions.checkNotNull(
        projectId,
        "Cannot initialize DefaultBigQueryClientFactory without project ID: %s is required",
        BigQueryMetastoreCatalog.PROJECT_ID);

    this.location =
        properties.getOrDefault(BigQueryMetastoreCatalog.GCP_LOCATION, DEFAULT_LOCATION);
  }

  @Override
  public BigQueryOptions bigQueryOptions() {
    return BigQueryOptions.newBuilder()
        .setProjectId(projectId)
        .setLocation(location)
        .setRetrySettings(ServiceOptions.getDefaultRetrySettings())
        .build();
  }
}
