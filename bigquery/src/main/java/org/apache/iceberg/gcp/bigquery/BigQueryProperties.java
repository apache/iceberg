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

import com.google.api.services.bigquery.BigqueryScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQueryOptions;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigQueryProperties implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryProperties.class);

  // User provided properties.
  public static final String PROJECT_ID = "gcp.bigquery.project-id";
  public static final String GCP_LOCATION = "gcp.bigquery.location";
  public static final String LIST_ALL_TABLES = "gcp.bigquery.list-all-tables";

  // Service account impersonation properties.
  public static final String IMPERSONATE_SERVICE_ACCOUNT =
      "gcp.bigquery.impersonate.service-account";
  public static final String IMPERSONATE_LIFETIME_SECONDS =
      "gcp.bigquery.impersonate.lifetime-seconds";
  public static final String IMPERSONATE_SCOPES = "gcp.bigquery.impersonate.scopes";
  public static final String IMPERSONATE_DELEGATES = "gcp.bigquery.impersonate.delegates";

  public static final String DEFAULT_GCP_LOCATION = "us";
  private static final int DEFAULT_LIFETIME_SECONDS = 3600;
  private static final List<String> DEFAULT_SCOPES =
      ImmutableList.of("https://www.googleapis.com/auth/cloud-platform");

  private final String projectId;
  private final String location;
  private final boolean listAllTables;
  private final String impersonateServiceAccount;
  private final int lifetimeSeconds;
  private final List<String> scopes;
  private final List<String> delegates;

  @VisibleForTesting
  List<String> parseCommaSeparatedList(String input, List<String> defaultValue) {
    if (input == null || input.trim().isEmpty()) {
      return defaultValue;
    }
    return Arrays.stream(input.split(","))
        .map(String::trim)
        .filter(str -> !str.isEmpty())
        .distinct()
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  List<String> expandScopes(List<String> inputScopes) {
    if (inputScopes == null || inputScopes.isEmpty()) {
      return inputScopes;
    }
    return inputScopes.stream()
        .map(
            inputScope -> {
              if (inputScope.startsWith("https://")) {
                return inputScope;
              }
              if (inputScope.startsWith("http://")) {
                return inputScope.replace("http://", "https://");
              }

              return "https://www.googleapis.com/auth/" + inputScope;
            })
        .collect(Collectors.toList());
  }

  BigQueryProperties(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Properties cannot be null");

    this.projectId = properties.get(PROJECT_ID);
    Preconditions.checkArgument(
        projectId != null, "Invalid GCP project: %s must be specified", PROJECT_ID);

    this.location = properties.getOrDefault(GCP_LOCATION, DEFAULT_GCP_LOCATION);

    this.listAllTables = Boolean.parseBoolean(properties.getOrDefault(LIST_ALL_TABLES, "true"));

    // Impersonation properties, optional
    this.impersonateServiceAccount = properties.get(IMPERSONATE_SERVICE_ACCOUNT);

    this.lifetimeSeconds =
        Integer.parseInt(
            properties.getOrDefault(
                IMPERSONATE_LIFETIME_SECONDS, String.valueOf(DEFAULT_LIFETIME_SECONDS)));

    List<String> rawScopes =
        parseCommaSeparatedList(properties.get(IMPERSONATE_SCOPES), DEFAULT_SCOPES);
    this.scopes = expandScopes(rawScopes);

    this.delegates = parseCommaSeparatedList(properties.get(IMPERSONATE_DELEGATES), null);

    if (impersonateServiceAccount != null) {
      LOG.info(
          "BigQuery impersonation configured for service account: {}, lifetime: {} seconds",
          impersonateServiceAccount,
          lifetimeSeconds);
    }
  }

  String projectId() {
    return projectId;
  }

  String location() {
    return location;
  }

  boolean listAllTables() {
    return listAllTables;
  }

  BigQueryOptions metastoreOptions() {
    BigQueryOptions.Builder builder =
        BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setLocation(location)
            .setRetrySettings(ServiceOptions.getDefaultRetrySettings());

    if (impersonateServiceAccount != null) {
      builder.setCredentials(buildImpersonatedCredentials());
    } else {
      builder.setCredentials(buildApplicationDefaultCredentials());
    }

    return builder.build();
  }

  private GoogleCredentials buildApplicationDefaultCredentials() {
    try {
      GoogleCredentials applicationDefaultCredentials =
          GoogleCredentials.getApplicationDefault().createScoped(BigqueryScopes.all());

      LOG.debug(
          "Created application default credentials for BigQuery: {}",
          applicationDefaultCredentials);

      return applicationDefaultCredentials;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to get application default credentials", e);
    }
  }

  private ImpersonatedCredentials buildImpersonatedCredentials() {
    try {
      GoogleCredentials sourceCredentials = GoogleCredentials.getApplicationDefault();

      ImpersonatedCredentials impersonatedCredentials =
          ImpersonatedCredentials.create(
              sourceCredentials, impersonateServiceAccount, delegates, scopes, lifetimeSeconds);

      // refresh to validate credentials and get intial token
      impersonatedCredentials.refresh();

      LOG.debug(
          "Created impersonated credentials for BigQuery: Target={}", impersonateServiceAccount);

      return impersonatedCredentials;
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to create impersonated credentials for " + impersonateServiceAccount, e);
    }
  }
}
