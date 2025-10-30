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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQueryOptions;
import java.io.IOException;
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

/**
 * BigQuery client factory that uses service account impersonation.
 *
 * <p>This implementation allows a source service account to act as a target service account, useful
 * for implementing least-privilege principles and separating operational credentials from data
 * access credentials.
 *
 * <p><b>Additional Configuration Properties:</b>
 *
 * <ul>
 *   <li><code>gcp.impersonate.service-account</code> - Required. Target service account email
 *   <li><code>gcp.impersonate.lifetime-seconds</code> - Optional. Token lifetime (default: 3600)
 *   <li><code>gcp.impersonate.scopes</code> - Optional. Comma-separated OAuth2 scopes
 *   <li><code>gcp.impersonate.delegates</code> - Optional. Delegation chain for chained
 *       impersonation
 * </ul>
 *
 * @see <a href="https://cloud.google.com/iam/docs/impersonating-service-accounts">Impersonating
 *     Service Accounts</a>
 */
public class ImpersonatedBigQueryClientFactory implements BigQueryClientFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(ImpersonatedBigQueryClientFactory.class);

  /** Configuration property for the target service account to impersonate. */
  public static final String IMPERSONATE_SERVICE_ACCOUNT = "gcp.impersonate.service-account";

  /** Token lifetime in seconds (default: 3600). */
  public static final String IMPERSONATE_LIFETIME_SECONDS = "gcp.impersonate.lifetime-seconds";

  /** Comma-separated OAuth2 scopes (defaults to BigQuery and Cloud Platform). */
  public static final String IMPERSONATE_SCOPES = "gcp.impersonate.scopes";

  /** Comma-separated delegation chain for chained impersonation. */
  public static final String IMPERSONATE_DELEGATES = "gcp.impersonate.delegates";

  private static final int DEFAULT_LIFETIME_SECONDS = 3600;

  private static final String DEFAULT_LOCATION = "us";

  private static final List<String> DEFAULT_SCOPES =
      ImmutableList.of(
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/cloud-platform");

  private String targetServiceAccount;
  private String projectId;
  private String location;
  private int lifetimeSeconds;
  private List<String> scopes;
  private List<String> delegates;

  @Override
  public void initialize(Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Properties cannot be null");
    this.targetServiceAccount = properties.get(IMPERSONATE_SERVICE_ACCOUNT);
    Preconditions.checkNotNull(
        targetServiceAccount,
        "Cannot initialize ImpersonatedBigQueryClientFactory without target service account: %s is required",
        IMPERSONATE_SERVICE_ACCOUNT);

    this.projectId = properties.get(BigQueryMetastoreCatalog.PROJECT_ID);
    Preconditions.checkNotNull(
        projectId,
        "Cannot initialize ImpersonatedBigQueryClientFactory without project ID: %s is required",
        BigQueryMetastoreCatalog.PROJECT_ID);

    this.location =
        properties.getOrDefault(BigQueryMetastoreCatalog.GCP_LOCATION, DEFAULT_LOCATION);

    this.lifetimeSeconds =
        Integer.parseInt(
            properties.getOrDefault(
                IMPERSONATE_LIFETIME_SECONDS, String.valueOf(DEFAULT_LIFETIME_SECONDS)));

    List<String> rawScopes =
        parseCommaSeparatedList(properties.get(IMPERSONATE_SCOPES), DEFAULT_SCOPES);
    this.scopes = expandScopes(rawScopes);
    this.delegates = parseCommaSeparatedList(properties.get(IMPERSONATE_DELEGATES), null);

    LOG.info(
        "Initialized ImpersonatedBigQueryClientFactory to impersonate {} with lifetime {} seconds",
        targetServiceAccount,
        lifetimeSeconds);
  }

  /**
   * Parses a comma-separated string into a list, or returns a default value if input is null.
   *
   * @param input the comma-separated string to parse
   * @param defaultValue the default value to return if input is null
   * @return a list of parsed values or the default value
   */
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

  /**
   * Expands scope strings to full Google OAuth2 URLs. Handles short names (bigquery), http URLs
   * (converts to https), and full https URLs (pass-through).
   *
   * @param inputScopes List of scope strings to expand
   * @return List of full OAuth2 scope URLs
   */
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

  @Override
  public BigQueryOptions bigQueryOptions() {
    try {
      GoogleCredentials sourceCredentials = GoogleCredentials.getApplicationDefault();

      ImpersonatedCredentials impersonatedCredentials =
          ImpersonatedCredentials.create(
              sourceCredentials, targetServiceAccount, delegates, scopes, lifetimeSeconds);

      impersonatedCredentials.refresh();

      return BigQueryOptions.newBuilder()
          .setProjectId(projectId)
          .setLocation(location)
          .setCredentials(impersonatedCredentials)
          .setRetrySettings(ServiceOptions.getDefaultRetrySettings())
          .build();

    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to create impersonated credentials for " + targetServiceAccount, e);
    }
  }
}
