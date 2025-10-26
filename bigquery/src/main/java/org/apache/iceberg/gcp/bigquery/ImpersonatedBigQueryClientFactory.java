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

import com.google.api.client.util.Preconditions;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQueryOptions;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

  /**
   * Configuration property for the impersonated token lifetime in seconds.
   *
   * <p>Per Google Cloud documentation: "If a value is not specified, the token's lifetime will be
   * set to a default value of 1 hour."
   *
   * @see <a
   *     href="https://cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken">generateAccessToken
   *     API - lifetime field</a>
   */
  public static final String IMPERSONATE_LIFETIME_SECONDS = "gcp.impersonate.lifetime-seconds";

  /**
   * Configuration property for OAuth2 scopes as a comma-separated list.
   *
   * <p>Scopes define the level of access granted to the impersonated credentials. If not specified,
   * defaults to BigQuery and Cloud Platform scopes.
   *
   * @see <a href="https://developers.google.com/identity/protocols/oauth2/scopes">Google OAuth2
   *     Scopes</a>
   */
  public static final String IMPERSONATE_SCOPES = "gcp.impersonate.scopes";

  /**
   * Configuration property for delegation chain as a comma-separated list of service account
   * emails.
   *
   * <p>Delegates are used for chained impersonation where SA1 impersonates SA2 which impersonates
   * SA3. Each service account in the chain must have the appropriate IAM permissions.
   *
   * @see <a
   *     href="https://cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken#body.request_body.FIELDS.delegates">generateAccessToken
   *     API - delegates field</a>
   */
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

    this.scopes = parseCommaSeparatedList(properties.get(IMPERSONATE_SCOPES), DEFAULT_SCOPES);
    this.delegates = parseCommaSeparatedList(properties.get(IMPERSONATE_DELEGATES), null);

    LOG.info(
        "Initialized ImpersonationGCPClientFactory to impersonate {} with lifetime {} seconds",
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
  private List<String> parseCommaSeparatedList(String input, List<String> defaultValue) {
    return input != null ? Arrays.asList(input.split(",")) : defaultValue;
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
      throw new RuntimeException(
          "Failed to create impersonated credentials for " + targetServiceAccount, e);
    }
  }
}
