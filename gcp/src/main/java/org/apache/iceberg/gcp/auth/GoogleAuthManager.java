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
package org.apache.iceberg.gcp.auth;

import com.google.auth.oauth2.GoogleCredentials;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authentication manager that uses Google Credentials (typically Application Default
 * Credentials) to create {@link GoogleAuthSession} instances.
 *
 * <p>This manager can be configured with properties such as:
 *
 * <ul>
 *   <li>{@code gcp.auth.credentials-path}: Path to a service account JSON key file. If not set,
 *       Application Default Credentials will be used.
 *   <li>{@code gcp.auth.scopes}: Comma-separated list of OAuth scopes to request. Defaults to
 *       "https://www.googleapis.com/auth/cloud-platform".
 * </ul>
 */
public class GoogleAuthManager implements AuthManager {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleAuthManager.class);
  public static final String DEFAULT_SCOPES = "https://www.googleapis.com/auth/cloud-platform";
  private final String name;

  private GoogleCredentials credentials;
  private boolean initialized = false;

  public GoogleAuthManager(String managerName) {
    this.name = managerName;
  }

  public String getName() {
    return name;
  }

  private void initialize(Map<String, String> properties) {
    if (initialized) {
      return;
    }

    String credentialsPath = properties.get(GCPProperties.GCP_CREDENTIALS_PATH_PROPERTY);
    String scopesString =
        properties.getOrDefault(GCPProperties.GCP_SCOPES_PROPERTY, DEFAULT_SCOPES);
    List<String> scopes = ImmutableList.copyOf(scopesString.split(","));

    try {
      if (credentialsPath != null && !credentialsPath.isEmpty()) {
        LOG.info("Using Google credentials from path: {}", credentialsPath);
        try (FileInputStream credentialsStream = new FileInputStream(credentialsPath)) {
          this.credentials = GoogleCredentials.fromStream(credentialsStream).createScoped(scopes);
        }
      } else {
        LOG.info("Using Application Default Credentials with scopes: {}", scopesString);
        this.credentials = GoogleCredentials.getApplicationDefault().createScoped(scopes);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load Google credentials", e);
    }
    this.initialized = true;
  }

  /**
   * Initializes and returns a short-lived session, typically for fetching configuration. This
   * implementation reuses the long-lived catalog session logic.
   */
  @Override
  public AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    return catalogSession(initClient, properties);
  }

  /**
   * Returns a long-lived session tied to the catalog's lifecycle. This session uses Google
   * Application Default Credentials or a specified service account.
   *
   * @param sharedClient The long-lived RESTClient (not used by this implementation for credential
   *     fetching).
   * @param properties Configuration properties for the auth manager.
   * @return A {@link GoogleAuthSession}.
   * @throws UncheckedIOException if credential loading fails.
   */
  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    initialize(properties);
    Preconditions.checkState(
        credentials != null, "GoogleAuthManager not initialized or failed to load credentials");
    return new GoogleAuthSession(credentials);
  }

  /** Returns a session for a specific context. Defaults to the catalog session. */
  @Override
  public AuthSession contextualSession(SessionCatalog.SessionContext context, AuthSession parent) {
    // For GCP, tokens are typically not context-specific in this manner.
    // Re-using the parent (which should be a GoogleAuthSession) is appropriate.
    // Or, if properties for a specific context were available, a new GoogleAuthSession could be
    // derived.
    if (parent instanceof GoogleAuthSession) {
      return parent;
    }
    // Fallback to a new catalog-level session if the parent is not a GoogleAuthSession for some
    // reason.
    // This would require properties to be available or a default initialization.
    LOG.warn(
        "Parent session is not a GoogleAuthSession. Creating a new default catalog session. This might not be intended.");
    return catalogSession(
        null, Collections.emptyMap()); // Assuming default ADC without specific props
  }

  /** Returns a session for a specific table or view. Defaults to the catalog session. */
  @Override
  public AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    // Similar to contextualSession, GCP tokens are generally not table-specific.
    if (parent instanceof GoogleAuthSession) {
      return parent;
    }
    LOG.warn(
        "Parent session for table {} is not a GoogleAuthSession. Creating a new default catalog session.",
        table);
    return catalogSession(null, properties);
  }

  /** Closes the manager. This is a no-op for GoogleAuthManager. */
  @Override
  public void close() {
    // No-op. Credentials lifecycle is managed by the GoogleCredentials object itself or by the
    // application.
  }
}
