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
package org.apache.iceberg.rest.auth.oauth2;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates legacy OAuth2 properties (see {@link OAuth2Properties}) into the properties understood
 * by {@link OAuth2Config}, and resolves token endpoints that are missing or relative.
 *
 * <p>Each warning is logged at most once per adapter instance, since the same properties are
 * typically seen many times over the lifetime of a catalog.
 */
final class LegacyConfigAdapter {

  /** The client ID assumed when {@link OAuth2Properties#CREDENTIAL} contains only a secret. */
  @VisibleForTesting static final String DEFAULT_CLIENT_ID = "iceberg";

  private static final Logger LOG = LoggerFactory.getLogger(LegacyConfigAdapter.class);

  private static final Splitter CREDENTIAL_SPLITTER = Splitter.on(':').limit(2).trimResults();

  private static final List<String> IGNORED_LEGACY_PROPERTIES =
      List.of(
          OAuth2Properties.TOKEN,
          OAuth2Properties.TOKEN_EXPIRES_IN_MS,
          OAuth2Properties.TOKEN_REFRESH_ENABLED,
          OAuth2Properties.TOKEN_EXCHANGE_ENABLED,
          OAuth2Properties.ACCESS_TOKEN_TYPE,
          OAuth2Properties.REFRESH_TOKEN_TYPE,
          OAuth2Properties.ID_TOKEN_TYPE,
          OAuth2Properties.SAML1_TOKEN_TYPE,
          OAuth2Properties.SAML2_TOKEN_TYPE,
          OAuth2Properties.JWT_TOKEN_TYPE);

  private final Set<String> loggedWarnings = ConcurrentHashMap.newKeySet();

  /**
   * Migrates legacy OAuth2 properties to their {@link OAuth2Config} equivalents. Properties already
   * using the {@link OAuth2Config#PREFIX} take precedence over migrated legacy properties.
   * Unrelated properties are dropped.
   */
  Map<String, String> migrate(Map<String, String> properties) {
    Map<String, String> migrated = Maps.newLinkedHashMap();

    String credential = properties.get(OAuth2Properties.CREDENTIAL);
    if (credential != null) {
      if (shouldWarn(OAuth2Properties.CREDENTIAL)) {
        LOG.warn(
            "Detected legacy OAuth2 property '{}', please use properties {} and {} instead",
            OAuth2Properties.CREDENTIAL,
            OAuth2Config.CLIENT_ID,
            OAuth2Config.CLIENT_SECRET);
      }

      List<String> parts = CREDENTIAL_SPLITTER.splitToList(credential);
      if (parts.size() == 2) {
        migrated.put(OAuth2Config.CLIENT_ID, parts.get(0));
        migrated.put(OAuth2Config.CLIENT_SECRET, parts.get(1));
      } else {
        if (shouldWarn(OAuth2Properties.CREDENTIAL + ":no-client-id")) {
          LOG.warn(
              "Legacy OAuth2 property '{}' does not contain a client ID, assuming '{}'",
              OAuth2Properties.CREDENTIAL,
              DEFAULT_CLIENT_ID);
        }

        migrated.put(OAuth2Config.CLIENT_ID, DEFAULT_CLIENT_ID);
        migrated.put(OAuth2Config.CLIENT_SECRET, parts.get(0));
      }
    }

    migrateProperty(
        properties, migrated, OAuth2Properties.OAUTH2_SERVER_URI, OAuth2Config.TOKEN_ENDPOINT);
    migrateProperty(properties, migrated, OAuth2Properties.SCOPE, OAuth2Config.SCOPE);
    migrateProperty(properties, migrated, OAuth2Properties.AUDIENCE, OAuth2Config.AUDIENCE);
    migrateProperty(properties, migrated, OAuth2Properties.RESOURCE, OAuth2Config.RESOURCE);

    for (String ignored : IGNORED_LEGACY_PROPERTIES) {
      if (properties.containsKey(ignored) && shouldWarn(ignored)) {
        LOG.warn(
            "Ignoring legacy OAuth2 property '{}': it is not supported by {}",
            ignored,
            OAuth2Manager.class.getName());
      }
    }

    properties.forEach(
        (key, value) -> {
          if (key.startsWith(OAuth2Config.PREFIX)) {
            migrated.put(key, value);
          }
        });

    if (credential != null && !migrated.containsKey(OAuth2Config.SCOPE)) {
      // the legacy OAuth2 manager requests the 'catalog' scope by default
      migrated.put(OAuth2Config.SCOPE, OAuth2Properties.CATALOG_SCOPE);
    }

    return migrated;
  }

  /** Returns the legacy OAuth2 properties present in the given map that are not supported. */
  List<String> unsupportedProperties(Map<String, String> properties) {
    return IGNORED_LEGACY_PROPERTIES.stream().filter(properties::containsKey).toList();
  }

  /**
   * Resolves the token endpoint against the catalog URI when it is missing or relative, mirroring
   * the behavior of the legacy OAuth2 manager. This fallback targets the deprecated token endpoint
   * of the REST catalog server and will be removed in a future release.
   */
  Map<String, String> resolveTokenEndpoint(Map<String, String> migrated, String catalogUri) {
    Preconditions.checkNotNull(catalogUri, "Invalid catalog URI: null");

    String tokenEndpoint = migrated.get(OAuth2Config.TOKEN_ENDPOINT);
    String issuerUrl = migrated.get(OAuth2Config.ISSUER_URL);
    String resolved;

    if (tokenEndpoint == null && issuerUrl == null) {
      resolved = RESTUtil.resolveEndpoint(catalogUri, ResourcePaths.tokens());
      if (shouldWarn("missing-token-endpoint:" + resolved)) {
        LOG.warn(
            "The OAuth2 configuration specifies neither a token endpoint nor an issuer URL: "
                + "the token endpoint defaults to {}. This fallback will be removed in a future "
                + "release. Please configure the OAuth2 endpoint using either {} or {}. "
                + "See https://github.com/apache/iceberg/issues/10537",
            resolved,
            OAuth2Config.TOKEN_ENDPOINT,
            OAuth2Config.ISSUER_URL);
      }
    } else if (tokenEndpoint != null && !URI.create(tokenEndpoint).isAbsolute()) {
      resolved = RESTUtil.resolveEndpoint(catalogUri, tokenEndpoint);
      if (shouldWarn("relative-token-endpoint:" + resolved)) {
        LOG.warn(
            "The OAuth2 token endpoint '{}' is relative and was resolved against the catalog URI "
                + "to {}. This fallback will be removed in a future release. Please configure "
                + "the token endpoint using an absolute URL",
            tokenEndpoint,
            resolved);
      }
    } else {
      return migrated;
    }

    Map<String, String> result = Maps.newLinkedHashMap(migrated);
    result.put(OAuth2Config.TOKEN_ENDPOINT, resolved);
    return result;
  }

  private void migrateProperty(
      Map<String, String> properties, Map<String, String> migrated, String legacy, String key) {
    String value = properties.get(legacy);
    if (value != null) {
      if (shouldWarn(legacy)) {
        LOG.warn(
            "Detected legacy OAuth2 property '{}', please use property {} instead", legacy, key);
      }

      migrated.put(key, value);
    }
  }

  private boolean shouldWarn(String key) {
    return loggedWarnings.add(key);
  }
}
