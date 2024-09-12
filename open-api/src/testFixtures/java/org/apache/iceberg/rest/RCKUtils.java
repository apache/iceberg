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
package org.apache.iceberg.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;

class RCKUtils {
  private static final String CATALOG_ENV_PREFIX = "CATALOG_";
  static final String RCK_LOCAL = "rck.local";
  static final String RCK_PURGE_TEST_NAMESPACES = "rck.purge-test-namespaces";

  static final List<Namespace> TEST_NAMESPACES = List.of(Namespace.of("ns"), Namespace.of("newdb"));

  private RCKUtils() {}

  /**
   * Utility method that allows configuring catalog properties via environment variables.
   *
   * <p>Returns a property map for all environment variables that start with <code>CATALOG_</code>
   * replacing double-underscore (<code>__</code>) with dash (<code>-</code>) and replacing single
   * underscore (<code>_</code>) with dot (<code>.</code>) to allow for common catalog property
   * conventions. All characters in the name are converted to lowercase and values are unmodified.
   *
   * <p>Examples:
   *
   * <pre><code>
   *     CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog -> catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog
   *     CATALOG_URI=jdbc:sqlite:memory: -> uri=jdbc:sqlite:memory:
   *     CATALOG_WAREHOUSE=test_warehouse -> warehouse=test_warehouse
   *     CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO -> io-impl=org.apache.iceberg.aws.s3.S3FileIO
   *     CATALOG_JDBC_USER=ice_user -> jdbc.user=ice_user
   * </code></pre>
   *
   * @return configuration map
   */
  static Map<String, String> environmentCatalogConfig() {
    return System.getenv().entrySet().stream()
        .filter(e -> e.getKey().startsWith(CATALOG_ENV_PREFIX))
        .collect(
            Collectors.toMap(
                e ->
                    e.getKey()
                        .replaceFirst(CATALOG_ENV_PREFIX, "")
                        .replaceAll("__", "-")
                        .replaceAll("_", ".")
                        .toLowerCase(Locale.ROOT),
                Map.Entry::getValue,
                (m1, m2) -> {
                  throw new IllegalArgumentException("Duplicate key: " + m1);
                },
                HashMap::new));
  }

  static RESTCatalog initCatalogClient() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.putAll(RCKUtils.environmentCatalogConfig());
    catalogProperties.putAll(Maps.fromProperties(System.getProperties()));

    // Set defaults
    catalogProperties.putIfAbsent(
        CatalogProperties.URI,
        String.format("http://localhost:%s/", RESTCatalogServer.REST_PORT_DEFAULT));
    catalogProperties.putIfAbsent(CatalogProperties.WAREHOUSE_LOCATION, "rck_warehouse");
    catalogProperties.putIfAbsent(
        CatalogProperties.VIEW_DEFAULT_PREFIX + "key1", "catalog-default-key1");

    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize("rck_catalog", catalogProperties);
    return catalog;
  }

  static void purgeCatalogTestEntries(RESTCatalog catalog) {
    if (!PropertyUtil.propertyAsBoolean(catalog.properties(), RCK_PURGE_TEST_NAMESPACES, true)) {
      return;
    }

    TEST_NAMESPACES.stream()
        .filter(catalog::namespaceExists)
        .forEach(
            namespace -> {
              catalog.listTables(namespace).forEach(catalog::dropTable);
              catalog.listViews(namespace).forEach(catalog::dropView);
              catalog.dropNamespace(namespace);
            });
  }
}
