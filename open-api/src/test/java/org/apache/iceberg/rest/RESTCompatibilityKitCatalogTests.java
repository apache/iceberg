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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.util.PropertyUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(RESTServerExtension.class)
public class RESTCompatibilityKitCatalogTests extends CatalogTests<RESTCatalog> {
  private static final Logger LOG = LoggerFactory.getLogger(RESTCompatibilityKitCatalogTests.class);

  private static RESTCatalog restCatalog;

  @BeforeAll
  static void beforeClass() throws Exception {
    restCatalog = RCKUtils.initCatalogClient();

    assertThat(restCatalog.listNamespaces())
        .withFailMessage("Namespaces list should not contain: %s", RCKUtils.TEST_NAMESPACES)
        .doesNotContainAnyElementsOf(RCKUtils.TEST_NAMESPACES);
  }

  @BeforeEach
  void before() {
    try {
      RCKUtils.purgeCatalogTestEntries(restCatalog);
    } catch (Exception e) {
      LOG.warn("Failure during test setup", e);
    }
  }

  @AfterAll
  static void afterClass() throws Exception {
    restCatalog.close();
  }

  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  @Override
  protected RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    return RCKUtils.initCatalogClient(additionalProperties);
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return PropertyUtil.propertyAsBoolean(
        restCatalog.properties(),
        RESTCompatibilityKitSuite.RCK_REQUIRES_NAMESPACE_CREATE,
        super.requiresNamespaceCreate());
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return PropertyUtil.propertyAsBoolean(
        restCatalog.properties(), RESTCompatibilityKitSuite.RCK_SUPPORTS_SERVERSIDE_RETRY, true);
  }

  @Override
  protected boolean overridesRequestedLocation() {
    return PropertyUtil.propertyAsBoolean(
        restCatalog.properties(),
        RESTCompatibilityKitSuite.RCK_OVERRIDES_REQUESTED_LOCATION,
        false);
  }

  @Override
  protected boolean supportsNamesWithDot() {
    // underlying JDBC catalog doesn't support namespaces with a dot
    return PropertyUtil.propertyAsBoolean(
        restCatalog.properties(), RESTCompatibilityKitSuite.RCK_SUPPORTS_NAMES_WITH_DOT, false);
  }

  @Override
  protected boolean supportsNamesWithSlashes() {
    // names with slashes are rejected and considered as suspicious characters after upgrading Jetty
    // and the Servlet API. See also
    // https://jakarta.ee/specifications/servlet/6.0/jakarta-servlet-spec-6.0.html#uri-path-canonicalization
    // for additional details
    return false;
  }

  @Test
  public void testUnregisterTable() {
    if (requiresNamespaceCreate()) {
      restCatalog.createNamespace(TABLE.namespace());
    }

    Table original =
        restCatalog
            .buildTable(TABLE, SCHEMA)
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .create();
    original.newFastAppend().appendFile(FILE_A).commit();
    original.newFastAppend().appendFile(FILE_B).commit();

    String metadataLocation = restCatalog.unregisterTable(TABLE);

    assertThat(metadataLocation).as("Returned metadata location must not be null").isNotNull();
    assertThat(restCatalog.tableExists(TABLE))
        .as("Table must not exist after being unregistered")
        .isFalse();

    // the underlying files are left in place, so the table can be registered again
    Table registered = restCatalog.registerTable(TABLE, metadataLocation);
    assertThat(registered.currentSnapshot())
        .as("Current snapshot must match the unregistered table")
        .isEqualTo(original.currentSnapshot());
    assertFiles(registered, FILE_A, FILE_B);

    assertThat(restCatalog.dropTable(TABLE)).isTrue();
  }

  @Test
  public void testUnregisterMissingTable() {
    if (requiresNamespaceCreate()) {
      restCatalog.createNamespace(TABLE.namespace());
    }

    assertThatThrownBy(() -> restCatalog.unregisterTable(TABLE))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Table does not exist");
  }

  @Disabled("RESTServerExtension isn’t configurable per test")
  @Test
  public void createTableInUniqueLocation() {
    super.createTableInUniqueLocation();
  }
}
