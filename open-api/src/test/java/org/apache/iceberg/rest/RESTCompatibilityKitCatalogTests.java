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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
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

  @Override
  protected boolean supportsVariant() {
    return PropertyUtil.propertyAsBoolean(
        restCatalog.properties(), RESTCompatibilityKitSuite.RCK_SUPPORTS_VARIANT, false);
  }

  @Test
  public void testUpdateTableSchemaRejectsReservedFieldIds() throws Exception {
    if (requiresNamespaceCreate()) {
      restCatalog.createNamespace(NS);
    }

    Table table = restCatalog.buildTable(TABLE, SCHEMA).create();
    Schema reservedSchema =
        new Schema(
            ImmutableList.<Types.NestedField>builder()
                .addAll(table.schema().columns())
                .add(Types.NestedField.optional(2147483448, "reserved", Types.StringType.get()))
                .build());

    // the client-side metadata builder rejects reserved IDs, so send the update directly to
    // check that the server validates it too
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE,
            List.of(
                new UpdateRequirement.AssertTableUUID(
                    ((BaseTable) table).operations().current().uuid())),
            List.of(
                new MetadataUpdate.AddSchema(reservedSchema),
                new MetadataUpdate.SetCurrentSchema(-1)));

    Map<String, String> props = restCatalog.properties();
    try (RESTClient client =
            HTTPClient.builder(props)
                .uri(props.get(CatalogProperties.URI))
                .withHeaders(RESTUtil.configHeaders(props))
                .build();
        AuthManager authManager = AuthManagers.loadAuthManager("rck", props);
        AuthSession session = authManager.catalogSession(client, props)) {
      assertThatThrownBy(
              () ->
                  client
                      .withAuthSession(session)
                      .post(
                          ResourcePaths.forCatalogProperties(props).table(TABLE),
                          request,
                          LoadTableResponse.class,
                          Map.of(),
                          ErrorHandlers.tableCommitHandler()))
          .isInstanceOf(BadRequestException.class);
    }

    assertThat(restCatalog.loadTable(TABLE).schema().asStruct())
        .isEqualTo(table.schema().asStruct());
  }

  @Disabled("RESTServerExtension isn’t configurable per test")
  @Test
  public void createTableInUniqueLocation() {
    super.createTableInUniqueLocation();
  }
}
