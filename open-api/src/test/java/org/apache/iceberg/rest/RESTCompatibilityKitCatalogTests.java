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

import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.util.PropertyUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
}
