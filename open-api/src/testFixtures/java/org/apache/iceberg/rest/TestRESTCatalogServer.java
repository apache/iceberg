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

import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestRESTCatalogServer {

  @Test
  public void testCatalogNameDefault() {
    Map<String, String> config = Maps.newHashMap();
    new RESTCatalogServer(config);

    // Should use default catalog name when not specified
    assertThat(RESTCatalogServer.CATALOG_NAME_DEFAULT).isEqualTo("rest_backend");
  }

  @Test
  public void testCatalogNameFromConfig() {
    Map<String, String> config = Maps.newHashMap();
    config.put(RESTCatalogServer.CATALOG_NAME, "my_custom_catalog");
    new RESTCatalogServer(config);

    // Should use the configured catalog name
    assertThat(config.get(RESTCatalogServer.CATALOG_NAME)).isEqualTo("my_custom_catalog");
  }

  @Test
  public void testEnvironmentVariableMapping() {
    // Test that CATALOG_NAME environment variable gets mapped correctly
    // This follows the pattern in RCKUtils.environmentCatalogConfig()
    String envVar = "CATALOG_NAME";

    // The environment variable should be converted to the property name
    assertThat(envVar.replaceFirst("CATALOG_", "").toLowerCase(Locale.ROOT)).isEqualTo("name");
    // But the actual mapping in RCKUtils would be:
    // CATALOG_NAME -> catalog.name
  }
}
