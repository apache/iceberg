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

public class RESTCatalogServerTest {

  @Test
  public void testCatalogNameDefault() throws Exception {
    Map<String, String> config = Maps.newHashMap();
    RESTCatalogServer server = new RESTCatalogServer(config);

    RESTCatalogServer.CatalogContext context = server.initializeBackendCatalog();

    assertThat(context.catalog().name()).isEqualTo(RESTCatalogServer.CATALOG_NAME_DEFAULT);
  }

  @Test
  public void testCatalogNameFromConfig() throws Exception {
    Map<String, String> config = Maps.newHashMap();
    String customCatalogName = "my_custom_catalog";
    config.put(RESTCatalogServer.CATALOG_NAME, customCatalogName);

    RESTCatalogServer server = new RESTCatalogServer(config);
    RESTCatalogServer.CatalogContext context = server.initializeBackendCatalog();

    assertThat(context.catalog().name()).isEqualTo(customCatalogName);
  }
}
