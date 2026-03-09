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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestRESTCatalogServer {

  @Test
  public void testCatalogNameConstants() {
    assertThat(RESTCatalogServer.CATALOG_NAME).isEqualTo("catalog.name");
    assertThat(RESTCatalogServer.CATALOG_NAME_DEFAULT).isEqualTo("rest_backend");
  }

  @Test
  public void testDefaultCatalogName() throws Exception {
    Map<String, String> config =
        ImmutableMap.of(RESTCatalogServer.REST_PORT, String.valueOf(RCKUtils.findFreePort()));
    RESTCatalogServer server = new RESTCatalogServer(config);
    try {
      server.start(false);
      RESTCatalog client = RCKUtils.initCatalogClient(config);
      try {
        // server started successfully with default catalog name and can serve requests
        assertThat(client.listNamespaces()).isNotNull();
      } finally {
        client.close();
      }
    } finally {
      server.stop();
    }
  }

  @Test
  public void testCustomCatalogName() throws Exception {
    Map<String, String> config =
        ImmutableMap.of(
            RESTCatalogServer.REST_PORT,
            String.valueOf(RCKUtils.findFreePort()),
            RESTCatalogServer.CATALOG_NAME,
            "my_custom_catalog");
    RESTCatalogServer server = new RESTCatalogServer(config);
    try {
      server.start(false);
      RESTCatalog client = RCKUtils.initCatalogClient(config);
      try {
        // server started successfully with custom catalog name and can serve requests
        assertThat(client.listNamespaces()).isNotNull();
      } finally {
        client.close();
      }
    } finally {
      server.stop();
    }
  }
}
