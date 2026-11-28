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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestRESTServerExtension {

  @RegisterExtension
  private static final RESTServerExtension EXTENSION =
      new RESTServerExtension(Map.of(RESTCatalogServer.REST_PORT, RESTServerExtension.FREE_PORT));

  @Test
  void freePortReportsTheBoundPort() {
    String port = EXTENSION.config().get(RESTCatalogServer.REST_PORT);

    assertThat(port).isNotEqualTo(RESTServerExtension.FREE_PORT);
    assertThat(Integer.parseInt(port)).isPositive();
    assertThat(EXTENSION.client().listNamespaces()).isEmpty();
  }
}
