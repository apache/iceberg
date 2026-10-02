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

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestRESTServerExtension {

  @Test
  public void recognizesRealBindFailureFromServer() throws IOException {
    // Guards the premise the retry depends on. #13017's retry went dead because it assumed the
    // server surfaces a direct BindException, so provoke a real bind conflict and assert the
    // failure the server actually throws is still recognized. A dependency upgrade that changes
    // the wrapping fails here instead of silently disabling the retry again.
    try (ServerSocket occupied = new ServerSocket(0)) {
      Map<String, String> config = Maps.newHashMap();
      config.put(RESTCatalogServer.REST_PORT, String.valueOf(occupied.getLocalPort()));
      RESTCatalogServer server = new RESTCatalogServer(config);

      assertThatThrownBy(() -> server.start(false))
          .hasMessageContaining("Failed to bind to")
          .matches(RESTServerExtension::isBindException, "recognized as a bind failure");
    }
  }

  @Test
  public void recognizesBindExceptionWrappedInIOException() {
    // Jetty surfaces a bind conflict as an IOException whose cause is the real BindException, so
    // the retry in RESTServerExtension#beforeAll must scan the cause chain to recognize it.
    IOException wrapped =
        new IOException(
            "Failed to bind to 0.0.0.0/0.0.0.0:35673", new BindException("Address already in use"));
    // A direct `catch (BindException)` would have missed this, which is why the retry never fired.
    assertThat(wrapped).isNotInstanceOf(BindException.class);
    assertThat(RESTServerExtension.isBindException(wrapped)).isTrue();
  }

  @Test
  public void recognizesDirectBindException() {
    assertThat(RESTServerExtension.isBindException(new BindException("Address already in use")))
        .isTrue();
  }

  @Test
  public void ignoresNonBindFailures() {
    assertThat(RESTServerExtension.isBindException(new RuntimeException("boom"))).isFalse();
    assertThat(RESTServerExtension.isBindException(new IOException("disk error"))).isFalse();
  }

  @Test
  public void handlesNullThrowable() {
    assertThat(RESTServerExtension.isBindException(null)).isFalse();
  }
}
