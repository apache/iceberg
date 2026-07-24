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

import java.io.IOException;
import java.net.BindException;
import org.junit.jupiter.api.Test;

public class TestRESTServerExtension {

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
