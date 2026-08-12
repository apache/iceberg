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
package org.apache.iceberg.io.http;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestHttpStatusCategory {

  @Test
  void classifiesSuccessCodes() {
    assertThat(HttpStatusCategory.classify(200)).isEqualTo(HttpStatusCategory.OK);
    assertThat(HttpStatusCategory.classify(206)).isEqualTo(HttpStatusCategory.PARTIAL_CONTENT);
  }

  @Test
  void classifiesTerminalClientErrors() {
    assertThat(HttpStatusCategory.classify(403)).isEqualTo(HttpStatusCategory.FORBIDDEN);
    assertThat(HttpStatusCategory.classify(404)).isEqualTo(HttpStatusCategory.NOT_FOUND);
    assertThat(HttpStatusCategory.classify(416))
        .isEqualTo(HttpStatusCategory.RANGE_NOT_SATISFIABLE);
  }

  @Test
  void classifiesThrottlingAndTransientServerErrorsAsTransient() {
    assertThat(HttpStatusCategory.classify(408)).isEqualTo(HttpStatusCategory.TRANSIENT);
    assertThat(HttpStatusCategory.classify(429)).isEqualTo(HttpStatusCategory.TRANSIENT);
    assertThat(HttpStatusCategory.classify(500)).isEqualTo(HttpStatusCategory.TRANSIENT);
    assertThat(HttpStatusCategory.classify(502)).isEqualTo(HttpStatusCategory.TRANSIENT);
    assertThat(HttpStatusCategory.classify(503)).isEqualTo(HttpStatusCategory.TRANSIENT);
    assertThat(HttpStatusCategory.classify(504)).isEqualTo(HttpStatusCategory.TRANSIENT);
  }

  @Test
  void classifiesNonRetryableCodesAsTerminal() {
    // not every 5xx is retryable, and specific client errors must never be retried
    assertThat(HttpStatusCategory.classify(301)).isEqualTo(HttpStatusCategory.TERMINAL);
    assertThat(HttpStatusCategory.classify(400)).isEqualTo(HttpStatusCategory.TERMINAL);
    assertThat(HttpStatusCategory.classify(401)).isEqualTo(HttpStatusCategory.TERMINAL);
    assertThat(HttpStatusCategory.classify(412)).isEqualTo(HttpStatusCategory.TERMINAL);
    assertThat(HttpStatusCategory.classify(501)).isEqualTo(HttpStatusCategory.TERMINAL);
    assertThat(HttpStatusCategory.classify(505)).isEqualTo(HttpStatusCategory.TERMINAL);
  }
}
