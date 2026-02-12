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
package org.apache.iceberg.gcp.bigquery.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestRetryDetector {
  @Test
  public void testNoAttempts() {
    RetryDetector detector = new RetryDetector();
    assertThat(detector.retried()).isFalse();
    assertThat(detector.attempts()).isEqualTo(0);
  }

  @Test
  public void testSingleAttempt() throws Exception {
    RetryDetector detector = new RetryDetector();
    detector.wrap(() -> "result").call();
    assertThat(detector.retried()).isFalse();
    assertThat(detector.attempts()).isEqualTo(1);
  }

  @Test
  public void testMultipleAttempts() throws Exception {
    RetryDetector detector = new RetryDetector();
    var wrapped = detector.wrap(() -> "result");
    wrapped.call();
    wrapped.call();
    assertThat(detector.retried()).isTrue();
    assertThat(detector.attempts()).isEqualTo(2);
  }

  @Test
  public void testWrappedCallableReturnsValue() throws Exception {
    RetryDetector detector = new RetryDetector();
    String result = detector.wrap(() -> "expected").call();
    assertThat(result).isEqualTo("expected");
  }
}
