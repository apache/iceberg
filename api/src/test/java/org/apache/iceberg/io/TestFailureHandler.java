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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

public class TestFailureHandler {

  private static final FileFailure FAILURE =
      new FileFailure("s3://b/k", FailureCategory.UNKNOWN, null, null);

  @Test
  public void noopDropsFailures() {
    assertThatCode(() -> FailureHandler.NOOP.onFailure(FAILURE)).doesNotThrowAnyException();
  }

  @Test
  public void safeNotifyDeliversToHandler() {
    CapturingFailureHandler handler = new CapturingFailureHandler();

    FailureHandler.safeNotify(LoggerFactory.getLogger(TestFailureHandler.class), handler, FAILURE);

    assertThat(handler.failures()).containsExactly(FAILURE);
  }

  @Test
  public void safeNotifySwallowsHandlerException() {
    FailureHandler throwing =
        failure -> {
          throw new RuntimeException("handler blew up");
        };

    // The contract: a misbehaving handler must not poison the bulk operation.
    assertThatCode(
            () ->
                FailureHandler.safeNotify(
                    LoggerFactory.getLogger(TestFailureHandler.class), throwing, FAILURE))
        .doesNotThrowAnyException();
  }
}
