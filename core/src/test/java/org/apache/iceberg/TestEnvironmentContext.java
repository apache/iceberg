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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import org.junit.jupiter.api.Test;

public class TestEnvironmentContext {

  @Test
  public void testDefaultValue() {
    assertThat(EnvironmentContext.get().get("iceberg-version"))
        .isEqualTo(IcebergBuild.fullVersion());
  }

  @Test
  public void testPutAndRemove() {
    EnvironmentContext.put("test-key", "test-value");
    assertThat(EnvironmentContext.get()).containsEntry("test-key", "test-value");

    EnvironmentContext.remove("test-key");
    assertThat(EnvironmentContext.get()).doesNotContainKey("test-key");

    assertThatNoException().isThrownBy(() -> EnvironmentContext.remove("test-key"));
  }
}
