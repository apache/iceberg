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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestNaNUtil {
  @Test
  void isNaN() {
    assertThat(NaNUtil.isNaN(Double.NaN)).isTrue();
    assertThat(NaNUtil.isNaN(Float.NaN)).isTrue();

    assertThat(NaNUtil.isNaN(null)).isFalse();
    assertThat(NaNUtil.isNaN(1.0d)).isFalse();
    assertThat(NaNUtil.isNaN(Double.POSITIVE_INFINITY)).isFalse();
    assertThat(NaNUtil.isNaN(Double.NEGATIVE_INFINITY)).isFalse();
    assertThat(NaNUtil.isNaN(1.0f)).isFalse();
    assertThat(NaNUtil.isNaN(Float.POSITIVE_INFINITY)).isFalse();
    assertThat(NaNUtil.isNaN("NaN")).isFalse();
    assertThat(NaNUtil.isNaN(1)).isFalse();
    assertThat(NaNUtil.isNaN(1L)).isFalse();
  }
}
