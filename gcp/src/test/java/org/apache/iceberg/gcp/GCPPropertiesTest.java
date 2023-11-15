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
package org.apache.iceberg.gcp;

import static org.apache.iceberg.gcp.GCPProperties.GCS_NO_AUTH;
import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_TOKEN;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class GCPPropertiesTest {

  @Test
  public void testOAuthWithNoAuth() {
    Assertions.assertThatIllegalStateException()
        .isThrownBy(
            () ->
                new GCPProperties(ImmutableMap.of(GCS_OAUTH2_TOKEN, "oauth", GCS_NO_AUTH, "true")))
        .withMessage(
            String.format(
                "Invalid auth settings: must not configure %s and %s",
                GCS_NO_AUTH, GCS_OAUTH2_TOKEN));

    GCPProperties gcpProperties =
        new GCPProperties(ImmutableMap.of(GCS_OAUTH2_TOKEN, "oauth", GCS_NO_AUTH, "false"));
    assertThat(gcpProperties.noAuth()).isFalse();
    assertThat(gcpProperties.oauth2Token()).get().isEqualTo("oauth");
    gcpProperties = new GCPProperties(ImmutableMap.of(GCS_NO_AUTH, "true"));
    assertThat(gcpProperties.noAuth()).isTrue();
    assertThat(gcpProperties.oauth2Token()).isNotPresent();
  }
}
