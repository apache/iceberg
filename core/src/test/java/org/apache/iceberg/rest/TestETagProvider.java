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

import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestETagProvider {
  static final String METADATA_LOCATION =
      "/var/folders/20/290st0_52y5fyjcj2mlg49500000gn/T/junit-3064022805908958416/db_name/tbl_name/metadata/00000-f7a7956e-61d0-499b-be60-b141283f8229.metadata.json";

  @Test
  public void testNullInput() {
    assertThatThrownBy(() -> ETagProvider.of(null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid metadata location: null");
  }

  @Test
  public void testEmptyInput() {
    assertThatThrownBy(() -> ETagProvider.of("", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid metadata location: empty");
  }

  @Test
  public void testETagContent() {
    assertThat("90b8ad4e")
        .isEqualTo(
            ETagProvider.of(METADATA_LOCATION, Map.of("param1", "value1", "param2", "value2")));

    assertThat("cb787e6a")
        .isEqualTo(
            ETagProvider.of(
                METADATA_LOCATION, Map.of("param1", "other_value1", "param2", "other_value2")));

    assertThat("55faa5d9").isEqualTo(ETagProvider.of("/short/path", null));

    assertThat("55faa5d9").isEqualTo(ETagProvider.of("/short/path", Map.of()));

    assertThat("8adf3766").isEqualTo(ETagProvider.of("/short/path", Map.of("param", "some_value")));
  }

  @Test
  public void testDifferentParameterOrderGiveSameETag() {
    assertThat(ETagProvider.of(METADATA_LOCATION, Map.of("param1", "value1", "param2", "value2")))
        .isEqualTo(
            ETagProvider.of(METADATA_LOCATION, Map.of("param2", "value2", "param1", "value1")));
  }
}
