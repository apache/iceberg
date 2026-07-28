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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestFieldLabelsParser {

  @Test
  public void nullCheck() {
    assertThatThrownBy(() -> FieldLabelsParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field labels: null");

    assertThatThrownBy(() -> FieldLabelsParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse field labels from null object");
  }

  @Test
  public void roundTrip() {
    FieldLabels fieldLabels =
        ImmutableFieldLabels.builder().fieldId(3).labels(ImmutableMap.of("pii", "true")).build();

    String expectedJson =
        """
        {
          "field-id" : 3,
          "labels" : {
            "pii" : "true"
          }
        }""";

    assertThat(FieldLabelsParser.toJson(fieldLabels, true)).isEqualTo(expectedJson);
    assertThat(FieldLabelsParser.fromJson(expectedJson)).isEqualTo(fieldLabels);
  }

  @Test
  public void invalidFieldId() {
    assertThatThrownBy(
            () ->
                ImmutableFieldLabels.builder().fieldId(0).labels(ImmutableMap.of("k", "v")).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field id: must be >= 1");
  }

  @Test
  public void invalidFieldIdFromJson() {
    assertThatThrownBy(
            () -> FieldLabelsParser.fromJson("{\"field-id\": 0, \"labels\": {\"k\": \"v\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field id: must be >= 1");
  }

  @Test
  public void emptyLabels() {
    FieldLabels fieldLabels = ImmutableFieldLabels.builder().fieldId(1).build();

    assertThat(fieldLabels.labels()).isEmpty();
  }

  @Test
  public void emptyLabelsFromJson() {
    FieldLabels fieldLabels = FieldLabelsParser.fromJson("{\"field-id\": 1, \"labels\": {}}");

    assertThat(fieldLabels.fieldId()).isEqualTo(1);
    assertThat(fieldLabels.labels()).isEmpty();
  }

  @Test
  public void missingLabelsFromJson() {
    assertThatThrownBy(() -> FieldLabelsParser.fromJson("{\"field-id\": 1}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing map: labels");
  }
}
