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

class TestFieldLabelParser {

  @Test
  void nullCheck() {
    assertThatThrownBy(() -> FieldLabelParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field labels: null");

    assertThatThrownBy(() -> FieldLabelParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse field labels from null object");
  }

  @Test
  void roundTrip() {
    FieldLabel fieldLabel =
        ImmutableFieldLabel.builder().fieldId(3).labels(ImmutableMap.of("pii", "true")).build();

    String expectedJson =
        """
        {
          "field-id" : 3,
          "labels" : {
            "pii" : "true"
          }
        }""";

    assertThat(FieldLabelParser.toJson(fieldLabel, true)).isEqualTo(expectedJson);
    assertThat(FieldLabelParser.fromJson(expectedJson)).isEqualTo(fieldLabel);
  }

  @Test
  void emptyLabels() {
    assertThat(ImmutableFieldLabel.builder().fieldId(1).build().labels()).isEmpty();
  }

  @Test
  void emptyLabelsFromJson() {
    FieldLabel fieldLabel = FieldLabelParser.fromJson("{\"field-id\": 1, \"labels\": {}}");

    assertThat(fieldLabel.fieldId()).isEqualTo(1);
    assertThat(fieldLabel.labels()).isEmpty();
  }

  @Test
  void missingLabelsFromJson() {
    assertThatThrownBy(() -> FieldLabelParser.fromJson("{\"field-id\": 1}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing map: labels");
  }

  @Test
  void emptyJson() {
    assertThatThrownBy(() -> FieldLabelParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: field-id");
  }
}
