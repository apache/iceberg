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
package org.apache.iceberg.rest.labels;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestLabelsParser {

  @Test
  public void nullCheck() {
    assertThatThrownBy(() -> LabelsParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid labels: null");

    assertThatThrownBy(() -> LabelsParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse labels from null object");
  }

  @Test
  public void emptyLabels() {
    Labels labels = ImmutableLabels.builder().build();
    assertThat(labels.isEmpty()).isTrue();

    String expectedJson = "{ }";
    assertThat(LabelsParser.toJson(labels, true)).isEqualTo(expectedJson);

    Labels roundTrip = LabelsParser.fromJson(expectedJson);
    assertThat(roundTrip.objectLabels()).isEmpty();
    assertThat(roundTrip.fields()).isEmpty();

    Labels nonEmpty = ImmutableLabels.builder().objectLabels(ImmutableMap.of("k", "v")).build();
    assertThat(nonEmpty.isEmpty()).isFalse();
  }

  @Test
  public void objectLabelsOnly() {
    Labels labels =
        ImmutableLabels.builder()
            .objectLabels(ImmutableMap.of("owner", "team-a", "sensitivity", "high"))
            .build();

    String expectedJson =
        """
        {
          "objectLabels" : {
            "owner" : "team-a",
            "sensitivity" : "high"
          }
        }""";

    assertThat(LabelsParser.toJson(labels, true)).isEqualTo(expectedJson);
    assertThat(LabelsParser.fromJson(expectedJson)).isEqualTo(labels);
  }

  @Test
  public void fieldLabelsOnly() {
    Labels labels =
        ImmutableLabels.builder()
            .addFields(
                ImmutableFieldLabels.builder()
                    .fieldId(3)
                    .labels(ImmutableMap.of("pii", "true"))
                    .build())
            .build();

    String expectedJson =
        """
        {
          "fields" : [ {
            "field-id" : 3,
            "labels" : {
              "pii" : "true"
            }
          } ]
        }""";

    assertThat(LabelsParser.toJson(labels, true)).isEqualTo(expectedJson);
    assertThat(LabelsParser.fromJson(expectedJson)).isEqualTo(labels);
  }

  @Test
  public void objectAndFieldLabels() {
    Labels labels =
        ImmutableLabels.builder()
            .objectLabels(ImmutableMap.of("owner", "team-a"))
            .addFields(
                ImmutableFieldLabels.builder()
                    .fieldId(1)
                    .labels(ImmutableMap.of("classification", "pii"))
                    .build())
            .addFields(
                ImmutableFieldLabels.builder()
                    .fieldId(2)
                    .labels(ImmutableMap.of("classification", "public"))
                    .build())
            .build();

    String expectedJson =
        """
        {
          "objectLabels" : {
            "owner" : "team-a"
          },
          "fields" : [ {
            "field-id" : 1,
            "labels" : {
              "classification" : "pii"
            }
          }, {
            "field-id" : 2,
            "labels" : {
              "classification" : "public"
            }
          } ]
        }""";

    assertThat(LabelsParser.toJson(labels, true)).isEqualTo(expectedJson);
    assertThat(LabelsParser.fromJson(expectedJson)).isEqualTo(labels);
  }
}
