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
package org.apache.iceberg.rest.requests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestPlanTableScanRequest {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> PlanTableScanRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid request: planTableScanRequest null");

    assertThatThrownBy(() -> PlanTableScanRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid request: planTableScanRequest null");
  }

  @Test
  public void roundTripSerdeWithEmptyObjectAndDefaultsPresent() {
    PlanTableScanRequest request = new PlanTableScanRequest.Builder().build();

    String expectedJson = "{" + "\"case-sensitive\":true," + "\"use-snapshot-schema\":false}";

    String json = PlanTableScanRequestParser.toJson(request, false);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on PlanTableScanRequest because we don't implement
    // equals/hashcode
    assertThat(PlanTableScanRequestParser.toJson(PlanTableScanRequestParser.fromJson(json), false))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithSelectField() {
    PlanTableScanRequest request =
        new PlanTableScanRequest.Builder()
            .withSnapshotId(1L)
            .withSelect(Lists.newArrayList("col1", "col2"))
            .build();

    String expectedJson =
        "{\"snapshot-id\":1,"
            + "\"select\":[\"col1\",\"col2\"],"
            + "\"case-sensitive\":true,"
            + "\"use-snapshot-schema\":false}";

    String json = PlanTableScanRequestParser.toJson(request, false);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on PlanTableScanRequest because we don't implement
    // equals/hashcode
    assertThat(PlanTableScanRequestParser.toJson(PlanTableScanRequestParser.fromJson(json), false))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithFilterField() {
    PlanTableScanRequest request =
        new PlanTableScanRequest.Builder()
            .withSnapshotId(1L)
            .withFilter(Expressions.alwaysFalse())
            .build();

    String expectedJson =
        "{\"snapshot-id\":1,"
            + "\"filter\":\"false\","
            + "\"case-sensitive\":true,"
            + "\"use-snapshot-schema\":false}";

    String json = PlanTableScanRequestParser.toJson(request, false);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on PlanTableScanRequest because we don't implement
    // equals/hashcode
    assertThat(PlanTableScanRequestParser.toJson(PlanTableScanRequestParser.fromJson(json), false))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithAllFieldsInvalidRequest() {
    PlanTableScanRequest request =
        new PlanTableScanRequest.Builder()
            .withSnapshotId(1L)
            .withSelect(Lists.newArrayList("col1", "col2"))
            .withFilter(Expressions.alwaysTrue())
            .withStartSnapshotId(1L)
            .withEndSnapshotId(2L)
            .withCaseSensitive(false)
            .withUseSnapshotSchema(true)
            .withStatsFields(Lists.newArrayList("col1", "col2"))
            .build();

    assertThatThrownBy(() -> PlanTableScanRequestParser.toJson(request))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Either snapshotId must be provided or both startSnapshotId and endSnapshotId must be provided");
  }

  @Test
  public void roundTripSerdeWithAllFieldsExceptSnapShotId() {
    PlanTableScanRequest request =
        new PlanTableScanRequest.Builder()
            .withSelect(Lists.newArrayList("col1", "col2"))
            .withFilter(Expressions.alwaysTrue())
            .withStartSnapshotId(1L)
            .withEndSnapshotId(2L)
            .withCaseSensitive(false)
            .withUseSnapshotSchema(true)
            .withStatsFields(Lists.newArrayList("col1", "col2"))
            .build();

    String expectedJson =
        "{\"start-snapshot-id\":1,"
            + "\"end-snapshot-id\":2,"
            + "\"select\":[\"col1\",\"col2\"],"
            + "\"filter\":\"true\","
            + "\"case-sensitive\":false,"
            + "\"use-snapshot-schema\":true,"
            + "\"stats-fields\":[\"col1\",\"col2\"]}";

    String json = PlanTableScanRequestParser.toJson(request, false);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison on PlanTableScanRequest because we don't implement
    // equals/hashcode
    assertThat(PlanTableScanRequestParser.toJson(PlanTableScanRequestParser.fromJson(json), false))
        .isEqualTo(expectedJson);
  }
}
