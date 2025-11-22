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

public class TestPlanTableScanRequestParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> PlanTableScanRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid planTableScanRequest: null");

    assertThatThrownBy(() -> PlanTableScanRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid planTableScanRequest: null");
  }

  @Test
  public void requestWithValidSnapshotIds() {
    PlanTableScanRequest request = PlanTableScanRequest.builder().build();
    assertThat(request).isNotNull();
    assertThat(request.snapshotId()).isNull();
    assertThat(request.startSnapshotId()).isNull();
    assertThat(request.endSnapshotId()).isNull();

    request = PlanTableScanRequest.builder().withSnapshotId(1L).build();
    assertThat(request.snapshotId()).isEqualTo(1L);
    assertThat(request.startSnapshotId()).isNull();
    assertThat(request.endSnapshotId()).isNull();

    request = PlanTableScanRequest.builder().withStartSnapshotId(1L).withEndSnapshotId(5L).build();
    assertThat(request.snapshotId()).isNull();
    assertThat(request.startSnapshotId()).isEqualTo(1L);
    assertThat(request.endSnapshotId()).isEqualTo(5);
  }

  @Test
  public void requestWithInvalidSnapshotIds() {
    assertThatThrownBy(
            () -> PlanTableScanRequest.builder().withSnapshotId(1L).withStartSnapshotId(1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid scan: cannot provide both snapshotId and startSnapshotId/endSnapshotId");

    assertThatThrownBy(
            () -> PlanTableScanRequest.builder().withSnapshotId(1L).withEndSnapshotId(5L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid scan: cannot provide both snapshotId and startSnapshotId/endSnapshotId");

    assertThatThrownBy(() -> PlanTableScanRequest.builder().withStartSnapshotId(1L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid incremental scan: startSnapshotId and endSnapshotId is required");

    assertThatThrownBy(() -> PlanTableScanRequest.builder().withEndSnapshotId(5L).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid incremental scan: startSnapshotId and endSnapshotId is required");
  }

  @Test
  public void roundTripSerdeWithSelectField() {
    PlanTableScanRequest request =
        PlanTableScanRequest.builder()
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
    assertThat(PlanTableScanRequestParser.toJson(PlanTableScanRequestParser.fromJson(json), false))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithFilterField() {
    PlanTableScanRequest request =
        PlanTableScanRequest.builder()
            .withSnapshotId(1L)
            .withFilter(Expressions.alwaysFalse())
            .build();

    String expectedJson =
        "{\"snapshot-id\":1,"
            + "\"filter\":false,"
            + "\"case-sensitive\":true,"
            + "\"use-snapshot-schema\":false}";

    String json = PlanTableScanRequestParser.toJson(request, false);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(PlanTableScanRequestParser.toJson(PlanTableScanRequestParser.fromJson(json), false))
        .isEqualTo(expectedJson);
  }

  @Test
  public void planTableScanRequestWithAllFieldsInvalidRequest() {
    assertThatThrownBy(
            () ->
                PlanTableScanRequest.builder()
                    .withSnapshotId(1L)
                    .withSelect(Lists.newArrayList("col1", "col2"))
                    .withFilter(Expressions.alwaysTrue())
                    .withStartSnapshotId(1L)
                    .withEndSnapshotId(2L)
                    .withCaseSensitive(false)
                    .withUseSnapshotSchema(true)
                    .withStatsFields(Lists.newArrayList("col1", "col2"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid scan: cannot provide both snapshotId and startSnapshotId/endSnapshotId");
  }

  @Test
  public void roundTripSerdeWithAllFieldsExceptSnapShotId() {
    PlanTableScanRequest request =
        PlanTableScanRequest.builder()
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
            + "\"filter\":true,"
            + "\"case-sensitive\":false,"
            + "\"use-snapshot-schema\":true,"
            + "\"stats-fields\":[\"col1\",\"col2\"]}";

    String json = PlanTableScanRequestParser.toJson(request, false);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(PlanTableScanRequestParser.toJson(PlanTableScanRequestParser.fromJson(json), false))
        .isEqualTo(expectedJson);
  }

  @Test
  public void testToStringContainsAllFields() {
    PlanTableScanRequest request =
        PlanTableScanRequest.builder()
            .withSnapshotId(123L)
            .withSelect(Lists.newArrayList("colA", "colB"))
            .withFilter(Expressions.alwaysTrue())
            .withCaseSensitive(false)
            .withUseSnapshotSchema(true)
            .withStatsFields(Lists.newArrayList("stat1"))
            .build();

    String str = request.toString();
    assertThat(str).contains("snapshotId=123");
    assertThat(str).contains("select=[colA, colB]");
    assertThat(str).contains("filter=true");
    assertThat(str).contains("caseSensitive=false");
    assertThat(str).contains("useSnapshotSchema=true");
    assertThat(str).contains("statsFields=[stat1]");
  }

  @Test
  public void roundTripSerdeWithFilterExpression() {
    PlanTableScanRequest request =
        PlanTableScanRequest.builder()
            .withSnapshotId(1L)
            .withFilter(Expressions.equal("id", 1))
            .build();

    String expectedJson =
        "{\"snapshot-id\":1,"
            + "\"filter\":{\"type\":\"eq\",\"term\":\"id\",\"value\":1},"
            + "\"case-sensitive\":true,"
            + "\"use-snapshot-schema\":false}";

    String json = PlanTableScanRequestParser.toJson(request, false);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(PlanTableScanRequestParser.toJson(PlanTableScanRequestParser.fromJson(json), false))
        .isEqualTo(expectedJson);
  }

  @Test
  public void testFilterFieldWithExplicitNullThrowsError() {
    String json = "{\"snapshot-id\":123,\"filter\":null,\"case-sensitive\":true}";

    assertThatThrownBy(() -> PlanTableScanRequestParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse expression from non-object: null");
  }
}
