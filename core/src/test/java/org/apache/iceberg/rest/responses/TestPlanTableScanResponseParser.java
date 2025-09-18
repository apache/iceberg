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
package org.apache.iceberg.rest.responses;

import static org.apache.iceberg.TestBase.FILE_A;
import static org.apache.iceberg.TestBase.FILE_A_DELETES;
import static org.apache.iceberg.TestBase.PARTITION_SPECS_BY_ID;
import static org.apache.iceberg.TestBase.SCHEMA;
import static org.apache.iceberg.TestBase.SPEC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.rest.PlanStatus;
import org.junit.jupiter.api.Test;

public class TestPlanTableScanResponseParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> PlanTableScanResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: planTableScanResponse null");

    assertThatThrownBy(
            () -> PlanTableScanResponseParser.fromJson((String) null, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse planTableScan response from empty or null object");
  }

  @Test
  public void roundTripSerdeWithEmptyObject() {

    assertThatThrownBy(() -> PlanTableScanResponse.builder().build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: plan status must be defined");

    String emptyJson = "{ }";
    assertThatThrownBy(
            () -> PlanTableScanResponseParser.fromJson(emptyJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse planTableScan response from empty or null object");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatus() {
    String invalidStatusJson = "{\"plan-status\": \"someStatus\"}";
    assertThatThrownBy(
            () ->
                PlanTableScanResponseParser.fromJson(
                    invalidStatusJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid status name: someStatus");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatusSubmittedWithoutPlanId() {
    PlanStatus planStatus = PlanStatus.fromName("submitted");

    assertThatThrownBy(() -> PlanTableScanResponse.builder().withPlanStatus(planStatus).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: plan id should be defined when status is 'submitted'");

    String invalidJson = "{\"plan-status\":\"submitted\"}";
    assertThatThrownBy(
            () -> PlanTableScanResponseParser.fromJson(invalidJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: plan id should be defined when status is 'submitted'");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatusCancelled() {
    PlanStatus planStatus = PlanStatus.fromName("cancelled");
    assertThatThrownBy(() -> PlanTableScanResponse.builder().withPlanStatus(planStatus).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: 'cancelled' is not a valid status for planTableScan");

    String invalidJson = "{\"plan-status\":\"cancelled\"}";
    assertThatThrownBy(
            () -> PlanTableScanResponseParser.fromJson(invalidJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: 'cancelled' is not a valid status for planTableScan");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatusSubmittedWithTasksPresent() {
    PlanStatus planStatus = PlanStatus.fromName("submitted");
    assertThatThrownBy(
            () ->
                PlanTableScanResponse.builder()
                    .withPlanStatus(planStatus)
                    .withPlanId("somePlanId")
                    .withPlanTasks(List.of("task1", "task2"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: tasks can only be defined when status is 'completed'");

    String invalidJson =
        "{\"plan-status\":\"submitted\","
            + "\"plan-id\":\"somePlanId\","
            + "\"plan-tasks\":[\"task1\",\"task2\"]}";

    assertThatThrownBy(
            () -> PlanTableScanResponseParser.fromJson(invalidJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: tasks can only be defined when status is 'completed'");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanIdWithIncorrectStatus() {
    PlanStatus planStatus = PlanStatus.fromName("failed");
    assertThatThrownBy(
            () ->
                PlanTableScanResponse.builder()
                    .withPlanStatus(planStatus)
                    .withPlanId("somePlanId")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: plan id can only be defined when status is 'submitted'");

    String invalidJson = "{\"plan-status\":\"failed\"," + "\"plan-id\":\"somePlanId\"}";

    assertThatThrownBy(
            () -> PlanTableScanResponseParser.fromJson(invalidJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: plan id can only be defined when status is 'submitted'");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatusSubmittedWithDeleteFilesNoFileScanTasksPresent() {
    PlanStatus planStatus = PlanStatus.fromName("submitted");
    assertThatThrownBy(
            () ->
                PlanTableScanResponse.builder()
                    .withPlanStatus(planStatus)
                    .withPlanId("somePlanId")
                    .withDeleteFiles(List.of(FILE_A_DELETES))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");

    String invalidJson =
        "{\"plan-status\":\"submitted\","
            + "\"plan-id\":\"somePlanId\","
            + "\"delete-files\":[{\"spec-id\":0,\"content\":\"POSITION_DELETES\","
            + "\"file-path\":\"/path/to/data-a-deletes.parquet\",\"file-format\":\"PARQUET\","
            + "\"partition\":{\"1000\":0},\"file-size-in-bytes\":10,\"record-count\":1}]"
            + "}";

    assertThatThrownBy(
            () -> PlanTableScanResponseParser.fromJson(invalidJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");
  }

  @Test
  public void roundTripSerdeWithValidStatusAndFileScanTasks() {
    ResidualEvaluator residualEvaluator =
        ResidualEvaluator.of(SPEC, Expressions.equal("id", 1), true);
    FileScanTask fileScanTask =
        new BaseFileScanTask(
            FILE_A,
            new DeleteFile[] {FILE_A_DELETES},
            SchemaParser.toJson(SCHEMA),
            PartitionSpecParser.toJson(SPEC),
            residualEvaluator);

    PlanStatus planStatus = PlanStatus.fromName("completed");
    PlanTableScanResponse response =
        PlanTableScanResponse.builder()
            .withPlanStatus(planStatus)
            .withFileScanTasks(List.of(fileScanTask))
            .withDeleteFiles(List.of(FILE_A_DELETES))
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .build();

    String expectedToJson =
        "{\"plan-status\":\"completed\","
            + "\"delete-files\":[{\"spec-id\":0,\"content\":\"POSITION_DELETES\","
            + "\"file-path\":\"/path/to/data-a-deletes.parquet\",\"file-format\":\"PARQUET\","
            + "\"partition\":{\"1000\":0},\"file-size-in-bytes\":10,\"record-count\":1}],"
            + "\"file-scan-tasks\":["
            + "{\"data-file\":{\"spec-id\":0,\"content\":\"DATA\",\"file-path\":\"/path/to/data-a.parquet\","
            + "\"file-format\":\"PARQUET\",\"partition\":{\"1000\":0},"
            + "\"file-size-in-bytes\":10,\"record-count\":1,\"sort-order-id\":0},"
            + "\"delete-file-references\":[0],"
            + "\"residual-filter\":{\"type\":\"eq\",\"term\":\"id\",\"value\":1}}]"
            + "}";

    String json = PlanTableScanResponseParser.toJson(response);
    assertThat(json).isEqualTo(expectedToJson);

    PlanTableScanResponse fromResponse =
        PlanTableScanResponseParser.fromJson(json, PARTITION_SPECS_BY_ID, false);
    PlanTableScanResponse copyResponse =
        PlanTableScanResponse.builder()
            .withPlanStatus(fromResponse.planStatus())
            .withPlanId(fromResponse.planId())
            .withPlanTasks(fromResponse.planTasks())
            .withDeleteFiles(fromResponse.deleteFiles())
            .withFileScanTasks(fromResponse.fileScanTasks())
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .build();

    assertThat(PlanTableScanResponseParser.toJson(copyResponse)).isEqualTo(expectedToJson);
  }
}
