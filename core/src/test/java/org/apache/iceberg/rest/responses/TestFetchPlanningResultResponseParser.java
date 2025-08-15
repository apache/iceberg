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

import com.fasterxml.jackson.databind.JsonNode;
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

public class TestFetchPlanningResultResponseParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> FetchPlanningResultResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: fetchPanningResultResponse null");

    assertThatThrownBy(() -> FetchPlanningResultResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: fetchPanningResultResponse null or empty");
  }

  @Test
  public void roundTripSerdeWithEmptyObject() {
    assertThatThrownBy(
            () ->
                FetchPlanningResultResponseParser.toJson(
                    FetchPlanningResultResponse.builder().build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid status: null");

    String emptyJson = "{ }";
    assertThatThrownBy(() -> FetchPlanningResultResponseParser.fromJson(emptyJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: fetchPanningResultResponse null or empty");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatus() {
    String invalidStatusJson = "{\"plan-status\": \"someStatus\"}";
    assertThatThrownBy(() -> FetchPlanningResultResponseParser.fromJson(invalidStatusJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid status name: someStatus");
  }

  @Test
  public void roundTripSerdeWithValidSubmittedStatus() {
    PlanStatus planStatus = PlanStatus.fromName("submitted");
    FetchPlanningResultResponse response =
        FetchPlanningResultResponse.builder().withPlanStatus(planStatus).build();

    String expectedJson = "{\"plan-status\":\"submitted\"}";
    String json = FetchPlanningResultResponseParser.toJson(response);
    assertThat(json).isEqualTo(expectedJson);

    FetchPlanningResultResponse fromResponse = FetchPlanningResultResponseParser.fromJson(json);
    assertThat(FetchPlanningResultResponseParser.toJson(fromResponse)).isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatusSubmittedWithTasksPresent() {
    PlanStatus planStatus = PlanStatus.fromName("submitted");
    assertThatThrownBy(
            () ->
                FetchPlanningResultResponse.builder()
                    .withPlanStatus(planStatus)
                    .withPlanTasks(List.of("task1", "task2"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: tasks can only be returned in a 'completed' status");

    String invalidJson =
        "{\"plan-status\":\"submitted\"," + "\"plan-tasks\":[\"task1\",\"task2\"]}";

    assertThatThrownBy(() -> FetchPlanningResultResponseParser.fromJson(invalidJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: tasks can only be returned in a 'completed' status");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatusSubmittedWithDeleteFilesNoFileScanTasksPresent() {
    PlanStatus planStatus = PlanStatus.fromName("submitted");
    assertThatThrownBy(
            () ->
                FetchPlanningResultResponse.builder()
                    .withPlanStatus(planStatus)
                    .withDeleteFiles(List.of(FILE_A_DELETES))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");

    String invalidJson =
        "{\"plan-status\":\"submitted\","
            + "\"delete-files\":[{\"spec-id\":0,\"content\":\"POSITION_DELETES\","
            + "\"file-path\":\"/path/to/data-a-deletes.parquet\",\"file-format\":\"PARQUET\","
            + "\"partition\":{\"1000\":0},\"file-size-in-bytes\":10,\"record-count\":1}]"
            + "}";

    assertThatThrownBy(() -> FetchPlanningResultResponseParser.fromJson(invalidJson))
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
    FetchPlanningResultResponse response =
        FetchPlanningResultResponse.builder()
            .withPlanStatus(planStatus)
            .withFileScanTasks(List.of(fileScanTask))
            .withDeleteFiles(List.of(FILE_A_DELETES))
            // assume this has been set
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

    String json = FetchPlanningResultResponseParser.toJson(response, false);
    assertThat(json).isEqualTo(expectedToJson);

    // make an unbound json where you expect to not have partitions for the data file,
    // delete files as service does not send partition spec
    String expectedFromJson =
        "{\"plan-status\":\"completed\","
            + "\"delete-files\":[{\"spec-id\":0,\"content\":\"POSITION_DELETES\","
            + "\"file-path\":\"/path/to/data-a-deletes.parquet\",\"file-format\":\"PARQUET\","
            + "\"partition\":{},\"file-size-in-bytes\":10,\"record-count\":1}],"
            + "\"file-scan-tasks\":["
            + "{\"data-file\":{\"spec-id\":0,\"content\":\"DATA\",\"file-path\":\"/path/to/data-a.parquet\","
            + "\"file-format\":\"PARQUET\",\"partition\":{},"
            + "\"file-size-in-bytes\":10,\"record-count\":1,\"sort-order-id\":0},"
            + "\"delete-file-references\":[0],"
            + "\"residual-filter\":{\"type\":\"eq\",\"term\":\"id\",\"value\":1}}]"
            + "}";

    FetchPlanningResultResponse fromResponse = FetchPlanningResultResponseParser.fromJson(json);
    // Need to make a new response with partitionSpec set
    FetchPlanningResultResponse copyResponse =
        FetchPlanningResultResponse.builder()
            .withPlanStatus(fromResponse.planStatus())
            .withPlanTasks(fromResponse.planTasks())
            .withDeleteFiles(fromResponse.deleteFiles())
            .withFileScanTasks(fromResponse.fileScanTasks())
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .build();

    // can't do an equality comparison on PlanTableScanRequest because we don't implement
    // equals/hashcode
    assertThat(FetchPlanningResultResponseParser.toJson(copyResponse, false))
        .isEqualTo(expectedFromJson);
  }
}
