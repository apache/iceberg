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
import static org.apache.iceberg.TestBase.FILE_B;
import static org.apache.iceberg.TestBase.FILE_B_DELETES;
import static org.apache.iceberg.TestBase.FILE_C;
import static org.apache.iceberg.TestBase.FILE_C2_DELETES;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
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
  public void roundTripSerdeWithCompletedPlanningWithAndWithoutPlanId() {
    PlanTableScanResponse response =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .build();
    assertThat(response.planStatus()).isEqualTo(PlanStatus.COMPLETED);
    assertThat(response.planId()).isNull();
    assertThat(PlanTableScanResponseParser.toJson(response))
        .isEqualTo("{\"status\":\"completed\"}");

    response =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withPlanId("somePlanId")
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .build();
    assertThat(response.planStatus()).isEqualTo(PlanStatus.COMPLETED);
    assertThat(response.planId()).isEqualTo("somePlanId");

    assertThat(PlanTableScanResponseParser.toJson(response))
        .isEqualTo("{\"status\":\"completed\",\"plan-id\":\"somePlanId\"}");
  }

  @Test
  public void roundTripSerdeWithSubmittedPlanningWithPlanId() {
    PlanTableScanResponse response =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.SUBMITTED)
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .withPlanId("somePlanId")
            .build();
    assertThat(response.planStatus()).isEqualTo(PlanStatus.SUBMITTED);
    assertThat(response.planId()).isEqualTo("somePlanId");
    assertThat(PlanTableScanResponseParser.toJson(response))
        .isEqualTo("{\"status\":\"submitted\",\"plan-id\":\"somePlanId\"}");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatus() {
    String invalidStatusJson = "{\"status\": \"someStatus\"}";
    assertThatThrownBy(
            () ->
                PlanTableScanResponseParser.fromJson(
                    invalidStatusJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid status name: someStatus");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatusSubmittedWithoutPlanId() {
    assertThatThrownBy(
            () -> PlanTableScanResponse.builder().withPlanStatus(PlanStatus.SUBMITTED).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: plan id should be defined when status is 'submitted'");

    String invalidJson = "{\"status\":\"submitted\"}";
    assertThatThrownBy(
            () -> PlanTableScanResponseParser.fromJson(invalidJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: plan id should be defined when status is 'submitted'");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatusCancelled() {
    assertThatThrownBy(
            () -> PlanTableScanResponse.builder().withPlanStatus(PlanStatus.CANCELLED).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: 'cancelled' is not a valid status for planTableScan");

    String invalidJson = "{\"status\":\"cancelled\"}";
    assertThatThrownBy(
            () -> PlanTableScanResponseParser.fromJson(invalidJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: 'cancelled' is not a valid status for planTableScan");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatusSubmittedWithTasksPresent() {
    assertThatThrownBy(
            () ->
                PlanTableScanResponse.builder()
                    .withPlanStatus(PlanStatus.SUBMITTED)
                    .withPlanId("somePlanId")
                    .withPlanTasks(List.of("task1", "task2"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: tasks can only be defined when status is 'completed'");

    String invalidJson =
        "{\"status\":\"submitted\","
            + "\"plan-id\":\"somePlanId\","
            + "\"plan-tasks\":[\"task1\",\"task2\"]}";

    assertThatThrownBy(
            () -> PlanTableScanResponseParser.fromJson(invalidJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: tasks can only be defined when status is 'completed'");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanIdWithIncorrectStatus() {
    assertThatThrownBy(
            () ->
                PlanTableScanResponse.builder()
                    .withPlanStatus(PlanStatus.FAILED)
                    .withPlanId("somePlanId")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid response: plan id can only be defined when status is 'submitted' or 'completed'");

    String invalidJson = "{\"status\":\"failed\"," + "\"plan-id\":\"somePlanId\"}";

    assertThatThrownBy(
            () -> PlanTableScanResponseParser.fromJson(invalidJson, PARTITION_SPECS_BY_ID, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid response: plan id can only be defined when status is 'submitted' or 'completed'");
  }

  @Test
  public void roundTripSerdeWithInvalidPlanStatusSubmittedWithDeleteFilesNoFileScanTasksPresent() {
    assertThatThrownBy(
            () ->
                PlanTableScanResponse.builder()
                    .withPlanStatus(PlanStatus.SUBMITTED)
                    .withPlanId("somePlanId")
                    .withDeleteFiles(List.of(FILE_A_DELETES))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");

    String invalidJson =
        "{\"status\":\"submitted\","
            + "\"plan-id\":\"somePlanId\","
            + "\"delete-files\":[{\"spec-id\":0,\"content\":\"position-deletes\","
            + "\"file-path\":\"/path/to/data-a-deletes.parquet\",\"file-format\":\"parquet\","
            + "\"partition\":[0],\"file-size-in-bytes\":10,\"record-count\":1}]"
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

    PlanTableScanResponse response =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withFileScanTasks(List.of(fileScanTask))
            .withDeleteFiles(List.of(FILE_A_DELETES))
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .build();

    String expectedToJson =
        "{\"status\":\"completed\","
            + "\"delete-files\":[{\"spec-id\":0,\"content\":\"position-deletes\","
            + "\"file-path\":\"/path/to/data-a-deletes.parquet\",\"file-format\":\"parquet\","
            + "\"partition\":[0],\"file-size-in-bytes\":10,\"record-count\":1}],"
            + "\"file-scan-tasks\":["
            + "{\"data-file\":{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data-a.parquet\","
            + "\"file-format\":\"parquet\",\"partition\":[0],"
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

  @Test
  public void multipleTasksWithDifferentDeleteFilesDontAccumulateReferences() {
    ResidualEvaluator residualEvaluator =
        ResidualEvaluator.of(SPEC, Expressions.alwaysTrue(), true);

    // Create three tasks, each with its own distinct delete file
    FileScanTask taskA =
        new BaseFileScanTask(
            FILE_A,
            new DeleteFile[] {FILE_A_DELETES},
            SchemaParser.toJson(SCHEMA),
            PartitionSpecParser.toJson(SPEC),
            residualEvaluator);

    FileScanTask taskB =
        new BaseFileScanTask(
            FILE_B,
            new DeleteFile[] {FILE_B_DELETES},
            SchemaParser.toJson(SCHEMA),
            PartitionSpecParser.toJson(SPEC),
            residualEvaluator);

    FileScanTask taskC =
        new BaseFileScanTask(
            FILE_C,
            new DeleteFile[] {FILE_C2_DELETES},
            SchemaParser.toJson(SCHEMA),
            PartitionSpecParser.toJson(SPEC),
            residualEvaluator);

    PlanTableScanResponse response =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withFileScanTasks(List.of(taskA, taskB, taskC))
            .withDeleteFiles(List.of(FILE_A_DELETES, FILE_B_DELETES, FILE_C2_DELETES))
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .build();

    String expectedJson =
        "{\n"
            + "  \"status\" : \"completed\",\n"
            + "  \"delete-files\" : [ {\n"
            + "    \"spec-id\" : 0,\n"
            + "    \"content\" : \"position-deletes\",\n"
            + "    \"file-path\" : \"/path/to/data-a-deletes.parquet\",\n"
            + "    \"file-format\" : \"parquet\",\n"
            + "    \"partition\" : [ 0 ],\n"
            + "    \"file-size-in-bytes\" : 10,\n"
            + "    \"record-count\" : 1\n"
            + "  }, {\n"
            + "    \"spec-id\" : 0,\n"
            + "    \"content\" : \"position-deletes\",\n"
            + "    \"file-path\" : \"/path/to/data-b-deletes.parquet\",\n"
            + "    \"file-format\" : \"parquet\",\n"
            + "    \"partition\" : [ 1 ],\n"
            + "    \"file-size-in-bytes\" : 10,\n"
            + "    \"record-count\" : 1\n"
            + "  }, {\n"
            + "    \"spec-id\" : 0,\n"
            + "    \"content\" : \"equality-deletes\",\n"
            + "    \"file-path\" : \"/path/to/data-c-deletes.parquet\",\n"
            + "    \"file-format\" : \"parquet\",\n"
            + "    \"partition\" : [ 2 ],\n"
            + "    \"file-size-in-bytes\" : 10,\n"
            + "    \"record-count\" : 1,\n"
            + "    \"equality-ids\" : [ 1 ],\n"
            + "    \"sort-order-id\" : 0\n"
            + "  } ],\n"
            + "  \"file-scan-tasks\" : [ {\n"
            + "    \"data-file\" : {\n"
            + "      \"spec-id\" : 0,\n"
            + "      \"content\" : \"data\",\n"
            + "      \"file-path\" : \"/path/to/data-a.parquet\",\n"
            + "      \"file-format\" : \"parquet\",\n"
            + "      \"partition\" : [ 0 ],\n"
            + "      \"file-size-in-bytes\" : 10,\n"
            + "      \"record-count\" : 1,\n"
            + "      \"sort-order-id\" : 0\n"
            + "    },\n"
            + "    \"delete-file-references\" : [ 0 ],\n"
            + "    \"residual-filter\" : true\n"
            + "  }, {\n"
            + "    \"data-file\" : {\n"
            + "      \"spec-id\" : 0,\n"
            + "      \"content\" : \"data\",\n"
            + "      \"file-path\" : \"/path/to/data-b.parquet\",\n"
            + "      \"file-format\" : \"parquet\",\n"
            + "      \"partition\" : [ 1 ],\n"
            + "      \"file-size-in-bytes\" : 10,\n"
            + "      \"record-count\" : 1,\n"
            + "      \"split-offsets\" : [ 1 ],\n"
            + "      \"sort-order-id\" : 0\n"
            + "    },\n"
            + "    \"delete-file-references\" : [ 1 ],\n"
            + "    \"residual-filter\" : true\n"
            + "  }, {\n"
            + "    \"data-file\" : {\n"
            + "      \"spec-id\" : 0,\n"
            + "      \"content\" : \"data\",\n"
            + "      \"file-path\" : \"/path/to/data-c.parquet\",\n"
            + "      \"file-format\" : \"parquet\",\n"
            + "      \"partition\" : [ 2 ],\n"
            + "      \"file-size-in-bytes\" : 10,\n"
            + "      \"record-count\" : 1,\n"
            + "      \"split-offsets\" : [ 2, 8 ],\n"
            + "      \"sort-order-id\" : 0\n"
            + "    },\n"
            + "    \"delete-file-references\" : [ 2 ],\n"
            + "    \"residual-filter\" : true\n"
            + "  } ]\n"
            + "}";
    String json = PlanTableScanResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithoutDeleteFiles() {
    ResidualEvaluator residualEvaluator =
        ResidualEvaluator.of(SPEC, Expressions.equal("id", 1), true);
    FileScanTask fileScanTask =
        new BaseFileScanTask(
            FILE_A,
            new DeleteFile[] {},
            SchemaParser.toJson(SCHEMA),
            PartitionSpecParser.toJson(SPEC),
            residualEvaluator);
    PlanTableScanResponse response =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withFileScanTasks(List.of(fileScanTask))
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .build();

    String expectedJson =
        "{\"status\":\"completed\","
            + "\"file-scan-tasks\":["
            + "{\"data-file\":{\"spec-id\":0,\"content\":\"data\",\"file-path\":\"/path/to/data-a.parquet\","
            + "\"file-format\":\"parquet\",\"partition\":[0],"
            + "\"file-size-in-bytes\":10,\"record-count\":1,\"sort-order-id\":0},"
            + "\"residual-filter\":{\"type\":\"eq\",\"term\":\"id\",\"value\":1}}]"
            + "}";

    String json = PlanTableScanResponseParser.toJson(response);
    assertThat(json).isEqualTo(expectedJson);

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

    assertThat(PlanTableScanResponseParser.toJson(copyResponse)).isEqualTo(expectedJson);
  }

  @Test
  public void emptyOrInvalidCredentials() {
    assertThat(
            PlanTableScanResponseParser.fromJson(
                    "{\"status\": \"completed\",\"storage-credentials\": null}",
                    PARTITION_SPECS_BY_ID,
                    false)
                .credentials())
        .isEmpty();

    assertThat(
            PlanTableScanResponseParser.fromJson(
                    "{\"status\": \"completed\",\"storage-credentials\": []}",
                    PARTITION_SPECS_BY_ID,
                    false)
                .credentials())
        .isEmpty();

    assertThatThrownBy(
            () ->
                PlanTableScanResponseParser.fromJson(
                    "{\"status\": \"completed\",\"storage-credentials\": \"invalid\"}",
                    PARTITION_SPECS_BY_ID,
                    false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse credentials from non-array: \"invalid\"");
  }

  @Test
  public void roundTripSerdeWithCredentials() {
    List<Credential> credentials =
        ImmutableList.of(
            ImmutableCredential.builder()
                .prefix("s3://custom-uri")
                .config(
                    ImmutableMap.of(
                        "s3.access-key-id",
                        "keyId",
                        "s3.secret-access-key",
                        "accessKey",
                        "s3.session-token",
                        "sessionToken"))
                .build(),
            ImmutableCredential.builder()
                .prefix("gs://custom-uri")
                .config(
                    ImmutableMap.of(
                        "gcs.oauth2.token", "gcsToken1", "gcs.oauth2.token-expires-at", "1000"))
                .build(),
            ImmutableCredential.builder()
                .prefix("gs")
                .config(
                    ImmutableMap.of(
                        "gcs.oauth2.token", "gcsToken2", "gcs.oauth2.token-expires-at", "2000"))
                .build());

    PlanTableScanResponse response =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withCredentials(credentials)
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .build();

    String expectedJson =
        "{\n"
            + "  \"status\" : \"completed\",\n"
            + "  \"storage-credentials\" : [ {\n"
            + "    \"prefix\" : \"s3://custom-uri\",\n"
            + "    \"config\" : {\n"
            + "      \"s3.access-key-id\" : \"keyId\",\n"
            + "      \"s3.secret-access-key\" : \"accessKey\",\n"
            + "      \"s3.session-token\" : \"sessionToken\"\n"
            + "    }\n"
            + "  }, {\n"
            + "    \"prefix\" : \"gs://custom-uri\",\n"
            + "    \"config\" : {\n"
            + "      \"gcs.oauth2.token\" : \"gcsToken1\",\n"
            + "      \"gcs.oauth2.token-expires-at\" : \"1000\"\n"
            + "    }\n"
            + "  }, {\n"
            + "    \"prefix\" : \"gs\",\n"
            + "    \"config\" : {\n"
            + "      \"gcs.oauth2.token\" : \"gcsToken2\",\n"
            + "      \"gcs.oauth2.token-expires-at\" : \"2000\"\n"
            + "    }\n"
            + "  } ]\n"
            + "}";

    String json = PlanTableScanResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);

    PlanTableScanResponse fromResponse =
        PlanTableScanResponseParser.fromJson(json, PARTITION_SPECS_BY_ID, false);
    PlanTableScanResponse copyResponse =
        PlanTableScanResponse.builder()
            .withPlanStatus(fromResponse.planStatus())
            .withPlanId(fromResponse.planId())
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .withCredentials(credentials)
            .build();

    assertThat(PlanTableScanResponseParser.toJson(copyResponse, true)).isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithValidStatusAndFileScanTasksAndCredentials() {
    ResidualEvaluator residualEvaluator =
        ResidualEvaluator.of(SPEC, Expressions.equal("id", 1), true);
    FileScanTask fileScanTask =
        new BaseFileScanTask(
            FILE_A,
            new DeleteFile[] {FILE_A_DELETES},
            SchemaParser.toJson(SCHEMA),
            PartitionSpecParser.toJson(SPEC),
            residualEvaluator);

    List<Credential> credentials =
        ImmutableList.of(
            ImmutableCredential.builder()
                .prefix("s3://custom-uri")
                .config(
                    ImmutableMap.of(
                        "s3.access-key-id",
                        "keyId",
                        "s3.secret-access-key",
                        "accessKey",
                        "s3.session-token",
                        "sessionToken"))
                .build(),
            ImmutableCredential.builder()
                .prefix("gs://custom-uri")
                .config(
                    ImmutableMap.of(
                        "gcs.oauth2.token", "gcsToken1", "gcs.oauth2.token-expires-at", "1000"))
                .build(),
            ImmutableCredential.builder()
                .prefix("gs")
                .config(
                    ImmutableMap.of(
                        "gcs.oauth2.token", "gcsToken2", "gcs.oauth2.token-expires-at", "2000"))
                .build());
    PlanTableScanResponse response =
        PlanTableScanResponse.builder()
            .withPlanStatus(PlanStatus.COMPLETED)
            .withFileScanTasks(List.of(fileScanTask))
            .withDeleteFiles(List.of(FILE_A_DELETES))
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .withCredentials(credentials)
            .build();

    String expectedJson =
        "{\n"
            + "  \"status\" : \"completed\",\n"
            + "  \"storage-credentials\" : [ {\n"
            + "    \"prefix\" : \"s3://custom-uri\",\n"
            + "    \"config\" : {\n"
            + "      \"s3.access-key-id\" : \"keyId\",\n"
            + "      \"s3.secret-access-key\" : \"accessKey\",\n"
            + "      \"s3.session-token\" : \"sessionToken\"\n"
            + "    }\n"
            + "  }, {\n"
            + "    \"prefix\" : \"gs://custom-uri\",\n"
            + "    \"config\" : {\n"
            + "      \"gcs.oauth2.token\" : \"gcsToken1\",\n"
            + "      \"gcs.oauth2.token-expires-at\" : \"1000\"\n"
            + "    }\n"
            + "  }, {\n"
            + "    \"prefix\" : \"gs\",\n"
            + "    \"config\" : {\n"
            + "      \"gcs.oauth2.token\" : \"gcsToken2\",\n"
            + "      \"gcs.oauth2.token-expires-at\" : \"2000\"\n"
            + "    }\n"
            + "  } ],\n"
            + "  \"delete-files\" : [ {\n"
            + "    \"spec-id\" : 0,\n"
            + "    \"content\" : \"position-deletes\",\n"
            + "    \"file-path\" : \"/path/to/data-a-deletes.parquet\",\n"
            + "    \"file-format\" : \"parquet\",\n"
            + "    \"partition\" : [ 0 ],\n"
            + "    \"file-size-in-bytes\" : 10,\n"
            + "    \"record-count\" : 1\n"
            + "  } ],\n"
            + "  \"file-scan-tasks\" : [ {\n"
            + "    \"data-file\" : {\n"
            + "      \"spec-id\" : 0,\n"
            + "      \"content\" : \"data\",\n"
            + "      \"file-path\" : \"/path/to/data-a.parquet\",\n"
            + "      \"file-format\" : \"parquet\",\n"
            + "      \"partition\" : [ 0 ],\n"
            + "      \"file-size-in-bytes\" : 10,\n"
            + "      \"record-count\" : 1,\n"
            + "      \"sort-order-id\" : 0\n"
            + "    },\n"
            + "    \"delete-file-references\" : [ 0 ],\n"
            + "    \"residual-filter\" : {\n"
            + "      \"type\" : \"eq\",\n"
            + "      \"term\" : \"id\",\n"
            + "      \"value\" : 1\n"
            + "    }\n"
            + "  } ]\n"
            + "}";

    String json = PlanTableScanResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);

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
            .withCredentials(credentials)
            .build();

    assertThat(PlanTableScanResponseParser.toJson(copyResponse, true)).isEqualTo(expectedJson);
  }
}
