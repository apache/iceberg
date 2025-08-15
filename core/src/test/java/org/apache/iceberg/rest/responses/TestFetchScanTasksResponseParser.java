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
import org.junit.jupiter.api.Test;

public class TestFetchScanTasksResponseParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> FetchScanTasksResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: fetchScanTasksResponse null");

    assertThatThrownBy(() -> FetchScanTasksResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: fetchScanTasksResponse null");
  }

  @Test
  public void roundTripSerdeWithEmptyObject() {
    assertThatThrownBy(
            () -> FetchScanTasksResponseParser.toJson(FetchScanTasksResponse.builder().build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: planTasks and fileScanTask cannot both be null");

    String emptyJson = "{ }";
    assertThatThrownBy(() -> FetchScanTasksResponseParser.fromJson(emptyJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid response: fetchScanTasksResponse null");
  }

  @Test
  public void roundTripSerdeWithPlanTasks() {
    String expectedJson = "{\"plan-tasks\":[\"task1\",\"task2\"]}";
    String json =
        FetchScanTasksResponseParser.toJson(
            FetchScanTasksResponse.builder().withPlanTasks(List.of("task1", "task2")).build());
    assertThat(json).isEqualTo(expectedJson);

    FetchScanTasksResponse fromResponse = FetchScanTasksResponseParser.fromJson(json);

    // can't do an equality comparison on PlanTableScanRequest because we don't implement
    // equals/hashcode
    assertThat(FetchScanTasksResponseParser.toJson(fromResponse, false)).isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithDeleteFilesNoFileScanTasksPresent() {
    assertThatThrownBy(
            () ->
                FetchScanTasksResponse.builder()
                    .withPlanTasks(List.of("task1", "task2"))
                    .withDeleteFiles(List.of(FILE_A_DELETES))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");

    String invalidJson =
        "{\"plan-tasks\":[\"task1\",\"task2\"],"
            + "\"delete-files\":[{\"spec-id\":0,\"content\":\"POSITION_DELETES\","
            + "\"file-path\":\"/path/to/data-a-deletes.parquet\",\"file-format\":\"PARQUET\","
            + "\"partition\":{\"1000\":0},\"file-size-in-bytes\":10,\"record-count\":1}]"
            + "}";

    assertThatThrownBy(() -> FetchScanTasksResponseParser.fromJson(invalidJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid response: deleteFiles should only be returned with fileScanTasks that reference them");
  }

  @Test
  public void roundTripSerdeWithFileScanTasks() {
    ResidualEvaluator residualEvaluator =
        ResidualEvaluator.of(SPEC, Expressions.equal("id", 1), true);
    FileScanTask fileScanTask =
        new BaseFileScanTask(
            FILE_A,
            new DeleteFile[] {FILE_A_DELETES},
            SchemaParser.toJson(SCHEMA),
            PartitionSpecParser.toJson(SPEC),
            residualEvaluator);

    FetchScanTasksResponse response =
        FetchScanTasksResponse.builder()
            .withFileScanTasks(List.of(fileScanTask))
            .withDeleteFiles(List.of(FILE_A_DELETES))
            // assume you have set this already
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .build();

    String expectedToJson =
        "{"
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

    String json = FetchScanTasksResponseParser.toJson(response, false);
    assertThat(json).isEqualTo(expectedToJson);

    // make an unbound json where you expect to not have partitions for the data file,
    // delete files as service does not send parition spec
    String expectedFromJson =
        "{"
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

    FetchScanTasksResponse fromResponse = FetchScanTasksResponseParser.fromJson(json);
    // Need to make a new response with partitionSpec set
    FetchScanTasksResponse copyResponse =
        FetchScanTasksResponse.builder()
            .withPlanTasks(fromResponse.planTasks())
            .withDeleteFiles(fromResponse.deleteFiles())
            .withFileScanTasks(fromResponse.fileScanTasks())
            .withSpecsById(PARTITION_SPECS_BY_ID)
            .build();

    // can't do an equality comparison on PlanTableScanRequest because we don't implement
    // equals/hashcode
    assertThat(FetchScanTasksResponseParser.toJson(copyResponse, false))
        .isEqualTo(expectedFromJson);
  }
}
