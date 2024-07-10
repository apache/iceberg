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
package org.apache.iceberg.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestCommitReportParser {

  @Test
  public void nullCommitReport() {
    assertThatThrownBy(() -> CommitReportParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse commit report from null object");

    assertThatThrownBy(() -> CommitReportParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid commit report: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> CommitReportParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: table-name");

    assertThatThrownBy(() -> CommitReportParser.fromJson("{\"table-name\":\"roundTripTableName\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: snapshot-id");

    assertThatThrownBy(
            () ->
                CommitReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: sequence-number");

    assertThatThrownBy(
            () ->
                CommitReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"sequence-number\":24}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: operation");

    assertThatThrownBy(
            () ->
                CommitReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"sequence-number\":24, \"operation\": \"DELETE\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: metrics");
  }

  @Test
  public void invalidTableName() {
    assertThatThrownBy(() -> CommitReportParser.fromJson("{\"table-name\":23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: table-name: 23");
  }

  @Test
  public void invalidSnapshotId() {
    assertThatThrownBy(
            () ->
                CommitReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":\"invalid\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: snapshot-id: \"invalid\"");
  }

  @SuppressWarnings("MethodLength")
  @Test
  public void roundTripSerde() {
    CommitMetrics commitMetrics = CommitMetrics.of(new DefaultMetricsContext());
    commitMetrics.totalDuration().record(100, TimeUnit.SECONDS);
    commitMetrics.attempts().increment(4);
    Map<String, String> snapshotSummary =
        com.google.common.collect.ImmutableMap.<String, String>builder()
            .put(SnapshotSummary.ADDED_FILES_PROP, "1")
            .put(SnapshotSummary.DELETED_FILES_PROP, "2")
            .put(SnapshotSummary.TOTAL_DATA_FILES_PROP, "3")
            .put(SnapshotSummary.ADDED_DELETE_FILES_PROP, "4")
            .put(SnapshotSummary.ADD_EQ_DELETE_FILES_PROP, "5")
            .put(SnapshotSummary.ADD_POS_DELETE_FILES_PROP, "6")
            .put(SnapshotSummary.REMOVED_POS_DELETE_FILES_PROP, "7")
            .put(SnapshotSummary.REMOVED_EQ_DELETE_FILES_PROP, "8")
            .put(SnapshotSummary.REMOVED_DELETE_FILES_PROP, "9")
            .put(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "10")
            .put(SnapshotSummary.ADDED_RECORDS_PROP, "11")
            .put(SnapshotSummary.DELETED_RECORDS_PROP, "12")
            .put(SnapshotSummary.TOTAL_RECORDS_PROP, "13")
            .put(SnapshotSummary.ADDED_FILE_SIZE_PROP, "14")
            .put(SnapshotSummary.REMOVED_FILE_SIZE_PROP, "15")
            .put(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "16")
            .put(SnapshotSummary.ADDED_POS_DELETES_PROP, "17")
            .put(SnapshotSummary.ADDED_EQ_DELETES_PROP, "18")
            .put(SnapshotSummary.REMOVED_POS_DELETES_PROP, "19")
            .put(SnapshotSummary.REMOVED_EQ_DELETES_PROP, "20")
            .put(SnapshotSummary.TOTAL_POS_DELETES_PROP, "21")
            .put(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "22")
            .build();

    String tableName = "roundTripTableName";
    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName(tableName)
            .snapshotId(23L)
            .operation("DELETE")
            .sequenceNumber(4L)
            .commitMetrics(CommitMetricsResult.from(commitMetrics, snapshotSummary))
            .build();

    String expectedJson =
        "{\n"
            + "  \"table-name\" : \"roundTripTableName\",\n"
            + "  \"snapshot-id\" : 23,\n"
            + "  \"sequence-number\" : 4,\n"
            + "  \"operation\" : \"DELETE\",\n"
            + "  \"metrics\" : {\n"
            + "    \"total-duration\" : {\n"
            + "      \"count\" : 1,\n"
            + "      \"time-unit\" : \"nanoseconds\",\n"
            + "      \"total-duration\" : 100000000000\n"
            + "    },\n"
            + "    \"attempts\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 4\n"
            + "    },\n"
            + "    \"added-data-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 1\n"
            + "    },\n"
            + "    \"removed-data-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 2\n"
            + "    },\n"
            + "    \"total-data-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 3\n"
            + "    },\n"
            + "    \"added-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 4\n"
            + "    },\n"
            + "    \"added-equality-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 5\n"
            + "    },\n"
            + "    \"added-positional-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 6\n"
            + "    },\n"
            + "    \"removed-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 9\n"
            + "    },\n"
            + "    \"removed-positional-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 7\n"
            + "    },\n"
            + "    \"removed-equality-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 8\n"
            + "    },\n"
            + "    \"total-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 10\n"
            + "    },\n"
            + "    \"added-records\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 11\n"
            + "    },\n"
            + "    \"removed-records\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 12\n"
            + "    },\n"
            + "    \"total-records\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 13\n"
            + "    },\n"
            + "    \"added-files-size-bytes\" : {\n"
            + "      \"unit\" : \"bytes\",\n"
            + "      \"value\" : 14\n"
            + "    },\n"
            + "    \"removed-files-size-bytes\" : {\n"
            + "      \"unit\" : \"bytes\",\n"
            + "      \"value\" : 15\n"
            + "    },\n"
            + "    \"total-files-size-bytes\" : {\n"
            + "      \"unit\" : \"bytes\",\n"
            + "      \"value\" : 16\n"
            + "    },\n"
            + "    \"added-positional-deletes\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 17\n"
            + "    },\n"
            + "    \"removed-positional-deletes\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 19\n"
            + "    },\n"
            + "    \"total-positional-deletes\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 21\n"
            + "    },\n"
            + "    \"added-equality-deletes\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 18\n"
            + "    },\n"
            + "    \"removed-equality-deletes\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 20\n"
            + "    },\n"
            + "    \"total-equality-deletes\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 22\n"
            + "    }\n"
            + "  }\n"
            + "}";

    String json = CommitReportParser.toJson(commitReport, true);
    assertThat(CommitReportParser.fromJson(json)).isEqualTo(commitReport);
    assertThat(json).isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithNoopMetrics() {
    String tableName = "roundTripTableName";
    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName(tableName)
            .snapshotId(23L)
            .operation("DELETE")
            .sequenceNumber(4L)
            .commitMetrics(CommitMetricsResult.from(CommitMetrics.noop(), ImmutableMap.of()))
            .build();

    String expectedJson =
        "{\n"
            + "  \"table-name\" : \"roundTripTableName\",\n"
            + "  \"snapshot-id\" : 23,\n"
            + "  \"sequence-number\" : 4,\n"
            + "  \"operation\" : \"DELETE\",\n"
            + "  \"metrics\" : { }\n"
            + "}";

    String json = CommitReportParser.toJson(commitReport, true);
    assertThat(CommitReportParser.fromJson(json)).isEqualTo(commitReport);
    assertThat(json).isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithMetadata() {
    String tableName = "roundTripTableName";
    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName(tableName)
            .snapshotId(23L)
            .operation("DELETE")
            .sequenceNumber(4L)
            .commitMetrics(CommitMetricsResult.from(CommitMetrics.noop(), ImmutableMap.of()))
            .metadata(ImmutableMap.of("k1", "v1", "k2", "v2"))
            .build();

    String expectedJson =
        "{\n"
            + "  \"table-name\" : \"roundTripTableName\",\n"
            + "  \"snapshot-id\" : 23,\n"
            + "  \"sequence-number\" : 4,\n"
            + "  \"operation\" : \"DELETE\",\n"
            + "  \"metrics\" : { },\n"
            + "  \"metadata\" : {\n"
            + "    \"k1\" : \"v1\",\n"
            + "    \"k2\" : \"v2\"\n"
            + "  }\n"
            + "}";

    String json = CommitReportParser.toJson(commitReport, true);
    assertThat(CommitReportParser.fromJson(json)).isEqualTo(commitReport);
    assertThat(json).isEqualTo(expectedJson);
  }
}
