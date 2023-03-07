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
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class TestCommitMetricsResultParser {

  @Test
  public void nullMetrics() {
    assertThatThrownBy(() -> CommitMetricsResultParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse commit metrics from null object");

    assertThatThrownBy(() -> CommitMetricsResultParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid commit metrics: null");

    assertThatThrownBy(() -> CommitMetricsResult.from(null, ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid commit metrics: null");

    assertThatThrownBy(
            () -> CommitMetricsResult.from(CommitMetrics.of(new DefaultMetricsContext()), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid snapshot summary: null");
  }

  @Test
  public void invalidNumberInSnapshotSummary() {
    CommitMetricsResult result =
        CommitMetricsResult.from(
            CommitMetrics.of(new DefaultMetricsContext()),
            ImmutableMap.of(SnapshotSummary.ADDED_FILES_PROP, "xyz"));
    assertThat(result.addedDataFiles()).isNull();
  }

  @SuppressWarnings("MethodLength")
  @Test
  public void roundTripSerde() {
    CommitMetrics commitMetrics = CommitMetrics.of(new DefaultMetricsContext());
    commitMetrics.totalDuration().record(100, TimeUnit.SECONDS);
    commitMetrics.attempts().increment(4);
    Map<String, String> snapshotSummary =
        ImmutableMap.<String, String>builder()
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

    CommitMetricsResult result = CommitMetricsResult.from(commitMetrics, snapshotSummary);
    assertThat(result.attempts().value()).isEqualTo(4L);
    assertThat(result.totalDuration().totalDuration()).isEqualTo(Duration.ofSeconds(100));
    assertThat(result.addedDataFiles().value()).isEqualTo(1L);
    assertThat(result.removedDataFiles().value()).isEqualTo(2L);
    assertThat(result.totalDataFiles().value()).isEqualTo(3L);
    assertThat(result.addedDeleteFiles().value()).isEqualTo(4L);
    assertThat(result.addedEqualityDeleteFiles().value()).isEqualTo(5L);
    assertThat(result.addedPositionalDeleteFiles().value()).isEqualTo(6L);
    assertThat(result.removedPositionalDeleteFiles().value()).isEqualTo(7L);
    assertThat(result.removedEqualityDeleteFiles().value()).isEqualTo(8L);
    assertThat(result.removedDeleteFiles().value()).isEqualTo(9L);
    assertThat(result.totalDeleteFiles().value()).isEqualTo(10L);
    assertThat(result.addedRecords().value()).isEqualTo(11L);
    assertThat(result.removedRecords().value()).isEqualTo(12L);
    assertThat(result.totalRecords().value()).isEqualTo(13L);
    assertThat(result.addedFilesSizeInBytes().value()).isEqualTo(14L);
    assertThat(result.removedFilesSizeInBytes().value()).isEqualTo(15L);
    assertThat(result.totalFilesSizeInBytes().value()).isEqualTo(16L);
    assertThat(result.addedPositionalDeletes().value()).isEqualTo(17L);
    assertThat(result.addedEqualityDeletes().value()).isEqualTo(18L);
    assertThat(result.removedPositionalDeletes().value()).isEqualTo(19L);
    assertThat(result.removedEqualityDeletes().value()).isEqualTo(20L);
    assertThat(result.totalPositionalDeletes().value()).isEqualTo(21L);
    assertThat(result.totalEqualityDeletes().value()).isEqualTo(22L);

    String expectedJson =
        "{\n"
            + "  \"total-duration\" : {\n"
            + "    \"count\" : 1,\n"
            + "    \"time-unit\" : \"nanoseconds\",\n"
            + "    \"total-duration\" : 100000000000\n"
            + "  },\n"
            + "  \"attempts\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 4\n"
            + "  },\n"
            + "  \"added-data-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 1\n"
            + "  },\n"
            + "  \"removed-data-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 2\n"
            + "  },\n"
            + "  \"total-data-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 3\n"
            + "  },\n"
            + "  \"added-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 4\n"
            + "  },\n"
            + "  \"added-equality-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 5\n"
            + "  },\n"
            + "  \"added-positional-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 6\n"
            + "  },\n"
            + "  \"removed-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 9\n"
            + "  },\n"
            + "  \"removed-positional-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 7\n"
            + "  },\n"
            + "  \"removed-equality-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 8\n"
            + "  },\n"
            + "  \"total-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 10\n"
            + "  },\n"
            + "  \"added-records\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 11\n"
            + "  },\n"
            + "  \"removed-records\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 12\n"
            + "  },\n"
            + "  \"total-records\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 13\n"
            + "  },\n"
            + "  \"added-files-size-bytes\" : {\n"
            + "    \"unit\" : \"bytes\",\n"
            + "    \"value\" : 14\n"
            + "  },\n"
            + "  \"removed-files-size-bytes\" : {\n"
            + "    \"unit\" : \"bytes\",\n"
            + "    \"value\" : 15\n"
            + "  },\n"
            + "  \"total-files-size-bytes\" : {\n"
            + "    \"unit\" : \"bytes\",\n"
            + "    \"value\" : 16\n"
            + "  },\n"
            + "  \"added-positional-deletes\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 17\n"
            + "  },\n"
            + "  \"removed-positional-deletes\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 19\n"
            + "  },\n"
            + "  \"total-positional-deletes\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 21\n"
            + "  },\n"
            + "  \"added-equality-deletes\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 18\n"
            + "  },\n"
            + "  \"removed-equality-deletes\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 20\n"
            + "  },\n"
            + "  \"total-equality-deletes\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 22\n"
            + "  }\n"
            + "}";

    String json = CommitMetricsResultParser.toJson(result, true);
    assertThat(CommitMetricsResultParser.fromJson(json)).isEqualTo(result);
    assertThat(json).isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeNoopCommitMetrics() {
    CommitMetricsResult commitMetricsResult =
        CommitMetricsResult.from(CommitMetrics.noop(), SnapshotSummary.builder().build());
    String expectedJson = "{ }";

    String json = CommitMetricsResultParser.toJson(commitMetricsResult, true);
    System.out.println(json);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(CommitMetricsResultParser.fromJson(json))
        .isEqualTo(ImmutableCommitMetricsResult.builder().build());
  }
}
