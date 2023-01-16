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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestScanReportParser {

  @Test
  public void nullScanReport() {
    Assertions.assertThatThrownBy(() -> ScanReportParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse scan report from null object");

    Assertions.assertThatThrownBy(() -> ScanReportParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid scan report: null");
  }

  @Test
  public void missingFields() {
    Assertions.assertThatThrownBy(() -> ScanReportParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: table-name");

    Assertions.assertThatThrownBy(
            () -> ScanReportParser.fromJson("{\"table-name\":\"roundTripTableName\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: snapshot-id");

    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"filter\":true}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: schema-id");

    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"filter\":true,"
                        + "\"schema-id\" : 4,\"projected-field-ids\" : [ 1, 2, 3 ],\"projected-field-names\" : [ \"c1\", \"c2\", \"c3\" ]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: metrics");
  }

  @Test
  public void extraFields() {
    ScanMetrics scanMetrics = ScanMetrics.of(new DefaultMetricsContext());
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.MINUTES);
    scanMetrics.resultDataFiles().increment(5L);
    scanMetrics.resultDeleteFiles().increment(5L);
    scanMetrics.scannedDataManifests().increment(5L);
    scanMetrics.skippedDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.totalDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(45L);
    scanMetrics.totalDeleteFileSizeInBytes().increment(23L);
    scanMetrics.skippedDataFiles().increment(3L);
    scanMetrics.skippedDeleteFiles().increment(3L);
    scanMetrics.scannedDeleteManifests().increment(3L);
    scanMetrics.skippedDeleteManifests().increment(3L);
    scanMetrics.indexedDeleteFiles().increment(10L);
    scanMetrics.positionalDeleteFiles().increment(6L);
    scanMetrics.equalityDeleteFiles().increment(4L);

    String tableName = "roundTripTableName";
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .schemaId(4)
            .addProjectedFieldIds(1, 2, 3)
            .addProjectedFieldNames("c1", "c2", "c3")
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(scanMetrics))
            .build();

    Assertions.assertThat(
            ScanReportParser.fromJson(
                "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,"
                    + "\"filter\":true,\"schema-id\": 4,\"projected-field-ids\": [ 1, 2, 3 ],\"projected-field-names\": [ \"c1\", \"c2\", \"c3\" ],"
                    + "\"metrics\":{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                    + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"skipped-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":1069},"
                    + "\"total-delete-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":23},"
                    + "\"skipped-data-files\":{\"unit\":\"count\",\"value\":3},"
                    + "\"skipped-delete-files\":{\"unit\":\"count\",\"value\":3},"
                    + "\"scanned-delete-manifests\":{\"unit\":\"count\",\"value\":3},"
                    + "\"skipped-delete-manifests\":{\"unit\":\"count\",\"value\":3},"
                    + "\"indexed-delete-files\":{\"unit\":\"count\",\"value\":10},"
                    + "\"equality-delete-files\":{\"unit\":\"count\",\"value\":4},"
                    + "\"positional-delete-files\":{\"unit\":\"count\",\"value\":6},"
                    + "\"extra-metric\":\"extra-val\"},"
                    + "\"extra\":\"extraVal\"}"))
        .isEqualTo(scanReport);
  }

  @Test
  public void invalidTableName() {
    Assertions.assertThatThrownBy(() -> ScanReportParser.fromJson("{\"table-name\":23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: table-name: 23");
  }

  @Test
  public void invalidSnapshotId() {
    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":\"invalid\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: snapshot-id: \"invalid\"");
  }

  @Test
  public void invalidExpressionFilter() {
    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"filter\":23,\"projection\":23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse expression from non-object: 23");
  }

  @Test
  public void invalidSchema() {
    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"filter\":true,\"schema-id\":\"23\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: schema-id: \"23\"");

    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"filter\":true,\"schema-id\":23,\"projected-field-ids\": [\"1\"],\"metrics\":{}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse integer from non-int value in projected-field-ids: \"1\"");

    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"filter\":true,\"schema-id\":23,\"projected-field-ids\": [1],\"projected-field-names\": [1],\"metrics\":{}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string from non-text value in projected-field-names: 1");
  }

  @Test
  public void roundTripSerde() {
    ScanMetrics scanMetrics = ScanMetrics.of(new DefaultMetricsContext());
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.MINUTES);
    scanMetrics.resultDataFiles().increment(5L);
    scanMetrics.resultDeleteFiles().increment(5L);
    scanMetrics.scannedDataManifests().increment(5L);
    scanMetrics.skippedDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.totalDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(45L);
    scanMetrics.totalDeleteFileSizeInBytes().increment(23L);
    scanMetrics.skippedDataFiles().increment(3L);
    scanMetrics.skippedDeleteFiles().increment(3L);
    scanMetrics.scannedDeleteManifests().increment(3L);
    scanMetrics.skippedDeleteManifests().increment(3L);
    scanMetrics.indexedDeleteFiles().increment(10L);
    scanMetrics.positionalDeleteFiles().increment(6L);
    scanMetrics.equalityDeleteFiles().increment(4L);

    String tableName = "roundTripTableName";
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .schemaId(4)
            .addProjectedFieldIds(1, 2, 3)
            .addProjectedFieldNames("c1", "c2", "c3")
            .filter(Expressions.alwaysTrue())
            .snapshotId(23L)
            .scanMetrics(ScanMetricsResult.fromScanMetrics(scanMetrics))
            .build();

    String expectedJson =
        "{\n"
            + "  \"table-name\" : \"roundTripTableName\",\n"
            + "  \"snapshot-id\" : 23,\n"
            + "  \"filter\" : true,\n"
            + "  \"schema-id\" : 4,\n"
            + "  \"projected-field-ids\" : [ 1, 2, 3 ],\n"
            + "  \"projected-field-names\" : [ \"c1\", \"c2\", \"c3\" ],\n"
            + "  \"metrics\" : {\n"
            + "    \"total-planning-duration\" : {\n"
            + "      \"count\" : 1,\n"
            + "      \"time-unit\" : \"nanoseconds\",\n"
            + "      \"total-duration\" : 600000000000\n"
            + "    },\n"
            + "    \"result-data-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 5\n"
            + "    },\n"
            + "    \"result-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 5\n"
            + "    },\n"
            + "    \"total-data-manifests\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 5\n"
            + "    },\n"
            + "    \"total-delete-manifests\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 0\n"
            + "    },\n"
            + "    \"scanned-data-manifests\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 5\n"
            + "    },\n"
            + "    \"skipped-data-manifests\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 5\n"
            + "    },\n"
            + "    \"total-file-size-in-bytes\" : {\n"
            + "      \"unit\" : \"bytes\",\n"
            + "      \"value\" : 1069\n"
            + "    },\n"
            + "    \"total-delete-file-size-in-bytes\" : {\n"
            + "      \"unit\" : \"bytes\",\n"
            + "      \"value\" : 23\n"
            + "    },\n"
            + "    \"skipped-data-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 3\n"
            + "    },\n"
            + "    \"skipped-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 3\n"
            + "    },\n"
            + "    \"scanned-delete-manifests\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 3\n"
            + "    },\n"
            + "    \"skipped-delete-manifests\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 3\n"
            + "    },\n"
            + "    \"indexed-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 10\n"
            + "    },\n"
            + "    \"equality-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 4\n"
            + "    },\n"
            + "    \"positional-delete-files\" : {\n"
            + "      \"unit\" : \"count\",\n"
            + "      \"value\" : 6\n"
            + "    }\n"
            + "  }\n"
            + "}";

    String json = ScanReportParser.toJson(scanReport, true);
    Assertions.assertThat(ScanReportParser.fromJson(json)).isEqualTo(scanReport);
    Assertions.assertThat(json).isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithNoopMetrics() {
    String tableName = "roundTripTableName";
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .schemaId(4)
            .addProjectedFieldIds(1, 2, 3)
            .addProjectedFieldNames("c1", "c2", "c3")
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build();

    String expectedJson =
        "{\n"
            + "  \"table-name\" : \"roundTripTableName\",\n"
            + "  \"snapshot-id\" : 23,\n"
            + "  \"filter\" : true,\n"
            + "  \"schema-id\" : 4,\n"
            + "  \"projected-field-ids\" : [ 1, 2, 3 ],\n"
            + "  \"projected-field-names\" : [ \"c1\", \"c2\", \"c3\" ],\n"
            + "  \"metrics\" : { }\n"
            + "}";

    String json = ScanReportParser.toJson(scanReport, true);
    Assertions.assertThat(ScanReportParser.fromJson(json)).isEqualTo(scanReport);
    Assertions.assertThat(json).isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithEmptyFieldIdsAndNames() {
    String tableName = "roundTripTableName";
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .schemaId(4)
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build();

    String expectedJson =
        "{\n"
            + "  \"table-name\" : \"roundTripTableName\",\n"
            + "  \"snapshot-id\" : 23,\n"
            + "  \"filter\" : true,\n"
            + "  \"schema-id\" : 4,\n"
            + "  \"projected-field-ids\" : [ ],\n"
            + "  \"projected-field-names\" : [ ],\n"
            + "  \"metrics\" : { }\n"
            + "}";

    String json = ScanReportParser.toJson(scanReport, true);
    Assertions.assertThat(ScanReportParser.fromJson(json)).isEqualTo(scanReport);
    Assertions.assertThat(json).isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithMetadata() {
    String tableName = "roundTripTableName";
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .schemaId(4)
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .metadata(ImmutableMap.of("k1", "v1", "k2", "v2"))
            .build();

    String expectedJson =
        "{\n"
            + "  \"table-name\" : \"roundTripTableName\",\n"
            + "  \"snapshot-id\" : 23,\n"
            + "  \"filter\" : true,\n"
            + "  \"schema-id\" : 4,\n"
            + "  \"projected-field-ids\" : [ ],\n"
            + "  \"projected-field-names\" : [ ],\n"
            + "  \"metrics\" : { },\n"
            + "  \"metadata\" : {\n"
            + "    \"k1\" : \"v1\",\n"
            + "    \"k2\" : \"v2\"\n"
            + "  }\n"
            + "}";

    String json = ScanReportParser.toJson(scanReport, true);
    Assertions.assertThat(ScanReportParser.fromJson(json)).isEqualTo(scanReport);
    Assertions.assertThat(json).isEqualTo(expectedJson);
  }
}
