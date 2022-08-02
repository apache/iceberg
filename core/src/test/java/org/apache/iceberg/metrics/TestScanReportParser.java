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
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.True;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
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
        .hasMessage("Cannot parse missing string table-name");

    Assertions.assertThatThrownBy(
            () -> ScanReportParser.fromJson("{\"table-name\":\"roundTripTableName\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long snapshot-id");

    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string filter");

    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"filter\":\"true\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing object projection");

    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"filter\":\"true\","
                        + "\"projection\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c1\",\"required\":true,\"type\":\"string\",\"doc\":\"c1\"}]}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing object metrics");
  }

  @Test
  public void extraFields() {
    ScanReport.ScanMetrics scanMetrics = new ScanReport.ScanMetrics(new DefaultMetricsContext());
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.MINUTES);
    scanMetrics.resultDataFiles().increment(5);
    scanMetrics.resultDeleteFiles().increment(5);
    scanMetrics.scannedDataManifests().increment(5);
    scanMetrics.skippedDataManifests().increment(5);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.totalDataManifests().increment(5);
    scanMetrics.totalFileSizeInBytes().increment(45L);
    scanMetrics.totalDeleteFileSizeInBytes().increment(23L);

    String tableName = "roundTripTableName";
    True filter = Expressions.alwaysTrue();
    Schema projection =
        new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));
    ScanReport scanReport =
        ScanReport.builder()
            .withTableName(tableName)
            .withFilter(filter)
            .withProjection(projection)
            .withSnapshotId(23L)
            .fromScanMetrics(scanMetrics)
            .build();

    Assertions.assertThat(
            ScanReportParser.fromJson(
                "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,"
                    + "\"filter\":\"true\",\"projection\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c1\",\"required\":true,\"type\":\"string\",\"doc\":\"c1\"}]},"
                    + "\"metrics\":{\"total-planning-duration\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration-nanos\":600000000000},"
                    + "\"result-data-files\":{\"name\":\"resultDataFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                    + "\"result-delete-files\":{\"name\":\"resultDeleteFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                    + "\"total-data-manifests\":{\"name\":\"totalDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                    + "\"total-delete-manifests\":{\"name\":\"totalDeleteManifests\",\"unit\":\"COUNT\",\"value\":0,\"type\":\"java.lang.Integer\"},"
                    + "\"scanned-data-manifests\":{\"name\":\"scannedDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                    + "\"skipped-data-manifests\":{\"name\":\"skippedDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                    + "\"total-file-size-bytes\":{\"name\":\"totalFileSizeInBytes\",\"unit\":\"BYTES\",\"value\":1069,\"type\":\"java.lang.Long\"},"
                    + "\"total-delete-file-size-bytes\":{\"name\":\"totalDeleteFileSizeInBytes\",\"unit\":\"BYTES\",\"value\":23,\"type\":\"java.lang.Long\"}},"
                    + "\"extra\":\"extraVal\"}"))
        .usingRecursiveComparison()
        .ignoringFields("projection")
        .isEqualTo(scanReport);
  }

  @Test
  public void invalidTableName() {
    Assertions.assertThatThrownBy(() -> ScanReportParser.fromJson("{\"table-name\":23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse table-name to a string value: 23");
  }

  @Test
  public void invalidSnapshotId() {
    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":\"invalid\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse snapshot-id to a long value: \"invalid\"");
  }

  @Test
  public void invalidFilter() {
    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"filter\":999}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse filter to a string value: 999");
  }

  @Test
  public void invalidSchema() {
    Assertions.assertThatThrownBy(
            () ->
                ScanReportParser.fromJson(
                    "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"filter\":\"true\",\"projection\":23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse type from json: 23");
  }

  @Test
  public void roundTripSerde() {
    ScanReport.ScanMetrics scanMetrics = new ScanReport.ScanMetrics(new DefaultMetricsContext());
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.MINUTES);
    scanMetrics.resultDataFiles().increment(5);
    scanMetrics.resultDeleteFiles().increment(5);
    scanMetrics.scannedDataManifests().increment(5);
    scanMetrics.skippedDataManifests().increment(5);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.totalDataManifests().increment(5);
    scanMetrics.totalFileSizeInBytes().increment(45L);
    scanMetrics.totalDeleteFileSizeInBytes().increment(23L);

    String tableName = "roundTripTableName";
    UnboundPredicate<Integer> filter = Expressions.greaterThan("x", 30);
    Schema projection =
        new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));
    ScanReport scanReport =
        ScanReport.builder()
            .withTableName(tableName)
            .withFilter(filter)
            .withProjection(projection)
            .withSnapshotId(23L)
            .fromScanMetrics(scanMetrics)
            .build();

    Assertions.assertThat(ScanReportParser.fromJson(ScanReportParser.toJson(scanReport)))
        .usingRecursiveComparison()
        .ignoringFields("projection")
        .isEqualTo(scanReport);
  }
}
