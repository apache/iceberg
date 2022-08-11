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
import org.apache.iceberg.metrics.ScanReport.ScanMetricsResult;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestScanMetricsResultParser {

  @Test
  public void nullMetrics() {
    Assertions.assertThatThrownBy(() -> ScanMetricsResultParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse scan metrics from null object");

    Assertions.assertThatThrownBy(() -> ScanMetricsResultParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid scan metrics: null");
  }

  @Test
  public void missingFields() {
    Assertions.assertThatThrownBy(() -> ScanMetricsResultParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse timer from missing field: total-planning-duration");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing field: result-data-files");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"unit\":\"count\",\"value\":5}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing field: result-delete-files");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                        + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing field: total-data-manifests");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                        + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                        + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing field: total-delete-manifests");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                        + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                        + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                        + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing field: scanned-data-manifests");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                        + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                        + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                        + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                        + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing field: skipped-data-manifests");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                        + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                        + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                        + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                        + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                        + "\"skipped-data-manifests\":{\"unit\":\"count\",\"value\":5}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing field: total-file-size-in-bytes");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                        + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                        + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                        + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                        + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                        + "\"skipped-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                        + "\"total-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":1069}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing field: total-delete-file-size-in-bytes");
  }

  @Test
  public void extraFields() {
    ScanReport.ScanMetrics scanMetrics = new ScanReport.ScanMetrics(new DefaultMetricsContext());
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.MINUTES);
    scanMetrics.resultDataFiles().increment(5L);
    scanMetrics.resultDeleteFiles().increment(5L);
    scanMetrics.scannedDataManifests().increment(5L);
    scanMetrics.skippedDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.totalDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(45L);
    scanMetrics.totalDeleteFileSizeInBytes().increment(23L);

    ScanMetricsResult scanMetricsResult = ScanMetricsResult.fromScanMetrics(scanMetrics);
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                    + "\"result-data-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"result-delete-files\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-delete-manifests\":{\"unit\":\"count\",\"value\":0},"
                    + "\"scanned-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"skipped-data-manifests\":{\"unit\":\"count\",\"value\":5},"
                    + "\"total-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":1069},"
                    + "\"total-delete-file-size-in-bytes\":{\"unit\":\"bytes\",\"value\":23},"
                    + "\"extra\": \"value\",\"extra2\":23}"))
        .isEqualTo(scanMetricsResult);
  }

  @Test
  public void invalidTimer() {
    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"unit\":\"count\",\"value\":5}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse timer from 'total-planning-duration': Missing field 'count'");
  }

  @Test
  public void invalidCounter() {
    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"count\":1,\"time-unit\":\"nanoseconds\",\"total-duration\":600000000000}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from 'result-data-files': Missing field 'unit'");
  }

  @Test
  public void roundTripSerde() {
    ScanReport.ScanMetrics scanMetrics = new ScanReport.ScanMetrics(new DefaultMetricsContext());
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.DAYS);
    scanMetrics.resultDataFiles().increment(5L);
    scanMetrics.resultDeleteFiles().increment(5L);
    scanMetrics.scannedDataManifests().increment(5L);
    scanMetrics.skippedDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.totalDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(45L);
    scanMetrics.totalDeleteFileSizeInBytes().increment(23L);

    ScanMetricsResult scanMetricsResult = ScanMetricsResult.fromScanMetrics(scanMetrics);

    String expectedJson =
        "{\n"
            + "  \"total-planning-duration\" : {\n"
            + "    \"count\" : 1,\n"
            + "    \"time-unit\" : \"nanoseconds\",\n"
            + "    \"total-duration\" : 864000000000000\n"
            + "  },\n"
            + "  \"result-data-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 5\n"
            + "  },\n"
            + "  \"result-delete-files\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 5\n"
            + "  },\n"
            + "  \"total-data-manifests\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 5\n"
            + "  },\n"
            + "  \"total-delete-manifests\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 0\n"
            + "  },\n"
            + "  \"scanned-data-manifests\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 5\n"
            + "  },\n"
            + "  \"skipped-data-manifests\" : {\n"
            + "    \"unit\" : \"count\",\n"
            + "    \"value\" : 5\n"
            + "  },\n"
            + "  \"total-file-size-in-bytes\" : {\n"
            + "    \"unit\" : \"bytes\",\n"
            + "    \"value\" : 1069\n"
            + "  },\n"
            + "  \"total-delete-file-size-in-bytes\" : {\n"
            + "    \"unit\" : \"bytes\",\n"
            + "    \"value\" : 23\n"
            + "  }\n"
            + "}";

    String json = ScanMetricsResultParser.toJson(scanMetricsResult, true);
    Assertions.assertThat(ScanMetricsResultParser.fromJson(json)).isEqualTo(scanMetricsResult);
    Assertions.assertThat(json).isEqualTo(expectedJson);
  }
}
