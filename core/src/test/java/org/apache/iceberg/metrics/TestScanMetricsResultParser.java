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
        .hasMessage("Cannot parse timer from missing object 'total-planning-duration'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration\":600000000000}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing object 'result-data-files'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"name\":\"resultDataFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing object 'result-delete-files'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"name\":\"resultDataFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"result-delete-files\":{\"name\":\"resultDeleteFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing object 'total-data-manifests'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"name\":\"resultDataFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"result-delete-files\":{\"name\":\"resultDeleteFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"total-data-manifests\":{\"name\":\"totalDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing object 'total-delete-manifests'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"name\":\"resultDataFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"result-delete-files\":{\"name\":\"resultDeleteFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"total-data-manifests\":{\"name\":\"totalDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"total-delete-manifests\":{\"name\":\"totalDeleteManifests\",\"unit\":\"COUNT\",\"value\":0,\"type\":\"java.lang.Integer\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing object 'scanned-data-manifests'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"name\":\"resultDataFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"result-delete-files\":{\"name\":\"resultDeleteFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"total-data-manifests\":{\"name\":\"totalDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"total-delete-manifests\":{\"name\":\"totalDeleteManifests\",\"unit\":\"COUNT\",\"value\":0,\"type\":\"java.lang.Integer\"},"
                        + "\"scanned-data-manifests\":{\"name\":\"scannedDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing object 'skipped-data-manifests'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"name\":\"resultDataFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"result-delete-files\":{\"name\":\"resultDeleteFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"total-data-manifests\":{\"name\":\"totalDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"total-delete-manifests\":{\"name\":\"totalDeleteManifests\",\"unit\":\"COUNT\",\"value\":0,\"type\":\"java.lang.Integer\"},"
                        + "\"scanned-data-manifests\":{\"name\":\"scannedDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"skipped-data-manifests\":{\"name\":\"skippedDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing object 'total-file-size-bytes'");

    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"name\":\"resultDataFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"result-delete-files\":{\"name\":\"resultDeleteFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"total-data-manifests\":{\"name\":\"totalDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"total-delete-manifests\":{\"name\":\"totalDeleteManifests\",\"unit\":\"COUNT\",\"value\":0,\"type\":\"java.lang.Integer\"},"
                        + "\"scanned-data-manifests\":{\"name\":\"scannedDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"skipped-data-manifests\":{\"name\":\"skippedDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                        + "\"total-file-size-bytes\":{\"name\":\"totalFileSizeInBytes\",\"unit\":\"BYTES\",\"value\":1069,\"type\":\"java.lang.Long\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from missing object 'total-delete-file-size-bytes'");
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

    ScanMetricsResult scanMetricsResult = ScanMetricsResult.fromScanMetrics(scanMetrics);
    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(
                "{\"total-planning-duration\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration\":600000000000},"
                    + "\"result-data-files\":{\"name\":\"resultDataFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                    + "\"result-delete-files\":{\"name\":\"resultDeleteFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                    + "\"total-data-manifests\":{\"name\":\"totalDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                    + "\"total-delete-manifests\":{\"name\":\"totalDeleteManifests\",\"unit\":\"COUNT\",\"value\":0,\"type\":\"java.lang.Integer\"},"
                    + "\"scanned-data-manifests\":{\"name\":\"scannedDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                    + "\"skipped-data-manifests\":{\"name\":\"skippedDataManifests\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"},"
                    + "\"total-file-size-bytes\":{\"name\":\"totalFileSizeInBytes\",\"unit\":\"BYTES\",\"value\":1069,\"type\":\"java.lang.Long\"},"
                    + "\"total-delete-file-size-bytes\":{\"name\":\"totalDeleteFileSizeInBytes\",\"unit\":\"BYTES\",\"value\":23,\"type\":\"java.lang.Long\"},"
                    + "\"extra\": \"value\",\"extra2\":23}"))
        .isEqualTo(scanMetricsResult);
  }

  @Test
  public void invalidTimer() {
    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"name\":\"resultDataFiles\",\"unit\":\"COUNT\",\"value\":5,\"type\":\"java.lang.Integer\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse timer from 'total-planning-duration': Missing field 'count'");
  }

  @Test
  public void invalidCounter() {
    Assertions.assertThatThrownBy(
            () ->
                ScanMetricsResultParser.fromJson(
                    "{\"total-planning-duration\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration\":600000000000},"
                        + "\"result-data-files\":{\"name\":\"totalPlanningDuration\",\"count\":1,\"time-unit\":\"NANOSECONDS\",\"total-duration\":600000000000}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from 'result-data-files': Missing field 'type'");
  }

  @Test
  public void roundTripSerde() {
    ScanReport.ScanMetrics scanMetrics = new ScanReport.ScanMetrics(new DefaultMetricsContext());
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.DAYS);
    scanMetrics.resultDataFiles().increment(5);
    scanMetrics.resultDeleteFiles().increment(5);
    scanMetrics.scannedDataManifests().increment(5);
    scanMetrics.skippedDataManifests().increment(5);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.totalDataManifests().increment(5);
    scanMetrics.totalFileSizeInBytes().increment(45L);
    scanMetrics.totalDeleteFileSizeInBytes().increment(23L);

    ScanMetricsResult scanMetricsResult = ScanMetricsResult.fromScanMetrics(scanMetrics);

    Assertions.assertThat(
            ScanMetricsResultParser.fromJson(ScanMetricsResultParser.toJson(scanMetricsResult)))
        .isEqualTo(scanMetricsResult);
  }
}
