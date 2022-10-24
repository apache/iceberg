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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestScanReport {

  @Test
  public void missingFields() {
    Assertions.assertThatThrownBy(() -> ImmutableScanReport.builder().build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ScanReport, some of required attributes are not set [tableName, snapshotId, filter, projection, scanMetrics]");

    Assertions.assertThatThrownBy(() -> ImmutableScanReport.builder().tableName("x").build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ScanReport, some of required attributes are not set [snapshotId, filter, projection, scanMetrics]");

    Assertions.assertThatThrownBy(
            () ->
                ImmutableScanReport.builder()
                    .tableName("x")
                    .filter(Expressions.alwaysTrue())
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ScanReport, some of required attributes are not set [snapshotId, projection, scanMetrics]");

    Assertions.assertThatThrownBy(
            () ->
                ImmutableScanReport.builder()
                    .tableName("x")
                    .filter(Expressions.alwaysTrue())
                    .snapshotId(23L)
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ScanReport, some of required attributes are not set [projection, scanMetrics]");

    Assertions.assertThatThrownBy(
            () ->
                ImmutableScanReport.builder()
                    .tableName("x")
                    .filter(Expressions.alwaysTrue())
                    .snapshotId(23L)
                    .projection(
                        new Schema(
                            Types.NestedField.required(1, "c1", Types.StringType.get(), "c1")))
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ScanReport, some of required attributes are not set [scanMetrics]");
  }

  @Test
  public void fromEmptyScanMetrics() {
    String tableName = "x";
    Schema projection =
        new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .projection(projection)
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build();

    Assertions.assertThat(scanReport.tableName()).isEqualTo(tableName);
    Assertions.assertThat(scanReport.projection()).isEqualTo(projection);
    Assertions.assertThat(scanReport.filter()).isEqualTo(Expressions.alwaysTrue());
    Assertions.assertThat(scanReport.snapshotId()).isEqualTo(23L);
    Assertions.assertThat(scanReport.scanMetrics().totalPlanningDuration()).isNull();
    Assertions.assertThat(scanReport.scanMetrics().resultDataFiles()).isNull();
    Assertions.assertThat(scanReport.scanMetrics().resultDeleteFiles()).isNull();
    Assertions.assertThat(scanReport.scanMetrics().totalDataManifests()).isNull();
    Assertions.assertThat(scanReport.scanMetrics().totalDeleteManifests()).isNull();
    Assertions.assertThat(scanReport.scanMetrics().scannedDataManifests()).isNull();
    Assertions.assertThat(scanReport.scanMetrics().skippedDataManifests()).isNull();
    Assertions.assertThat(scanReport.scanMetrics().totalFileSizeInBytes()).isNull();
    Assertions.assertThat(scanReport.scanMetrics().totalDeleteFileSizeInBytes()).isNull();
  }

  @Test
  public void fromScanMetrics() {
    ScanMetrics scanMetrics = ScanMetrics.of(new DefaultMetricsContext());
    scanMetrics.totalPlanningDuration().record(10, TimeUnit.MINUTES);
    scanMetrics.resultDataFiles().increment(5L);
    scanMetrics.resultDeleteFiles().increment(5L);
    scanMetrics.scannedDataManifests().increment(5L);
    scanMetrics.totalFileSizeInBytes().increment(1024L);
    scanMetrics.totalDataManifests().increment(5L);

    String tableName = "x";
    Schema projection =
        new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));

    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .projection(projection)
            .scanMetrics(ScanMetricsResult.fromScanMetrics(scanMetrics))
            .build();

    Assertions.assertThat(scanReport.tableName()).isEqualTo(tableName);
    Assertions.assertThat(scanReport.projection()).isEqualTo(projection);
    Assertions.assertThat(scanReport.filter()).isEqualTo(Expressions.alwaysTrue());
    Assertions.assertThat(scanReport.snapshotId()).isEqualTo(23L);
    Assertions.assertThat(scanReport.scanMetrics().totalPlanningDuration().totalDuration())
        .isEqualTo(Duration.ofMinutes(10L));
    Assertions.assertThat(scanReport.scanMetrics().resultDataFiles().value()).isEqualTo(5);
    Assertions.assertThat(scanReport.scanMetrics().resultDeleteFiles().value()).isEqualTo(5);
    Assertions.assertThat(scanReport.scanMetrics().scannedDataManifests().value()).isEqualTo(5);
    Assertions.assertThat(scanReport.scanMetrics().totalDataManifests().value()).isEqualTo(5);
    Assertions.assertThat(scanReport.scanMetrics().totalFileSizeInBytes().value()).isEqualTo(1024L);
  }

  @Test
  public void nullScanMetrics() {
    Assertions.assertThatThrownBy(() -> ScanMetrics.of(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("metricsContext");
  }
}
