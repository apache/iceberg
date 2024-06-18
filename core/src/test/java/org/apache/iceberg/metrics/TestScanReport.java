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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.Test;

public class TestScanReport {

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> ImmutableScanReport.builder().build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ScanReport, some of required attributes are not set [tableName, snapshotId, filter, schemaId, scanMetrics]");

    assertThatThrownBy(() -> ImmutableScanReport.builder().tableName("x").build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ScanReport, some of required attributes are not set [snapshotId, filter, schemaId, scanMetrics]");

    assertThatThrownBy(
            () ->
                ImmutableScanReport.builder()
                    .tableName("x")
                    .filter(Expressions.alwaysTrue())
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ScanReport, some of required attributes are not set [snapshotId, schemaId, scanMetrics]");

    assertThatThrownBy(
            () ->
                ImmutableScanReport.builder()
                    .tableName("x")
                    .filter(Expressions.alwaysTrue())
                    .snapshotId(23L)
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ScanReport, some of required attributes are not set [schemaId, scanMetrics]");

    assertThatThrownBy(
            () ->
                ImmutableScanReport.builder()
                    .tableName("x")
                    .filter(Expressions.alwaysTrue())
                    .snapshotId(23L)
                    .schemaId(4)
                    .addProjectedFieldIds(1, 2)
                    .addProjectedFieldNames("c1", "c2")
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot build ScanReport, some of required attributes are not set [scanMetrics]");
  }

  @Test
  public void fromEmptyScanMetrics() {
    String tableName = "x";
    int schemaId = 4;
    List<Integer> fieldIds = Arrays.asList(1, 2);
    List<String> fieldNames = Arrays.asList("c1", "c2");
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .schemaId(schemaId)
            .projectedFieldIds(fieldIds)
            .projectedFieldNames(fieldNames)
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build();

    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.schemaId()).isEqualTo(schemaId);
    assertThat(scanReport.projectedFieldIds()).isEqualTo(fieldIds);
    assertThat(scanReport.projectedFieldNames()).isEqualTo(fieldNames);
    assertThat(scanReport.filter()).isEqualTo(Expressions.alwaysTrue());
    assertThat(scanReport.snapshotId()).isEqualTo(23L);
    assertThat(scanReport.scanMetrics().totalPlanningDuration()).isNull();
    assertThat(scanReport.scanMetrics().resultDataFiles()).isNull();
    assertThat(scanReport.scanMetrics().resultDeleteFiles()).isNull();
    assertThat(scanReport.scanMetrics().totalDataManifests()).isNull();
    assertThat(scanReport.scanMetrics().totalDeleteManifests()).isNull();
    assertThat(scanReport.scanMetrics().scannedDataManifests()).isNull();
    assertThat(scanReport.scanMetrics().skippedDataManifests()).isNull();
    assertThat(scanReport.scanMetrics().totalFileSizeInBytes()).isNull();
    assertThat(scanReport.scanMetrics().totalDeleteFileSizeInBytes()).isNull();
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
    int schemaId = 4;
    List<Integer> fieldIds = Arrays.asList(1, 2);
    List<String> fieldNames = Arrays.asList("c1", "c2");

    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .schemaId(schemaId)
            .projectedFieldIds(fieldIds)
            .projectedFieldNames(fieldNames)
            .scanMetrics(ScanMetricsResult.fromScanMetrics(scanMetrics))
            .build();

    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.schemaId()).isEqualTo(schemaId);
    assertThat(scanReport.projectedFieldIds()).isEqualTo(fieldIds);
    assertThat(scanReport.projectedFieldNames()).isEqualTo(fieldNames);
    assertThat(scanReport.filter()).isEqualTo(Expressions.alwaysTrue());
    assertThat(scanReport.snapshotId()).isEqualTo(23L);
    assertThat(scanReport.scanMetrics().totalPlanningDuration().totalDuration())
        .isEqualTo(Duration.ofMinutes(10L));
    assertThat(scanReport.scanMetrics().resultDataFiles().value()).isEqualTo(5);
    assertThat(scanReport.scanMetrics().resultDeleteFiles().value()).isEqualTo(5);
    assertThat(scanReport.scanMetrics().scannedDataManifests().value()).isEqualTo(5);
    assertThat(scanReport.scanMetrics().totalDataManifests().value()).isEqualTo(5);
    assertThat(scanReport.scanMetrics().totalFileSizeInBytes().value()).isEqualTo(1024L);
  }

  @Test
  public void nullScanMetrics() {
    assertThatThrownBy(() -> ScanMetrics.of(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("metricsContext");
  }
}
