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
    Assertions.assertThatThrownBy(() -> ScanReport.builder().build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table name: null");

    Assertions.assertThatThrownBy(() -> ScanReport.builder().withTableName("x").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid expression filter: null");

    Assertions.assertThatThrownBy(
            () ->
                ScanReport.builder()
                    .withTableName("x")
                    .withFilter(Expressions.alwaysTrue())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid schema projection: null");

    Assertions.assertThatThrownBy(
            () ->
                ScanReport.builder()
                    .withTableName("x")
                    .withFilter(Expressions.alwaysTrue())
                    .withProjection(
                        new Schema(
                            Types.NestedField.required(1, "c1", Types.StringType.get(), "c1")))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid scan metrics: null");
  }

  @Test
  public void fromEmptyScanMetrics() {
    String tableName = "x";
    Schema projection =
        new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));
    ScanReport scanReport =
        ScanReport.builder()
            .withTableName(tableName)
            .withProjection(projection)
            .withFilter(Expressions.alwaysTrue())
            .fromScanMetrics(ScanReport.ScanMetrics.NOOP)
            .build();

    Assertions.assertThat(scanReport.tableName()).isEqualTo(tableName);
    Assertions.assertThat(scanReport.projection()).isEqualTo(projection);
    Assertions.assertThat(scanReport.filter()).isEqualTo(Expressions.alwaysTrue());
    Assertions.assertThat(scanReport.snapshotId()).isEqualTo(-1);
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
    ScanReport.ScanMetrics scanMetrics = new ScanReport.ScanMetrics(new DefaultMetricsContext());
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
        ScanReport.builder()
            .withTableName(tableName)
            .withProjection(projection)
            .withSnapshotId(23L)
            .withFilter(Expressions.alwaysTrue())
            .fromScanMetrics(scanMetrics)
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
    Assertions.assertThatThrownBy(() -> new ScanReport.ScanMetrics(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid metrics context: null");
  }
}
