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

import java.util.List;
import java.util.regex.PatternSyntaxException;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestFilteringMetricsReporter {

  private static final ScanReport SCAN_PROD = newScanReport("prod_db.orders");
  private static final ScanReport SCAN_TMP = newScanReport("prod_db.tmp_staging");
  private static final ScanReport SCAN_DEV = newScanReport("dev_db.orders");
  private static final CommitReport COMMIT_PROD = newCommitReport("prod_db.orders");

  @Test
  public void wrapReturnsDelegateWhenNoPropertiesSet() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped = FilteringMetricsReporter.wrap(delegate, ImmutableMap.of());
    assertThat(wrapped).isSameAs(delegate);
  }

  @Test
  public void wrapReturnsDelegateWhenPropertiesAreEmpty() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE, "",
                CatalogProperties.METRICS_REPORTER_TABLE_NAME_EXCLUDE, ""));
    assertThat(wrapped).isSameAs(delegate);
  }

  @Test
  public void includeOnlyForwardsMatchingTableNames() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            ImmutableMap.of(CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE, "prod_db\\..*"));

    wrapped.report(SCAN_PROD);
    wrapped.report(SCAN_DEV);
    wrapped.report(COMMIT_PROD);

    assertThat(delegate.reports).containsExactly(SCAN_PROD, COMMIT_PROD);
  }

  @Test
  public void excludeOnlyDropsMatchingTableNames() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            ImmutableMap.of(CatalogProperties.METRICS_REPORTER_TABLE_NAME_EXCLUDE, ".*\\.tmp_.*"));

    wrapped.report(SCAN_PROD);
    wrapped.report(SCAN_TMP);

    assertThat(delegate.reports).containsExactly(SCAN_PROD);
  }

  @Test
  public void excludeWinsOverInclude() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE, "prod_db\\..*",
                CatalogProperties.METRICS_REPORTER_TABLE_NAME_EXCLUDE, ".*\\.tmp_.*"));

    wrapped.report(SCAN_PROD);
    wrapped.report(SCAN_TMP);
    wrapped.report(SCAN_DEV);

    assertThat(delegate.reports).containsExactly(SCAN_PROD);
  }

  @Test
  public void unknownReportSubtypeIsForwardedWithoutFiltering() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            ImmutableMap.of(CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE, "no_such\\..*"));

    MetricsReport unknown = new MetricsReport() {};
    wrapped.report(unknown);

    assertThat(delegate.reports).containsExactly(unknown);
  }

  @Test
  public void wrapThrowsClearErrorForInvalidRegex() {
    assertThatThrownBy(
            () ->
                FilteringMetricsReporter.wrap(
                    new CapturingMetricsReporter(),
                    ImmutableMap.of(
                        CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE, "[invalid")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE)
        .hasMessageContaining("[invalid")
        .hasCauseInstanceOf(PatternSyntaxException.class);
  }

  @Test
  public void loadMetricsReporterFiltersThroughUserConfiguredReporter() {
    StaticCapturingReporter.REPORTS.clear();
    MetricsReporter reporter =
        CatalogUtil.loadMetricsReporter(
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_IMPL,
                StaticCapturingReporter.class.getName(),
                CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE,
                "prod_db\\..*"));

    reporter.report(SCAN_PROD);
    reporter.report(SCAN_DEV);
    reporter.report(COMMIT_PROD);

    assertThat(StaticCapturingReporter.REPORTS)
        .as(
            "Reports configured via metrics-reporter-impl receive only table names that pass the"
                + " include filter")
        .containsExactly(SCAN_PROD, COMMIT_PROD);
  }

  @Test
  public void closeIsDelegated() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate, ImmutableMap.of(CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE, ".*"));

    wrapped.close();

    assertThat(delegate.closed).isTrue();
  }

  private static ScanReport newScanReport(String tableName) {
    return ImmutableScanReport.builder()
        .tableName(tableName)
        .snapshotId(1L)
        .filter(Expressions.alwaysTrue())
        .schemaId(1)
        .projectedFieldIds(ImmutableList.of())
        .projectedFieldNames(ImmutableList.of())
        .scanMetrics(ImmutableScanMetricsResult.builder().build())
        .metadata(ImmutableMap.of())
        .build();
  }

  private static CommitReport newCommitReport(String tableName) {
    return ImmutableCommitReport.builder()
        .tableName(tableName)
        .snapshotId(1L)
        .sequenceNumber(1L)
        .operation("append")
        .commitMetrics(ImmutableCommitMetricsResult.builder().build())
        .metadata(ImmutableMap.of())
        .build();
  }

  private static class CapturingMetricsReporter implements MetricsReporter {
    private final List<MetricsReport> reports = Lists.newArrayList();
    private boolean closed = false;

    @Override
    public void report(MetricsReport report) {
      reports.add(report);
    }

    @Override
    public void close() {
      this.closed = true;
    }
  }

  /**
   * Public no-arg reporter usable via {@code metrics-reporter-impl}. Captured reports live on a
   * static list so the test can inspect what reached the underlying reporter after CatalogUtil
   * instantiated it via reflection.
   */
  public static class StaticCapturingReporter implements MetricsReporter {
    static final List<MetricsReport> REPORTS = Lists.newCopyOnWriteArrayList();

    @Override
    public void report(MetricsReport report) {
      REPORTS.add(report);
    }
  }
}
