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
    MetricsReporter wrapped = FilteringMetricsReporter.wrap(delegate, null, ImmutableMap.of());
    assertThat(wrapped).isSameAs(delegate);
  }

  @Test
  public void wrapReturnsDelegateWhenPropertiesAreEmpty() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            null,
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
            null,
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
            null,
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
            null,
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
            null,
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
                    null,
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
            delegate,
            null,
            ImmutableMap.of(CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE, ".*"));

    wrapped.close();

    assertThat(delegate.closed).isTrue();
  }

  @Test
  public void includeAcceptsCommaSeparatedPatterns() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            null,
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE,
                "prod_db\\..*, analytics_db\\..*"));

    ScanReport analytics = newScanReport("analytics_db.events");
    wrapped.report(SCAN_PROD);
    wrapped.report(analytics);
    wrapped.report(SCAN_DEV);

    assertThat(delegate.reports).containsExactly(SCAN_PROD, analytics);
  }

  @Test
  public void excludeAcceptsCommaSeparatedPatterns() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            null,
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_TABLE_NAME_EXCLUDE, ".*\\.tmp_.*,dev_db\\..*"));

    wrapped.report(SCAN_PROD);
    wrapped.report(SCAN_TMP);
    wrapped.report(SCAN_DEV);

    assertThat(delegate.reports).containsExactly(SCAN_PROD);
  }

  @Test
  public void patternsMatchWholeNameNotSubstring() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            null,
            ImmutableMap.of(CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE, "prod\\..*"));

    ScanReport prod = newScanReport("prod.orders");
    // names that a substring match would wrongly accept
    wrapped.report(prod);
    wrapped.report(newScanReport("production.orders"));
    wrapped.report(newScanReport("prod_sandbox.orders"));
    wrapped.report(newScanReport("staging.prod.orders"));

    assertThat(delegate.reports).containsExactly(prod);
  }

  @Test
  public void namespaceIncludeFiltersOnNamespaceOnly() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            "cat",
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_NAMESPACE_INCLUDE, "prod,analytics"));

    ScanReport prod = newScanReport("cat.prod.orders");
    ScanReport analytics = newScanReport("cat.analytics.events");
    wrapped.report(prod);
    wrapped.report(analytics);
    wrapped.report(newScanReport("cat.staging.orders"));
    // a namespace that only shares a prefix must not match
    wrapped.report(newScanReport("cat.production.orders"));

    assertThat(delegate.reports).containsExactly(prod, analytics);
  }

  @Test
  public void namespaceExcludeDropsMatchingNamespaces() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            "cat",
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_NAMESPACE_EXCLUDE, "staging,sandbox"));

    ScanReport prod = newScanReport("cat.prod.orders");
    wrapped.report(prod);
    wrapped.report(newScanReport("cat.staging.orders"));
    wrapped.report(newScanReport("cat.sandbox.orders"));

    assertThat(delegate.reports).containsExactly(prod);
  }

  @Test
  public void namespaceAndTableNameFiltersCombine() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            "cat",
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_NAMESPACE_INCLUDE, "prod",
                CatalogProperties.METRICS_REPORTER_TABLE_NAME_EXCLUDE, ".*\\.bench_.*"));

    ScanReport prod = newScanReport("cat.prod.orders");
    wrapped.report(prod);
    // in the included namespace, but excluded by table name
    wrapped.report(newScanReport("cat.prod.bench_scratch"));
    // outside the included namespace
    wrapped.report(newScanReport("cat.staging.orders"));

    assertThat(delegate.reports).containsExactly(prod);
  }

  @Test
  public void namespaceFilterHandlesMultiLevelAndEmptyNamespaces() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            "cat",
            ImmutableMap.of(CatalogProperties.METRICS_REPORTER_NAMESPACE_INCLUDE, "a\\.b"));

    ScanReport nested = newScanReport("cat.a.b.orders");
    wrapped.report(nested);
    wrapped.report(newScanReport("cat.a.orders"));
    // table directly under the catalog has an empty namespace
    wrapped.report(newScanReport("cat.orders"));

    assertThat(delegate.reports).containsExactly(nested);
  }

  @Test
  public void namespaceFilterHandlesCatalogNameContainingDots() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            "my.cat",
            ImmutableMap.of(CatalogProperties.METRICS_REPORTER_NAMESPACE_INCLUDE, "db"));

    ScanReport report = newScanReport("my.cat.db.orders");
    wrapped.report(report);
    wrapped.report(newScanReport("my.cat.other.orders"));

    assertThat(delegate.reports).containsExactly(report);
  }

  @Test
  public void namespaceFilterHandlesUriStyleCatalogNames() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            "thrift://localhost:9083",
            ImmutableMap.of(CatalogProperties.METRICS_REPORTER_NAMESPACE_INCLUDE, "db"));

    ScanReport report = newScanReport("thrift://localhost:9083/db.orders");
    wrapped.report(report);
    wrapped.report(newScanReport("thrift://localhost:9083/other.orders"));

    assertThat(delegate.reports).containsExactly(report);
  }

  @Test
  public void namespaceFilterHandlesUriStyleCatalogNamesJoinedWithADot() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            "thrift://localhost:9083",
            ImmutableMap.of(CatalogProperties.METRICS_REPORTER_NAMESPACE_INCLUDE, "db"));

    // RESTSessionCatalog joins the catalog name with a dot even when it looks like a URI
    ScanReport report = newScanReport("thrift://localhost:9083.db.orders");
    wrapped.report(report);
    wrapped.report(newScanReport("thrift://localhost:9083.other.orders"));

    assertThat(delegate.reports).containsExactly(report);
  }

  @Test
  public void patternsMayContainBoundedQuantifiers() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            null,
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE,
                "prod_db\\.table_[0-9]{1,3}, prod_db\\.orders"));

    ScanReport shard = newScanReport("prod_db.table_42");
    wrapped.report(shard);
    wrapped.report(SCAN_PROD);
    wrapped.report(newScanReport("prod_db.table_1234"));

    assertThat(delegate.reports).containsExactly(shard, SCAN_PROD);
  }

  @Test
  public void patternsMayContainCharacterClassesWithCommas() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            null,
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_TABLE_NAME_EXCLUDE, "prod_db\\.[a-z,]+"));

    wrapped.report(SCAN_PROD);
    wrapped.report(SCAN_DEV);

    assertThat(delegate.reports).containsExactly(SCAN_DEV);
  }

  @Test
  public void namespaceFilterDropsReportsWithoutExpectedCatalogPrefix() {
    CapturingMetricsReporter delegate = new CapturingMetricsReporter();
    MetricsReporter wrapped =
        FilteringMetricsReporter.wrap(
            delegate,
            "cat",
            ImmutableMap.of(CatalogProperties.METRICS_REPORTER_NAMESPACE_INCLUDE, ".*"));

    // the namespace cannot be derived, so the report cannot be shown to pass the filter
    wrapped.report(newScanReport("other.db.orders"));

    assertThat(delegate.reports).isEmpty();
  }

  @Test
  public void namespaceFilterWithoutCatalogNameIsRejected() {
    assertThatThrownBy(
            () ->
                FilteringMetricsReporter.wrap(
                    new CapturingMetricsReporter(),
                    null,
                    ImmutableMap.of(CatalogProperties.METRICS_REPORTER_NAMESPACE_INCLUDE, "prod")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(CatalogProperties.METRICS_REPORTER_NAMESPACE_INCLUDE);
  }

  @Test
  public void wrapThrowsClearErrorForInvalidNamespaceRegex() {
    assertThatThrownBy(
            () ->
                FilteringMetricsReporter.wrap(
                    new CapturingMetricsReporter(),
                    "cat",
                    ImmutableMap.of(
                        CatalogProperties.METRICS_REPORTER_NAMESPACE_EXCLUDE, "[invalid")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(CatalogProperties.METRICS_REPORTER_NAMESPACE_EXCLUDE)
        .hasMessageContaining("[invalid");
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
