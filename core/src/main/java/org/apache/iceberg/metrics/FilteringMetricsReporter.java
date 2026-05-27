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

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.iceberg.CatalogProperties;

/**
 * A {@link MetricsReporter} wrapper that drops {@link ScanReport} and {@link CommitReport}
 * instances whose {@code tableName()} does not pass the configured include / exclude patterns
 * before forwarding to a delegate reporter.
 *
 * <p>The patterns are Java regular expressions sourced from the catalog properties {@link
 * CatalogProperties#METRICS_REPORTER_TABLE_NAME_INCLUDE} and {@link
 * CatalogProperties#METRICS_REPORTER_TABLE_NAME_EXCLUDE}. When both are set, {@code exclude} wins
 * over {@code include} (an explicit deny overrides an include). When neither is set, {@link
 * #wrap(MetricsReporter, Map)} returns the delegate unchanged.
 *
 * <p>{@link MetricsReport} subtypes other than {@link ScanReport} and {@link CommitReport} are
 * forwarded to the delegate without filtering, since they do not expose a {@code tableName()}.
 */
public class FilteringMetricsReporter implements MetricsReporter {

  private final MetricsReporter delegate;
  private final Pattern include;
  private final Pattern exclude;

  private FilteringMetricsReporter(MetricsReporter delegate, Pattern include, Pattern exclude) {
    this.delegate = delegate;
    this.include = include;
    this.exclude = exclude;
  }

  /**
   * Wraps the given delegate in a {@code FilteringMetricsReporter} when either include or exclude
   * is configured in {@code properties}; otherwise returns the delegate unchanged so the default
   * case incurs no runtime overhead.
   *
   * @param delegate the underlying reporter that receives forwarded reports
   * @param properties catalog properties; consulted for the table-name include / exclude regex
   * @return either the delegate unchanged, or a new filtering wrapper around it
   */
  public static MetricsReporter wrap(MetricsReporter delegate, Map<String, String> properties) {
    Pattern include =
        compileIfPresent(
            properties.get(CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE),
            CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE);
    Pattern exclude =
        compileIfPresent(
            properties.get(CatalogProperties.METRICS_REPORTER_TABLE_NAME_EXCLUDE),
            CatalogProperties.METRICS_REPORTER_TABLE_NAME_EXCLUDE);
    if (include == null && exclude == null) {
      return delegate;
    }
    return new FilteringMetricsReporter(delegate, include, exclude);
  }

  private static Pattern compileIfPresent(String value, String propertyName) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    try {
      return Pattern.compile(value);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid regex for %s: %s", propertyName, value), e);
    }
  }

  @Override
  public void report(MetricsReport report) {
    String tableName = extractTableName(report);
    if (tableName == null) {
      delegate.report(report);
      return;
    }
    if (exclude != null && exclude.matcher(tableName).matches()) {
      return;
    }
    if (include != null && !include.matcher(tableName).matches()) {
      return;
    }
    delegate.report(report);
  }

  private static String extractTableName(MetricsReport report) {
    if (report instanceof ScanReport) {
      return ((ScanReport) report).tableName();
    }
    if (report instanceof CommitReport) {
      return ((CommitReport) report).tableName();
    }
    return null;
  }

  @Override
  public void close() {
    delegate.close();
  }
}
