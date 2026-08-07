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

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * A {@link MetricsReporter} wrapper that drops {@link ScanReport} and {@link CommitReport}
 * instances whose {@code tableName()} does not pass the configured include / exclude filters before
 * forwarding to a delegate reporter.
 *
 * <p>The filters come from the catalog properties {@link
 * CatalogProperties#METRICS_REPORTER_TABLE_NAME_INCLUDE} and {@link
 * CatalogProperties#METRICS_REPORTER_TABLE_NAME_EXCLUDE}, each holding a comma-separated list of
 * Java regular expressions. An exclude match wins over an include match. When neither is set,
 * {@link #wrap(MetricsReporter, Map)} returns the delegate unchanged so the default path incurs no
 * overhead.
 *
 * <p>Patterns are matched against the entire table name rather than any substring of it, so {@code
 * prod\..*} matches {@code prod.db.table} but not {@code production.db.table}.
 *
 * <p>{@link MetricsReport} subtypes other than {@link ScanReport} and {@link CommitReport} are
 * forwarded without filtering, since they do not identify a table.
 */
public class FilteringMetricsReporter implements MetricsReporter {

  private final MetricsReporter delegate;
  private final List<Pattern> tableNameInclude;
  private final List<Pattern> tableNameExclude;

  private FilteringMetricsReporter(
      MetricsReporter delegate, List<Pattern> tableNameInclude, List<Pattern> tableNameExclude) {
    this.delegate = delegate;
    this.tableNameInclude = tableNameInclude;
    this.tableNameExclude = tableNameExclude;
  }

  /**
   * Wraps the given delegate in a {@code FilteringMetricsReporter} when either include or exclude
   * is configured in {@code properties}; otherwise returns the delegate unchanged so the default
   * case incurs no runtime overhead.
   *
   * @param delegate the underlying reporter that receives forwarded reports
   * @param properties catalog properties; consulted for the table-name include / exclude filters
   * @return either the delegate unchanged, or a new filtering wrapper around it
   */
  public static MetricsReporter wrap(MetricsReporter delegate, Map<String, String> properties) {
    List<Pattern> tableNameInclude =
        compilePatterns(properties, CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE);
    List<Pattern> tableNameExclude =
        compilePatterns(properties, CatalogProperties.METRICS_REPORTER_TABLE_NAME_EXCLUDE);

    if (tableNameInclude.isEmpty() && tableNameExclude.isEmpty()) {
      return delegate;
    }

    return new FilteringMetricsReporter(delegate, tableNameInclude, tableNameExclude);
  }

  private static List<Pattern> compilePatterns(
      Map<String, String> properties, String propertyName) {
    String value = properties.get(propertyName);
    if (value == null || value.trim().isEmpty()) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<Pattern> patterns = ImmutableList.builder();
    for (String pattern : value.split(",", -1)) {
      String trimmed = pattern.trim();
      if (trimmed.isEmpty()) {
        continue;
      }

      try {
        patterns.add(Pattern.compile(trimmed));
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException(
            String.format("Invalid regex for %s: %s", propertyName, trimmed), e);
      }
    }

    return patterns.build();
  }

  @Override
  public void report(MetricsReport report) {
    String tableName = tableName(report);
    if (tableName == null) {
      delegate.report(report);
      return;
    }

    if (!passes(tableName, tableNameInclude, tableNameExclude)) {
      return;
    }

    delegate.report(report);
  }

  private static boolean passes(String value, List<Pattern> include, List<Pattern> exclude) {
    if (matchesAny(value, exclude)) {
      return false;
    }

    return include.isEmpty() || matchesAny(value, include);
  }

  private static boolean matchesAny(String value, List<Pattern> patterns) {
    for (Pattern pattern : patterns) {
      if (pattern.matcher(value).matches()) {
        return true;
      }
    }

    return false;
  }

  private static String tableName(MetricsReport report) {
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
