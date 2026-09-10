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
 * instances that do not pass the configured namespace and table-name filters before forwarding to a
 * delegate reporter.
 *
 * <p>Filtering happens on two levels, either of which may be configured on its own:
 *
 * <ul>
 *   <li>namespace, via {@link CatalogProperties#METRICS_REPORTER_NAMESPACE_INCLUDE} and {@link
 *       CatalogProperties#METRICS_REPORTER_NAMESPACE_EXCLUDE}
 *   <li>table name, via {@link CatalogProperties#METRICS_REPORTER_TABLE_NAME_INCLUDE} and {@link
 *       CatalogProperties#METRICS_REPORTER_TABLE_NAME_EXCLUDE}
 * </ul>
 *
 * <p>Each property holds a comma-separated list of Java regular expressions. A report is forwarded
 * unless a filter drops it, and the levels are applied independently: an exclude match on either
 * level drops the report, and when a level has an include list configured the report must match it.
 * An exclude match always wins over an include match. When no property is set, {@link
 * #wrap(MetricsReporter, String, Map)} returns the delegate unchanged so the default path incurs no
 * overhead.
 *
 * <p>Patterns are matched against the entire namespace or table name rather than any substring of
 * it, so {@code prod\..*} matches {@code prod.db.table} but not {@code production.db.table}.
 *
 * <p>{@link MetricsReport} subtypes other than {@link ScanReport} and {@link CommitReport} are
 * forwarded without filtering, since they do not identify a table.
 *
 * <p>Reports carry the table name as a single flattened string, so the namespace is recovered by
 * dropping the catalog prefix and the last dot-separated element. A table name that itself contains
 * a dot is therefore attributed to a namespace one level deeper than it belongs to; filter on the
 * table-name level for those.
 */
public class FilteringMetricsReporter implements MetricsReporter {

  private static final List<String> SEPARATORS = ImmutableList.of(".", "/");

  private final MetricsReporter delegate;
  private final String catalogName;
  private final List<Pattern> namespaceInclude;
  private final List<Pattern> namespaceExclude;
  private final List<Pattern> tableNameInclude;
  private final List<Pattern> tableNameExclude;

  private FilteringMetricsReporter(
      MetricsReporter delegate,
      String catalogName,
      List<Pattern> namespaceInclude,
      List<Pattern> namespaceExclude,
      List<Pattern> tableNameInclude,
      List<Pattern> tableNameExclude) {
    this.delegate = delegate;
    this.catalogName = catalogName;
    this.namespaceInclude = namespaceInclude;
    this.namespaceExclude = namespaceExclude;
    this.tableNameInclude = tableNameInclude;
    this.tableNameExclude = tableNameExclude;
  }

  /**
   * Wraps the given delegate in a {@code FilteringMetricsReporter} when any of the namespace or
   * table-name filters is configured in {@code properties}; otherwise returns the delegate
   * unchanged so the default case incurs no runtime overhead.
   *
   * @param delegate the underlying reporter that receives forwarded reports
   * @param catalogName name of the catalog the reports originate from, used to derive a table's
   *     namespace from the reported table name. When null, namespace filters cannot be applied and
   *     configuring them is rejected.
   * @param properties catalog properties; consulted for the namespace and table-name filters
   * @return either the delegate unchanged, or a new filtering wrapper around it
   */
  public static MetricsReporter wrap(
      MetricsReporter delegate, String catalogName, Map<String, String> properties) {
    List<Pattern> namespaceInclude =
        compilePatterns(properties, CatalogProperties.METRICS_REPORTER_NAMESPACE_INCLUDE);
    List<Pattern> namespaceExclude =
        compilePatterns(properties, CatalogProperties.METRICS_REPORTER_NAMESPACE_EXCLUDE);
    List<Pattern> tableNameInclude =
        compilePatterns(properties, CatalogProperties.METRICS_REPORTER_TABLE_NAME_INCLUDE);
    List<Pattern> tableNameExclude =
        compilePatterns(properties, CatalogProperties.METRICS_REPORTER_TABLE_NAME_EXCLUDE);

    if (namespaceInclude.isEmpty()
        && namespaceExclude.isEmpty()
        && tableNameInclude.isEmpty()
        && tableNameExclude.isEmpty()) {
      return delegate;
    }

    if (catalogName == null && !(namespaceInclude.isEmpty() && namespaceExclude.isEmpty())) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot filter metrics by namespace without a catalog name: %s, %s",
              CatalogProperties.METRICS_REPORTER_NAMESPACE_INCLUDE,
              CatalogProperties.METRICS_REPORTER_NAMESPACE_EXCLUDE));
    }

    return new FilteringMetricsReporter(
        delegate,
        catalogName,
        namespaceInclude,
        namespaceExclude,
        tableNameInclude,
        tableNameExclude);
  }

  private static List<Pattern> compilePatterns(
      Map<String, String> properties, String propertyName) {
    String value = properties.get(propertyName);
    if (value == null || value.trim().isEmpty()) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<Pattern> patterns = ImmutableList.builder();
    for (String pattern : splitPatterns(value)) {
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

  /**
   * Splits a comma-separated list of regular expressions, ignoring commas that belong to a pattern
   * rather than separating two of them: bounded quantifiers such as {@code x{1,3}} and character
   * classes such as {@code [,;]} both contain commas that are part of the regex.
   */
  private static List<String> splitPatterns(String value) {
    ImmutableList.Builder<String> parts = ImmutableList.builder();
    StringBuilder current = new StringBuilder();
    int braceDepth = 0;
    boolean inCharClass = false;
    boolean escaped = false;

    for (char c : value.toCharArray()) {
      if (escaped) {
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (inCharClass) {
        if (c == ']') {
          inCharClass = false;
        }
      } else if (c == '[') {
        inCharClass = true;
      } else if (c == '{') {
        braceDepth++;
      } else if (c == '}') {
        braceDepth = Math.max(0, braceDepth - 1);
      } else if (c == ',' && braceDepth == 0) {
        parts.add(current.toString());
        current.setLength(0);
        continue;
      }

      current.append(c);
    }

    parts.add(current.toString());
    return parts.build();
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

    if (!namespaceInclude.isEmpty() || !namespaceExclude.isEmpty()) {
      String namespace = namespace(tableName);
      if (namespace == null || !passes(namespace, namespaceInclude, namespaceExclude)) {
        return;
      }
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

  /**
   * Derives the namespace of a reported table by removing the catalog name prefix and the table
   * name, mirroring how {@code CatalogUtil#fullTableName} builds the reported name. Returns an
   * empty string for a table directly under the catalog, or null when the name does not carry the
   * expected catalog prefix.
   */
  private String namespace(String tableName) {
    // CatalogUtil#fullTableName joins a URI-like catalog name with / and any other name with .,
    // while RESTSessionCatalog always joins with ., so accept either rather than deciding from the
    // catalog name which one the report must have used.
    String withoutCatalog = null;
    for (String separator : SEPARATORS) {
      String prefix = catalogName.endsWith(separator) ? catalogName : catalogName + separator;
      if (tableName.startsWith(prefix)) {
        withoutCatalog = tableName.substring(prefix.length());
        break;
      }
    }

    if (withoutCatalog == null) {
      return null;
    }

    int lastDot = withoutCatalog.lastIndexOf('.');
    return lastDot < 0 ? "" : withoutCatalog.substring(0, lastDot);
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
