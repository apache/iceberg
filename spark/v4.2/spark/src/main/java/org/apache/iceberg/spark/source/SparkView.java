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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewOperations;
import org.apache.spark.sql.connector.catalog.DependencyList;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableSummary;
import org.apache.spark.sql.connector.catalog.View;

/**
 * Converts Iceberg view metadata to Spark's {@link View} representation.
 *
 * <p>Keeps this conversion in Iceberg instead of relying on Spark's built-in view handling so
 * reserved properties and Iceberg defaults are exposed consistently to Spark commands.
 */
public class SparkView {

  public static final String PROP_CREATE_ENGINE_VERSION = "create_engine_version";
  public static final String PROP_ENGINE_VERSION = "engine_version";
  public static final String QUERY_COLUMN_NAMES = "spark.query-column-names";
  public static final String SQL_CONFIG_PREFIX = "spark.sql-config.";
  public static final String VIEW_SCHEMA_MODE = "spark.view-schema-mode";
  public static final String VIEW_DEPENDENCIES = "spark.view-dependencies";
  private static final Set<String> INTERNAL_PROPERTIES =
      ImmutableSet.of(
          TableCatalog.PROP_PROVIDER,
          TableCatalog.PROP_LOCATION,
          TableCatalog.PROP_TABLE_TYPE,
          FORMAT_VERSION,
          PROP_CREATE_ENGINE_VERSION,
          PROP_ENGINE_VERSION,
          QUERY_COLUMN_NAMES,
          VIEW_SCHEMA_MODE,
          VIEW_DEPENDENCIES);

  private SparkView() {}

  public static View toView(String catalogName, org.apache.iceberg.view.View icebergView) {
    SQLViewRepresentation sqlRepr = icebergView.sqlFor("spark");
    Preconditions.checkState(sqlRepr != null, "Cannot load SQL for view %s", icebergView.name());

    Namespace defaultNamespace = icebergView.currentVersion().defaultNamespace();
    String defaultCatalog = icebergView.currentVersion().defaultCatalog();

    View.Builder builder =
        new View.Builder()
            .withQueryText(sqlRepr.sql())
            .withCurrentCatalog(defaultCatalog != null ? defaultCatalog : catalogName)
            .withCurrentNamespace(
                defaultNamespace != null ? defaultNamespace.levels() : new String[0])
            .withSchema(SparkSchemaUtil.convert(icebergView.schema()))
            .withQueryColumnNames(queryColumnNames(icebergView.properties()))
            .withSqlConfigs(sqlConfigs(icebergView.properties()))
            .withProperties(properties(icebergView));

    String schemaMode = icebergView.properties().get(VIEW_SCHEMA_MODE);
    if (schemaMode != null) {
      builder.withSchemaMode(schemaMode);
    }

    DependencyList dependencies = viewDependencies(icebergView.properties());
    if (dependencies != null) {
      builder.withViewDependencies(dependencies);
    }

    return builder.build();
  }

  /**
   * Returns reserved properties that preserve Spark view metadata not represented by Iceberg view
   * metadata fields.
   */
  public static Map<String, String> internalProperties(View view) {
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    String tableType = view.properties().get(TableCatalog.PROP_TABLE_TYPE);
    if (tableType != null && !TableSummary.VIEW_TABLE_TYPE.equals(tableType)) {
      propsBuilder.put(TableCatalog.PROP_TABLE_TYPE, tableType);
    }

    propsBuilder.put(QUERY_COLUMN_NAMES, String.join(",", view.queryColumnNames()));
    view.sqlConfigs().forEach((key, value) -> propsBuilder.put(SQL_CONFIG_PREFIX + key, value));

    if (view.schemaMode() != null) {
      propsBuilder.put(VIEW_SCHEMA_MODE, view.schemaMode());
    }

    if (view.viewDependencies() != null) {
      propsBuilder.put(
          VIEW_DEPENDENCIES, SparkViewDependenciesParser.toJson(view.viewDependencies()));
    }

    return propsBuilder.build();
  }

  public static boolean isReservedProperty(String property) {
    return INTERNAL_PROPERTIES.contains(property) || property.startsWith(SQL_CONFIG_PREFIX);
  }

  private static String[] queryColumnNames(Map<String, String> properties) {
    String queryColumnNames = properties.get(QUERY_COLUMN_NAMES);
    return queryColumnNames != null && !queryColumnNames.isEmpty()
        ? queryColumnNames.split(",")
        : new String[0];
  }

  private static Map<String, String> sqlConfigs(Map<String, String> properties) {
    ImmutableMap.Builder<String, String> configsBuilder = ImmutableMap.builder();
    properties.forEach(
        (key, value) -> {
          if (key.startsWith(SQL_CONFIG_PREFIX)) {
            configsBuilder.put(key.substring(SQL_CONFIG_PREFIX.length()), value);
          }
        });
    return configsBuilder.build();
  }

  private static DependencyList viewDependencies(Map<String, String> properties) {
    String dependencies = properties.get(VIEW_DEPENDENCIES);
    return dependencies != null ? SparkViewDependenciesParser.fromJson(dependencies) : null;
  }

  private static Map<String, String> properties(org.apache.iceberg.view.View icebergView) {
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();

    propsBuilder.put(TableCatalog.PROP_PROVIDER, "iceberg");
    propsBuilder.put(TableCatalog.PROP_LOCATION, icebergView.location());

    if (icebergView instanceof BaseView) {
      ViewOperations ops = ((BaseView) icebergView).operations();
      propsBuilder.put(FORMAT_VERSION, String.valueOf(ops.current().formatVersion()));
    }

    icebergView.properties().entrySet().stream()
        .filter(
            entry ->
                !isReservedProperty(entry.getKey())
                    || TableCatalog.PROP_TABLE_TYPE.equals(entry.getKey())
                    || PROP_CREATE_ENGINE_VERSION.equals(entry.getKey())
                    || PROP_ENGINE_VERSION.equals(entry.getKey()))
        .forEach(propsBuilder::put);

    return propsBuilder.build();
  }
}
