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
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.View;

/**
 * Converts Iceberg view metadata to Spark's {@link View} representation.
 *
 * <p>Keeps this conversion in Iceberg instead of relying on Spark's built-in view handling so
 * reserved properties and Iceberg defaults are exposed consistently to Spark commands.
 */
public class SparkView {

  public static final String QUERY_COLUMN_NAMES = "spark.query-column-names";
  public static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of(
          TableCatalog.PROP_PROVIDER,
          TableCatalog.PROP_LOCATION,
          TableCatalog.PROP_TABLE_TYPE,
          FORMAT_VERSION,
          QUERY_COLUMN_NAMES);

  private SparkView() {}

  public static View toView(String catalogName, org.apache.iceberg.view.View icebergView) {
    SQLViewRepresentation sqlRepr = icebergView.sqlFor("spark");
    Preconditions.checkState(sqlRepr != null, "Cannot load SQL for view %s", icebergView.name());

    Namespace defaultNamespace = icebergView.currentVersion().defaultNamespace();
    String defaultCatalog = icebergView.currentVersion().defaultCatalog();

    return new View.Builder()
        .withQueryText(sqlRepr.sql())
        .withCurrentCatalog(defaultCatalog != null ? defaultCatalog : catalogName)
        .withCurrentNamespace(defaultNamespace != null ? defaultNamespace.levels() : new String[0])
        .withSchema(SparkSchemaUtil.convert(icebergView.schema()))
        .withQueryColumnNames(queryColumnNames(icebergView.properties()))
        .withProperties(properties(icebergView))
        .build();
  }

  private static String[] queryColumnNames(Map<String, String> properties) {
    String queryColumnNames = properties.get(QUERY_COLUMN_NAMES);
    return queryColumnNames != null && !queryColumnNames.isEmpty()
        ? queryColumnNames.split(",")
        : new String[0];
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
        .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(propsBuilder::put);

    return propsBuilder.build();
  }
}
