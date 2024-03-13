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
package org.apache.iceberg.spark;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import scala.collection.JavaConverters;

// Possible to merge with Spark3Util
public class MaterializedViewUtil {

  private MaterializedViewUtil() {}

  public static final String MATERIALIZED_VIEW_PROPERTY_KEY = "iceberg.materialized.view";
  public static final String MATERIALIZED_VIEW_STORAGE_TABLE_PROPERTY_KEY =
      "iceberg.materialized.view.storage.table";
  public static final String MATERIALIZED_VIEW_BASE_SNAPSHOT_PROPERTY_KEY_PREFIX =
      "iceberg.base.snapshot.";
  public static final String MATERIALIZED_VIEW_VERSION_PROPERTY_KEY =
      "iceberg.materialized.view.version";
  private static final String MATERIALIZED_VIEW_STORAGE_TABLE_IDENTIFIER_SUFFIX = ".storage.table";

  public static List<Table> extractBaseTables(String query) {
    return extractBaseTableIdentifiers(query).stream()
        .filter(optional -> !optional.isEmpty())
        .map(id -> toSparkTable(id).get())
        .collect(Collectors.toList());
  }

  private static List<List<String>> extractBaseTableIdentifiers(String query) {
    try {
      // Parse the SQL query to get the LogicalPlan
      LogicalPlan logicalPlan = SparkSession.active().sessionState().sqlParser().parsePlan(query);

      // Recursively traverse the LogicalPlan to extract base table names
      return extractBaseTableIdentifiers(logicalPlan).stream()
          .distinct()
          .collect(Collectors.toList());
    } catch (ParseException e) {
      throw new IllegalArgumentException("Failed to parse the SQL query: " + query, e);
    }
  }

  private static List<List<String>> extractBaseTableIdentifiers(LogicalPlan plan) {
    if (plan instanceof UnresolvedRelation) {
      UnresolvedRelation relation = (UnresolvedRelation) plan;
      List<List<String>> result = Lists.newArrayListWithCapacity(1);
      result.add(JavaConverters.seqAsJavaList(relation.multipartIdentifier()));
      return result;
    } else {
      return (JavaConverters.seqAsJavaList(plan.children()))
          .stream()
              .flatMap(child -> extractBaseTableIdentifiers(child).stream())
              .collect(Collectors.toList());
    }
  }

  public static Optional<Table> toSparkTable(List<String> multipartIdent) {
    Spark3Util.CatalogAndIdentifier catalogAndIdentifier =
        Spark3Util.catalogAndIdentifier(SparkSession.active(), multipartIdent);
    if (catalogAndIdentifier.catalog() instanceof TableCatalog) {
      TableCatalog tableCatalog = (TableCatalog) catalogAndIdentifier.catalog();
      try {
        return Optional.of(tableCatalog.loadTable(catalogAndIdentifier.identifier()));
      } catch (Exception e) {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  public static Identifier getDefaultMaterializedViewStorageTableIdentifier(
      Identifier viewIdentifier) {
    return Identifier.of(
        viewIdentifier.namespace(),
        viewIdentifier.name() + MATERIALIZED_VIEW_STORAGE_TABLE_IDENTIFIER_SUFFIX);
  }
}
