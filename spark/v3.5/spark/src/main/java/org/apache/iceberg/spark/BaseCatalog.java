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

import org.apache.iceberg.spark.procedures.SparkProcedures;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.iceberg.catalog.Procedure;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

abstract class BaseCatalog
    implements StagingTableCatalog,
        ProcedureCatalog,
        SupportsNamespaces,
        HasIcebergCatalog,
        SupportsFunctions {
  /**
   * Controls whether to mark all the fields as nullable when executing CREATE/REPLACE TABLE ... AS
   * SELECT ... and creating the table. If false, fields' nullability will be preserved when
   * creating the table.
   */
  private static final String TABLE_CREATE_NULLABLE_QUERY_SCHEMA = "use-nullable-query-schema";

  private static final boolean TABLE_CREATE_NULLABLE_QUERY_SCHEMA_DEFAULT = true;

  private boolean useNullableQuerySchema = TABLE_CREATE_NULLABLE_QUERY_SCHEMA_DEFAULT;

  @Override
  public Procedure loadProcedure(Identifier ident) throws NoSuchProcedureException {
    String[] namespace = ident.namespace();
    String name = ident.name();

    // namespace resolution is case insensitive until we have a way to configure case sensitivity in
    // catalogs
    if (isSystemNamespace(namespace)) {
      ProcedureBuilder builder = SparkProcedures.newBuilder(name);
      if (builder != null) {
        return builder.withTableCatalog(this).build();
      }
    }

    throw new NoSuchProcedureException(ident);
  }

  @Override
  public boolean isFunctionNamespace(String[] namespace) {
    // Allow for empty namespace, as Spark's storage partitioned joins look up
    // the corresponding functions to generate transforms for partitioning
    // with an empty namespace, such as `bucket`.
    // Otherwise, use `system` namespace.
    return namespace.length == 0 || isSystemNamespace(namespace);
  }

  @Override
  public boolean isExistingNamespace(String[] namespace) {
    return namespaceExists(namespace);
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.useNullableQuerySchema =
        PropertyUtil.propertyAsBoolean(
            options,
            TABLE_CREATE_NULLABLE_QUERY_SCHEMA,
            TABLE_CREATE_NULLABLE_QUERY_SCHEMA_DEFAULT);
  }

  @Override
  public boolean useNullableQuerySchema() {
    return useNullableQuerySchema;
  }

  private static boolean isSystemNamespace(String[] namespace) {
    return namespace.length == 1 && namespace[0].equalsIgnoreCase("system");
  }
}
