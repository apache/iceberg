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

import java.util.Set;
import org.apache.iceberg.rest.functions.RestFunctionService;
import org.apache.iceberg.spark.functions.SparkFunctions;
import org.apache.iceberg.spark.functions.UserSqlFunctions;
import org.apache.iceberg.spark.procedures.SparkProcedures;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.iceberg.spark.udf.SparkUDFRegistrar;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.ProcedureCatalog;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.procedures.UnboundProcedure;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

abstract class BaseCatalog
    implements StagingTableCatalog,
        ProcedureCatalog,
        SupportsNamespaces,
        HasIcebergCatalog,
        SupportsFunctions,
        ViewCatalog,
        SupportsReplaceView {
  private static final String USE_NULLABLE_QUERY_SCHEMA_CTAS_RTAS = "use-nullable-query-schema";
  private static final boolean USE_NULLABLE_QUERY_SCHEMA_CTAS_RTAS_DEFAULT = true;

  private boolean useNullableQuerySchema = USE_NULLABLE_QUERY_SCHEMA_CTAS_RTAS_DEFAULT;
  private String functionsRestUri = null;
  private String functionsRestAuth = null;
  private org.apache.iceberg.rest.functions.RestFunctionService restFunctions = null;
  private java.util.List<String> cachedRestFunctionNames = null;

  @Override
  public UnboundProcedure loadProcedure(Identifier ident) {
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

    throw new RuntimeException("Procedure " + ident + " not found");
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
            USE_NULLABLE_QUERY_SCHEMA_CTAS_RTAS,
            USE_NULLABLE_QUERY_SCHEMA_CTAS_RTAS_DEFAULT);

    // POC config for REST-backed function listing/loading
    this.functionsRestUri = options.get("functions.rest.uri");
    this.functionsRestAuth = options.get("functions.rest.auth");
    if (functionsRestUri != null && !functionsRestUri.isEmpty()) {
      this.restFunctions = new RestFunctionService(functionsRestUri, functionsRestAuth);
    }
  }

  @Override
  public boolean useNullableQuerySchema() {
    return useNullableQuerySchema;
  }

  private static boolean isSystemNamespace(String[] namespace) {
    return namespace.length == 1 && namespace[0].equalsIgnoreCase("system");
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    if (isFunctionNamespace(namespace)) {
      Set<String> names = new java.util.LinkedHashSet<>();
      names.addAll(SparkFunctions.list());
      names.addAll(UserSqlFunctions.list());

      // If configured, fetch functions from REST and register them as SQL UDFs
      if (restFunctions != null) {
        if (cachedRestFunctionNames == null) {
          cachedRestFunctionNames = restFunctions.listFunctions(namespace);
        }
        for (String fn : cachedRestFunctionNames) {
          try {
            // Only register if not already present
            if (!names.contains(fn)) {
              String json = restFunctions.getFunctionSpecJson(namespace, fn);
              SparkUDFRegistrar.registerFromJson(SparkSession.active(), fn, json);
              names.add(fn);
            }
          } catch (RuntimeException e) {
            // Best-effort: skip failing functions in POC
          }
        }
      }

      return names.stream().map(name -> Identifier.of(namespace, name)).toArray(Identifier[]::new);
    } else if (isExistingNamespace(namespace)) {
      return new Identifier[0];
    }
    throw new NoSuchNamespaceException(namespace);
  }
}
