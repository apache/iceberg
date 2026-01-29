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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.functions.RestFunctionService;
import org.apache.iceberg.spark.functions.SparkFunctions;
import org.apache.iceberg.spark.procedures.SparkProcedures;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.iceberg.spark.udf.SqlFunctionCatalog;
import org.apache.iceberg.spark.udf.SqlFunctionSpec;
import org.apache.iceberg.spark.udf.SqlFunctionSpecParser;
import org.apache.iceberg.spark.udf.SqlUdfCatalogFunction;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.ProcedureCatalog;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.catalog.procedures.UnboundProcedure;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

abstract class BaseCatalog
    implements StagingTableCatalog,
        ProcedureCatalog,
        SupportsNamespaces,
        HasIcebergCatalog,
        SupportsFunctions,
        ViewCatalog,
        SupportsReplaceView,
        SqlFunctionCatalog {
  private static final String USE_NULLABLE_QUERY_SCHEMA_CTAS_RTAS = "use-nullable-query-schema";
  private static final boolean USE_NULLABLE_QUERY_SCHEMA_CTAS_RTAS_DEFAULT = true;

  private boolean useNullableQuerySchema = USE_NULLABLE_QUERY_SCHEMA_CTAS_RTAS_DEFAULT;
  private String functionsRestUri = null;
  private String functionsRestAuth = null;
  private RestFunctionService restFunctions = null;
  private final java.util.Map<String, java.util.List<String>> cachedRestFunctionNamesByNamespace =
      new java.util.concurrent.ConcurrentHashMap<>();
  private final java.util.Map<String, java.util.List<String>>
      cachedCatalogFunctionNamesByNamespace = new java.util.concurrent.ConcurrentHashMap<>();

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
  public Identifier[] listProcedures(String[] namespace) {
    if (isSystemNamespace(namespace)) {
      return SparkProcedures.names().stream()
          .map(name -> Identifier.of(namespace, name))
          .toArray(Identifier[]::new);
    } else {
      return new Identifier[0];
    }
  }

  @Override
  public boolean isFunctionNamespace(String[] namespace) {
    // Allow for empty namespace, as Spark's storage partitioned joins look up
    // the corresponding functions to generate transforms for partitioning
    // with an empty namespace, such as `bucket`.
    // Otherwise, use `system` namespace.
    //
    if (namespace.length == 0 || isSystemNamespace(namespace)) {
      return true;
    }

    // Note: user namespaces (e.g. `default`) are NOT function namespaces for built-in Iceberg
    // functions. They are treated as function namespaces only when user-defined functions exist.
    //
    // For the (legacy) POC REST function service (functions.rest.uri), namespaces are treated as
    // function namespaces if they exist.
    if (restFunctions != null) {
      return namespaceExists(namespace);
    }

    // For catalog-backed REST functions, treat namespaces as function namespaces only if the
    // catalog reports at least one function under the namespace. This preserves the historical
    // behavior that existing namespaces (like `default`) don't implicitly expose functions.
    RESTCatalog catalog = restCatalog();
    if (catalog != null && namespaceExists(namespace)) {
      String cacheKey = namespaceCacheKey(namespace);
      java.util.List<String> cached =
          cachedCatalogFunctionNamesByNamespace.computeIfAbsent(
              cacheKey, ignored -> catalog.listFunctions(Namespace.of(namespace)));
      return !cached.isEmpty();
    }

    return false;
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

    // Optional POC config for REST-backed function listing/loading (read-only Stage 1: list/load
    // specs). A production REST-catalog implementation should serve functions from the catalog
    // server itself, in which case this config is not needed.
    this.functionsRestUri = options.get("functions.rest.uri");
    this.functionsRestAuth = options.get("functions.rest.auth");
    if (functionsRestUri != null && !functionsRestUri.isEmpty()) {
      this.restFunctions = new RestFunctionService(functionsRestUri, functionsRestAuth);
    }
  }

  private static String namespaceCacheKey(String[] namespace) {
    // deterministic and unambiguous enough for caching test usage
    return String.join("\u0001", namespace);
  }

  @Override
  public boolean useNullableQuerySchema() {
    return useNullableQuerySchema;
  }

  private static boolean isSystemNamespace(String[] namespace) {
    return namespace.length == 1 && namespace[0].equalsIgnoreCase("system");
  }

  private RESTCatalog restCatalog() {
    // Note: when SparkCatalog wraps the underlying Iceberg catalog in CachingCatalog, the runtime
    // type will no longer be RESTCatalog. For now, REST-backed functions via the catalog are only
    // supported when the underlying catalog is directly a RESTCatalog (cache disabled).
    if (icebergCatalog() instanceof RESTCatalog) {
      return (RESTCatalog) icebergCatalog();
    }

    return null;
  }

  private void addBuiltInFunctions(Set<String> names, String[] namespace) {
    if (namespace.length == 0 || isSystemNamespace(namespace)) {
      names.addAll(SparkFunctions.list());
    }
  }

  private void addRestServiceFunctions(Set<String> names, String[] namespace) {
    if (restFunctions == null) {
      return;
    }

    String cacheKey = namespaceCacheKey(namespace);
    java.util.List<String> cached =
        cachedRestFunctionNamesByNamespace.computeIfAbsent(
            cacheKey, ignored -> restFunctions.listFunctions(namespace));
    names.addAll(cached);
  }

  private void addRestCatalogFunctions(Set<String> names, String[] namespace) {
    if (namespace.length == 0 || isSystemNamespace(namespace)) {
      return;
    }

    RESTCatalog catalog = restCatalog();
    if (catalog == null) {
      return;
    }

    String cacheKey = namespaceCacheKey(namespace);
    java.util.List<String> cached =
        cachedCatalogFunctionNamesByNamespace.computeIfAbsent(
            cacheKey, ignored -> catalog.listFunctions(Namespace.of(namespace)));
    names.addAll(cached);
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    if (!isFunctionNamespace(namespace)) {
      if (isExistingNamespace(namespace)) {
        return new Identifier[0];
      }
      throw new NoSuchNamespaceException(namespace);
    }

    Set<String> names = new java.util.LinkedHashSet<>();
    addBuiltInFunctions(names, namespace);
    addRestServiceFunctions(names, namespace);
    addRestCatalogFunctions(names, namespace);
    return names.stream().map(name -> Identifier.of(namespace, name)).toArray(Identifier[]::new);
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    String[] namespace = ident.namespace();
    String name = ident.name();

    if (!isFunctionNamespace(namespace)) {
      throw new NoSuchFunctionException(ident);
    }

    // Try built-ins first, but only for empty/system namespaces.
    if (namespace.length == 0 || isSystemNamespace(namespace)) {
      UnboundFunction builtin = SparkFunctions.load(name);
      if (builtin != null) {
        return builtin;
      }
    }

    // For Iceberg SQL UDFs, return a bindable placeholder so Spark's analyzer does not fail early.
    // The actual semantics are provided by the Spark extensions rewrite rule using loadSqlFunction.
    try {
      SqlFunctionSpec spec = loadSqlFunction(ident);
      return new SqlUdfCatalogFunction(name(), ident, spec);
    } catch (NoSuchFunctionException e) {
      throw e;
    }
  }

  @Override
  public SqlFunctionSpec loadSqlFunction(Identifier ident) throws NoSuchFunctionException {
    String[] namespace = ident.namespace();
    String name = ident.name();
    if (!isFunctionNamespace(namespace)) {
      throw new NoSuchFunctionException(ident);
    }

    // If explicitly configured, use the (legacy) POC REST function service.
    if (restFunctions != null) {
      try {
        String json = restFunctions.getFunctionSpecJson(namespace, name);
        return SqlFunctionSpecParser.parseScalar(json);
      } catch (RuntimeException e) {
        throw new NoSuchFunctionException(ident);
      }
    }

    // Otherwise, prefer REST-catalog-backed functions if available.
    RESTCatalog catalog = restCatalog();
    if (catalog != null && namespace.length > 0 && !isSystemNamespace(namespace)) {
      try {
        String json = catalog.loadFunctionSpec(Namespace.of(namespace), name).toString();
        return SqlFunctionSpecParser.parseScalar(json);
      } catch (RuntimeException e) {
        throw new NoSuchFunctionException(ident);
      }
    }

    throw new NoSuchFunctionException(ident);
  }
}
