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
package org.apache.iceberg.spark.udf;

import java.util.stream.Collectors;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.StructType;

/**
 * Minimal UnboundFunction placeholder for Iceberg SQL UDFs backed by a catalog (e.g., REST).
 *
 * <p>This enables Spark to resolve the function name through the catalog (so Spark's analyzer
 * doesn't fail early), but execution is provided by Iceberg's analyzer/optimizer rewrite rule (not
 * by {@link #bind(StructType)}).
 */
public final class SqlUdfCatalogFunction implements UnboundFunction {

  private final String catalogName;
  private final Identifier ident;
  private final SqlFunctionSpec spec;

  public SqlUdfCatalogFunction(String catalogName, Identifier ident, SqlFunctionSpec spec) {
    this.catalogName = catalogName;
    this.ident = ident;
    this.spec = spec;
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    return new IcebergSqlUdfScalarFunction(
        catalogName,
        ident.namespace(),
        ident.name(),
        spec.parameters().stream()
            .map(SqlFunctionSpec.Parameter::name)
            .collect(Collectors.toList()),
        spec.parameters().stream()
            .map(SqlFunctionSpec.Parameter::icebergTypeJson)
            .collect(Collectors.toList()),
        spec.returnTypeJson(),
        spec.body(),
        spec.deterministic());
  }

  @Override
  public String name() {
    return ident.name();
  }

  @Override
  public String description() {
    return "Iceberg SQL UDF placeholder for '" + ident + "'";
  }
}
