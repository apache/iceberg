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

import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.StructType;

/**
 * Minimal UnboundFunction placeholder for SQL UDFs registered in the Spark session.
 *
 * <p>This enables FunctionCatalog.loadFunction to succeed for user UDF names discovered via REST.
 * Binding/execution is handled by Spark SQL's native SQL UDF support; this placeholder is not
 * intended for direct binding/execution.
 */
public final class SqlUdfCatalogFunction implements UnboundFunction {

  private final String functionName;

  public SqlUdfCatalogFunction(String functionName) {
    this.functionName = functionName;
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    throw new UnsupportedOperationException(
        "SQL UDF '"
            + functionName
            + "' is registered for SQL invocation; binding is not supported");
  }

  @Override
  public String name() {
    return functionName;
  }

  @Override
  public String description() {
    return "Spark SQL UDF registered for name '" + functionName + "'";
  }
}
