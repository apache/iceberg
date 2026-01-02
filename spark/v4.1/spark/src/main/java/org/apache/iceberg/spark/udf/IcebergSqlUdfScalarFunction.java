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

import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataType;

/**
 * A marker Spark V2 {@link ScalarFunction} used as a stable hook for Iceberg SQL UDF rewrite.
 *
 * <p>Spark resolves catalog functions by calling {@code FunctionCatalog.loadFunction} and binding
 * the returned {@code UnboundFunction} to a {@code ScalarFunction}. Spark then creates an {@code
 * ApplyFunctionExpression} that would execute {@link #produceResult(InternalRow)}.
 *
 * <p>Iceberg's Spark extensions rewrite that expression into a native Catalyst expression using the
 * SQL body and parameter metadata carried by this object. If rewrite doesn't happen, this function
 * will fail fast.
 */
public class IcebergSqlUdfScalarFunction implements ScalarFunction<Object> {

  private final String catalogName;
  private final String[] namespace;
  private final String functionName;
  private final List<String> paramNames;
  private final List<String> paramIcebergTypeJson;
  private final String returnIcebergTypeJson;
  private final String body;
  private final boolean deterministic;

  public IcebergSqlUdfScalarFunction(
      String catalogName,
      String[] namespace,
      String functionName,
      List<String> paramNames,
      List<String> paramIcebergTypeJson,
      String returnIcebergTypeJson,
      String body,
      boolean deterministic) {
    this.catalogName = catalogName;
    this.namespace = namespace;
    this.functionName = functionName;
    this.paramNames = paramNames;
    this.paramIcebergTypeJson = paramIcebergTypeJson;
    this.returnIcebergTypeJson = returnIcebergTypeJson;
    this.body = body;
    this.deterministic = deterministic;
  }

  public String catalogName() {
    return catalogName;
  }

  public String[] namespace() {
    return namespace;
  }

  @Override
  public String name() {
    return functionName;
  }

  public List<String> paramNames() {
    return paramNames;
  }

  public List<String> paramIcebergTypeJson() {
    return paramIcebergTypeJson;
  }

  public String returnIcebergTypeJson() {
    return returnIcebergTypeJson;
  }

  public String body() {
    return body;
  }

  @Override
  public DataType[] inputTypes() {
    DataType[] types = new DataType[paramIcebergTypeJson.size()];
    for (int i = 0; i < paramIcebergTypeJson.size(); i++) {
      types[i] = SqlFunctionSpecParser.icebergTypeToSparkType(paramIcebergTypeJson.get(i));
    }

    return types;
  }

  @Override
  public DataType resultType() {
    return SqlFunctionSpecParser.icebergTypeToSparkType(returnIcebergTypeJson);
  }

  @Override
  public boolean isResultNullable() {
    return true;
  }

  @Override
  public boolean isDeterministic() {
    return deterministic;
  }

  @Override
  public Object produceResult(InternalRow input) {
    throw new UnsupportedOperationException(
        "Iceberg SQL UDF '" + functionName + "' was not rewritten; this is a bug");
  }
}
