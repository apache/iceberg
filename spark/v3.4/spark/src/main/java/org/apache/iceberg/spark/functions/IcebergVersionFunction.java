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
package org.apache.iceberg.spark.functions;

import org.apache.iceberg.IcebergBuild;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A function for use in SQL that returns the current Iceberg version, e.g. {@code SELECT
 * system.iceberg_version()} will return a String such as "0.14.0" or "0.15.0-SNAPSHOT"
 */
public class IcebergVersionFunction implements UnboundFunction {
  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.fields().length > 0) {
      throw new UnsupportedOperationException(
          String.format("Cannot bind: %s does not accept arguments", name()));
    }

    return new IcebergVersionFunctionImpl();
  }

  @Override
  public String description() {
    return name() + " - Returns the runtime Iceberg version";
  }

  @Override
  public String name() {
    return "iceberg_version";
  }

  // Implementing class cannot be private, otherwise Spark is unable to access the static invoke
  // function during code-gen and calling the function fails
  static class IcebergVersionFunctionImpl implements ScalarFunction<UTF8String> {
    private static final UTF8String VERSION = UTF8String.fromString(IcebergBuild.version());

    // magic function used in code-gen. must be named `invoke`.
    public static UTF8String invoke() {
      return VERSION;
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[0];
    }

    @Override
    public DataType resultType() {
      return DataTypes.StringType;
    }

    @Override
    public boolean isResultNullable() {
      return false;
    }

    @Override
    public String canonicalName() {
      return "iceberg." + name();
    }

    @Override
    public String name() {
      return "iceberg_version";
    }

    @Override
    public UTF8String produceResult(InternalRow input) {
      return invoke();
    }
  }
}
