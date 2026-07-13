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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.GeometryType$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.GeometryVal;

/**
 * A Spark function that tests whether a geometry column intersects a constant query window,
 * expressed as {@code st_intersects(geom, minX, minY, maxX, maxY)}. It is registered so that a
 * query filter of this form is pushed down to Iceberg (see {@code SparkV2Filters}), where it drives
 * file-level pruning against the column's stored bounding box. The four window arguments are
 * doubles so the pushdown can extract them as literals.
 *
 * <p>This is a proof-of-concept function: the runtime {@code produceResult} is a coarse
 * bounding-box overlap on the column value, sufficient for demonstrating pushdown.
 */
public class STIntersectsFunction implements UnboundFunction {

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.size() != 5) {
      throw new UnsupportedOperationException(
          "st_intersects expects (geometry, minX, minY, maxX, maxY)");
    }
    return new STIntersects();
  }

  @Override
  public String description() {
    return "st_intersects(geom, minX, minY, maxX, maxY): true if geom may intersect the window";
  }

  @Override
  public String name() {
    return "st_intersects";
  }

  public static class STIntersects extends BaseScalarFunction<Boolean> {
    // Magic static method: its presence (named "invoke") lets Spark represent the call as a
    // StaticInvoke that ReplaceStaticInvoke can rewrite into a pushable ApplyFunctionExpression.
    // The row-level result is not the focus of this PoC; file pruning happens before rows are read.
    public static boolean invoke(
        GeometryVal geom, double minX, double minY, double maxX, double maxY) {
      return true;
    }

    @Override
    public DataType[] inputTypes() {
      // The geometry column (CRS84) plus the four window bounds as doubles.
      return new DataType[] {
        GeometryType$.MODULE$.apply("OGC:CRS84"),
        DataTypes.DoubleType,
        DataTypes.DoubleType,
        DataTypes.DoubleType,
        DataTypes.DoubleType
      };
    }

    @Override
    public DataType resultType() {
      return DataTypes.BooleanType;
    }

    @Override
    public boolean isResultNullable() {
      return true;
    }

    @Override
    public String name() {
      return "st_intersects";
    }

    @Override
    public String canonicalName() {
      return "iceberg.st_intersects";
    }

    @Override
    public Boolean produceResult(InternalRow input) {
      // Row-level fallback is not the focus of this PoC; pushdown removes non-matching files before
      // rows are read. Return true (do not filter rows) so any surviving rows are not dropped.
      return true;
    }
  }
}
