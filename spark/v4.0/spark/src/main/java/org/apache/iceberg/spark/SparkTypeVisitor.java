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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;

class SparkTypeVisitor<T> {
  static <T> T visit(DataType type, SparkTypeVisitor<T> visitor) {
    if (type instanceof StructType) {
      StructField[] fields = ((StructType) type).fields();
      List<T> fieldResults = Lists.newArrayListWithExpectedSize(fields.length);

      for (StructField field : fields) {
        fieldResults.add(visitor.field(field, visit(field.dataType(), visitor)));
      }

      return visitor.struct((StructType) type, fieldResults);

    } else if (type instanceof MapType) {
      return visitor.map(
          (MapType) type,
          visit(((MapType) type).keyType(), visitor),
          visit(((MapType) type).valueType(), visitor));

    } else if (type instanceof ArrayType) {
      return visitor.array((ArrayType) type, visit(((ArrayType) type).elementType(), visitor));

    } else if (type instanceof UserDefinedType) {
      throw new UnsupportedOperationException("User-defined types are not supported");

    } else {
      return visitor.atomic(type);
    }
  }

  public T struct(StructType struct, List<T> fieldResults) {
    return null;
  }

  public T field(StructField field, T typeResult) {
    return null;
  }

  public T array(ArrayType array, T elementResult) {
    return null;
  }

  public T map(MapType map, T keyResult, T valueResult) {
    return null;
  }

  public T atomic(DataType atomic) {
    return null;
  }
}
