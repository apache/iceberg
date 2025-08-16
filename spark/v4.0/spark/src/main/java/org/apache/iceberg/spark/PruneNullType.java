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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class PruneNullType extends SparkTypeVisitor<DataType> {

  public static StructType prune(StructType sparkType) {
    DataType type = SparkTypeVisitor.visit(sparkType, new PruneNullType());

    if (type instanceof NullType) {
      return new StructType();
    } else {
      return (StructType) type;
    }
  }

  private boolean needsRewrite(StructField field, DataType type) {
    return field.dataType() instanceof NullType
        || type instanceof NullType
        || !field.dataType().equals(type);
  }

  @Override
  public DataType struct(StructType struct, List<DataType> fieldResults) {
    StructField[] fields = struct.fields();
    List<StructField> newFields = Lists.newArrayListWithExpectedSize(struct.length());

    int pos = 0;
    boolean rewritten = false;
    for (StructField field : fields) {
      DataType fieldResult = fieldResults.get(pos);

      if (needsRewrite(field, fieldResult)) {
        if (!(fieldResult instanceof NullType) && !(field.dataType() instanceof NullType)) {
          newFields.add(
              StructField.apply(field.name(), fieldResult, field.nullable(), field.metadata()));
        }
      } else {
        newFields.add(field);
      }
      rewritten = true;

      pos += 1;
    }

    if (rewritten) {
      if (newFields.isEmpty()) {
        return DataTypes.NullType;
      } else {
        return new StructType(newFields.toArray(new StructField[0]));
      }
    } else {
      return struct;
    }
  }

  @Override
  public DataType field(StructField field, DataType typeResult) {
    return typeResult;
  }

  @Override
  public DataType array(ArrayType array, DataType elementResult) {
    if (elementResult instanceof NullType) {
      return elementResult;
    } else if (!array.elementType().equals(elementResult)) {
      return new ArrayType(elementResult, array.containsNull());
    } else {
      return array;
    }
  }

  @Override
  public DataType map(MapType map, DataType keyResult, DataType valueResult) {
    Preconditions.checkArgument(
        !(valueResult instanceof NullType), "Cannot create a map with a with a NullType value");

    if (!map.valueType().equals(valueResult)) {
      return new MapType(keyResult, valueResult, map.valueContainsNull());
    } else {
      return map;
    }
  }

  @Override
  public DataType atomic(DataType atomic) {
    return atomic;
  }
}
