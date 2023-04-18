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
package org.apache.iceberg.spark.data;

import org.apache.iceberg.avro.AvroWithPartnerByStructureVisitor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public abstract class AvroWithSparkSchemaVisitor<T>
    extends AvroWithPartnerByStructureVisitor<DataType, T> {

  @Override
  protected boolean isStringType(DataType dataType) {
    return dataType instanceof StringType;
  }

  @Override
  protected boolean isMapType(DataType dataType) {
    return dataType instanceof MapType;
  }

  @Override
  protected DataType arrayElementType(DataType arrayType) {
    Preconditions.checkArgument(
        arrayType instanceof ArrayType, "Invalid array: %s is not an array", arrayType);
    return ((ArrayType) arrayType).elementType();
  }

  @Override
  protected DataType mapKeyType(DataType mapType) {
    Preconditions.checkArgument(isMapType(mapType), "Invalid map: %s is not a map", mapType);
    return ((MapType) mapType).keyType();
  }

  @Override
  protected DataType mapValueType(DataType mapType) {
    Preconditions.checkArgument(isMapType(mapType), "Invalid map: %s is not a map", mapType);
    return ((MapType) mapType).valueType();
  }

  @Override
  protected Pair<String, DataType> fieldNameAndType(DataType structType, int pos) {
    Preconditions.checkArgument(
        structType instanceof StructType, "Invalid struct: %s is not a struct", structType);
    StructField field = ((StructType) structType).apply(pos);
    return Pair.of(field.name(), field.dataType());
  }

  @Override
  protected DataType nullType() {
    return DataTypes.NullType;
  }
}
