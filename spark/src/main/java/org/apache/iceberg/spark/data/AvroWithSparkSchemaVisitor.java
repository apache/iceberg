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

import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroWithPartnerSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Avro {@link Schema} with Spark {@link DataType} visitor. This class is for writing. The avro schema should be
 * consistent with the spark data type.
 */
public abstract class AvroWithSparkSchemaVisitor<T> extends AvroWithPartnerSchemaVisitor<DataType, T> {

  @Override
  public boolean isValidMapKey(DataType dataType) {
    return dataType instanceof StringType;
  }

  @Override
  public boolean isMapType(DataType dataType) {
    return dataType instanceof MapType;
  }

  @Override
  public DataType arrayElementType(DataType arrayType) {
    Preconditions.checkArgument(arrayType instanceof ArrayType, "Invalid array: %s is not an array", arrayType);
    return ((ArrayType) arrayType).elementType();
  }

  @Override
  public DataType mapKeyType(DataType mapType) {
    Preconditions.checkArgument(mapType instanceof MapType, "Invalid map: %s is not a map", mapType);
    return ((MapType) mapType).keyType();
  }

  @Override
  public DataType mapValueType(DataType mapType) {
    Preconditions.checkArgument(mapType instanceof MapType, "Invalid map: %s is not a map", mapType);
    return ((MapType) mapType).valueType();
  }

  @Override
  public String[] structFieldNames(DataType structType) {
    Preconditions.checkArgument(structType instanceof StructType, "Invalid struct: %s is not a struct", structType);
    return ((StructType) structType).fieldNames();
  }

  @Override
  public DataType[] structFieldTypes(DataType structType) {
    Preconditions.checkArgument(structType instanceof StructType, "Invalid struct: %s is not a struct", structType);
    return Arrays.stream(((StructType) structType).fields()).map(StructField::dataType).toArray(DataType[]::new);
  }

  @Override
  public DataType nullType() {
    return DataTypes.NullType;
  }
}
