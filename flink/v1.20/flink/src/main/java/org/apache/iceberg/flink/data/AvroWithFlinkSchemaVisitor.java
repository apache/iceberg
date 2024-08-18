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
package org.apache.iceberg.flink.data;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.avro.AvroWithPartnerByStructureVisitor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Pair;

public abstract class AvroWithFlinkSchemaVisitor<T>
    extends AvroWithPartnerByStructureVisitor<LogicalType, T> {

  @Override
  protected boolean isStringType(LogicalType logicalType) {
    return logicalType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING);
  }

  @Override
  protected boolean isMapType(LogicalType logicalType) {
    return logicalType instanceof MapType;
  }

  @Override
  protected LogicalType arrayElementType(LogicalType arrayType) {
    Preconditions.checkArgument(
        arrayType instanceof ArrayType, "Invalid array: %s is not an array", arrayType);
    return ((ArrayType) arrayType).getElementType();
  }

  @Override
  protected LogicalType mapKeyType(LogicalType mapType) {
    Preconditions.checkArgument(isMapType(mapType), "Invalid map: %s is not a map", mapType);
    return ((MapType) mapType).getKeyType();
  }

  @Override
  protected LogicalType mapValueType(LogicalType mapType) {
    Preconditions.checkArgument(isMapType(mapType), "Invalid map: %s is not a map", mapType);
    return ((MapType) mapType).getValueType();
  }

  @Override
  protected Pair<String, LogicalType> fieldNameAndType(LogicalType structType, int pos) {
    Preconditions.checkArgument(
        structType instanceof RowType, "Invalid struct: %s is not a struct", structType);
    RowType.RowField field = ((RowType) structType).getFields().get(pos);
    return Pair.of(field.getName(), field.getType());
  }

  @Override
  protected LogicalType nullType() {
    return new NullType();
  }
}
