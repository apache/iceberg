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

package org.apache.iceberg.hive.legacy;

import java.util.List;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;


public class HiveTypeToIcebergType extends HiveTypeUtil.HiveSchemaVisitor<Type> {
  private int nextId = 1;

  @Override
  public Type struct(StructTypeInfo struct, List<String> names, List<Type> fieldResults) {
    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(fieldResults.size());
    for (int i = 0; i < names.size(); i++) {
      fields.add(Types.NestedField.optional(allocateId(), names.get(i), fieldResults.get(i)));
    }
    return Types.StructType.of(fields);
  }

  @Override
  public Type map(MapTypeInfo map, Type keyResult, Type valueResult) {
    return Types.MapType.ofOptional(allocateId(), allocateId(), keyResult, valueResult);
  }

  @Override
  public Type list(ListTypeInfo list, Type elementResult) {
    return Types.ListType.ofOptional(allocateId(), elementResult);
  }

  @Override
  public Type primitive(PrimitiveTypeInfo primitive) {
    switch (primitive.getPrimitiveCategory()) {
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case BOOLEAN:
        return Types.BooleanType.get();
      case BYTE:
      case SHORT:
      case INT:
        return Types.IntegerType.get();
      case LONG:
        return Types.LongType.get();
      case CHAR:
      case VARCHAR:
      case STRING:
        return Types.StringType.get();
      case BINARY:
        return Types.BinaryType.get();
      case DECIMAL:
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitive;
        return Types.DecimalType.of(decimalTypeInfo.precision(), decimalTypeInfo.scale());
      case TIMESTAMP:
        return Types.TimestampType.withoutZone();
      case DATE:
        return Types.DateType.get();
      default:
        throw new UnsupportedOperationException("Unsupported primitive type " + primitive);
    }
  }

  private int allocateId() {
    int current = nextId;
    nextId += 1;
    return current;
  }
}
