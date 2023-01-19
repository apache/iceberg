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
package org.apache.iceberg.hudi;

import java.util.List;
import org.apache.hudi.internal.schema.Types;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;

public class HudiDataTypeToType extends HudiDataTypeVisitor<Type> {
  private final Types.RecordType root;
  private int nextId = 0;

  HudiDataTypeToType() {
    this.root = null;
  }

  HudiDataTypeToType(Types.RecordType root) {
    this.root = root;
    this.nextId = root.fields().size();
  }

  private int getNextId() {
    int next = nextId;
    nextId += 1;
    return next;
  }

  @SuppressWarnings("ReferenceEquality")
  @Override
  public Type record(Types.RecordType record, List<Type> fieldResults) {
    List<Types.Field> fields = record.fields();
    List<org.apache.iceberg.types.Types.NestedField> newFields =
        Lists.newArrayListWithExpectedSize(fields.size());
    boolean isRoot = root == record;
    for (int i = 0; i < fields.size(); i += 1) {
      Types.Field field = fields.get(i);
      Type type = fieldResults.get(i);
      int id;
      if (isRoot) {
        id = i;
      } else {
        id = getNextId();
      }

      String doc = field.doc();
      if (field.isOptional()) {
        newFields.add(
            org.apache.iceberg.types.Types.NestedField.optional(id, field.name(), type, doc));
      } else {
        newFields.add(
            org.apache.iceberg.types.Types.NestedField.required(id, field.name(), type, doc));
      }
    }

    return org.apache.iceberg.types.Types.StructType.of(newFields);
  }

  @Override
  public Type field(Types.Field field, Type typeResult) {
    return typeResult;
  }

  @Override
  public Type map(Types.MapType map, Type keyResult, Type valueResult) {
    if (map.isValueOptional()) {
      return org.apache.iceberg.types.Types.MapType.ofOptional(
          getNextId(), getNextId(), keyResult, valueResult);
    } else {
      return org.apache.iceberg.types.Types.MapType.ofRequired(
          getNextId(), getNextId(), keyResult, valueResult);
    }
  }

  @Override
  public Type array(Types.ArrayType array, Type elementResult) {
    if (array.isElementOptional()) {
      return org.apache.iceberg.types.Types.ListType.ofOptional(getNextId(), elementResult);
    } else {
      return org.apache.iceberg.types.Types.ListType.ofRequired(getNextId(), elementResult);
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public Type atomic(org.apache.hudi.internal.schema.Type atomic) {
    if (atomic instanceof Types.BooleanType) {
      return org.apache.iceberg.types.Types.BooleanType.get();
    } else if (atomic instanceof Types.IntType) {
      return org.apache.iceberg.types.Types.IntegerType.get();
    } else if (atomic instanceof Types.LongType) {
      return org.apache.iceberg.types.Types.LongType.get();
    } else if (atomic instanceof Types.FloatType) {
      return org.apache.iceberg.types.Types.FloatType.get();
    } else if (atomic instanceof Types.DoubleType) {
      return org.apache.iceberg.types.Types.DoubleType.get();
    } else if (atomic instanceof Types.DateType) {
      return org.apache.iceberg.types.Types.DateType.get();
    } else if (atomic instanceof Types.TimestampType) {
      return org.apache.iceberg.types.Types.TimestampType.withZone();
    } else if (atomic instanceof Types.StringType) {
      return org.apache.iceberg.types.Types.StringType.get();
    } else if (atomic instanceof Types.BinaryType) {
      return org.apache.iceberg.types.Types.BinaryType.get();
    } else if (atomic instanceof Types.UUIDType) {
      return org.apache.iceberg.types.Types.UUIDType.get();
    } else if (atomic instanceof Types.DecimalType) {
      return org.apache.iceberg.types.Types.DecimalType.of(
          ((Types.DecimalType) atomic).precision(), ((Types.DecimalType) atomic).scale());
    } else if (atomic instanceof Types.FixedType) {
      return org.apache.iceberg.types.Types.FixedType.ofLength(
          ((Types.FixedType) atomic).getFixedSize());
    } else if (atomic instanceof Types.TimeType) {
      return org.apache.iceberg.types.Types.TimeType.get();
    }

    throw new ValidationException("Not a supported type: %s", atomic.getClass().getName());
  }
}
