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

package com.netflix.iceberg.expressions;

import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import java.util.List;

public class BoundReference<T> implements Reference {
  private final int fieldId;
  private final Type type;
  private final int pos;

  BoundReference(Types.StructType struct, int fieldId) {
    this.fieldId = fieldId;
    this.pos = findTopFieldPos(fieldId, struct);
    this.type = (pos > -1) ? struct.fields().get(pos).type() : findStructFieldType(fieldId, struct);
  }

  private int findTopFieldPos(int fieldId, Types.StructType struct) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      if (fields.get(i).fieldId() == fieldId) {
        return i;
      }
    }

    return -1;
  }

  private Type findStructFieldType(int fieldId, Types.StructType struct) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {

      if (fields.get(i).fieldId() == fieldId) {
        return fields.get(i).type();
      } else {

        // look under struct by calling this method recursively
        if (fields.get(i).type() instanceof Types.StructType) {
          Types.StructType subStruct = fields.get(i).type().asStructType();

          Type ret = findStructFieldType(fieldId, subStruct);
          if (ret != null) {
            return ret;
          }
        }
      }
    }
    return null;
  }

  public Type type() {
    return type;
  }

  public int fieldId() {
    return fieldId;
  }

  public int pos() {
    return pos;
  }

  public T get(StructLike struct) {
    return struct.get(pos, javaType());
  }

  @Override
  public String toString() {
    return String.format("ref(id=%d, pos=%d, type=%s)", fieldId, pos, type);
  }

  @SuppressWarnings("unchecked")
  private Class<T> javaType() {
    return (Class<T>) type.asPrimitiveType().typeId().javaClass();
  }
}
