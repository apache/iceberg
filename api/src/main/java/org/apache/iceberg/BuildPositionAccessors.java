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

package org.apache.iceberg;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

class BuildPositionAccessors
    extends TypeUtil.SchemaVisitor<Map<Integer, Accessor<StructLike>>> {

  @Override
  public Map<Integer, Accessor<StructLike>> schema(
      Schema schema, Map<Integer, Accessor<StructLike>> structResult) {
    return structResult;
  }

  @Override
  public Map<Integer, Accessor<StructLike>> struct(
      Types.StructType struct, List<Map<Integer, Accessor<StructLike>>> fieldResults) {
    Map<Integer, Accessor<StructLike>> accessors = Maps.newHashMap();
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fieldResults.size(); i += 1) {
      Types.NestedField field = fields.get(i);
      Map<Integer, Accessor<StructLike>> result = fieldResults.get(i);
      if (result != null) {
        for (Map.Entry<Integer, Accessor<StructLike>> entry : result.entrySet()) {
          accessors.put(entry.getKey(), newAccessor(i, field.isOptional(), entry.getValue()));
        }
      } else {
        accessors.put(field.fieldId(), newAccessor(i, field.type()));
      }
    }

    if (accessors.isEmpty()) {
      return null;
    }

    return accessors;
  }

  @Override
  public Map<Integer, Accessor<StructLike>> field(
      Types.NestedField field, Map<Integer, Accessor<StructLike>> fieldResult) {
    return fieldResult;
  }

  private static Accessor<StructLike> newAccessor(int pos, Type type) {
    return new PositionAccessor(pos, type);
  }

  private static Accessor<StructLike> newAccessor(int pos, boolean isOptional,
      Accessor<StructLike> accessor) {
    if (isOptional) {
      // the wrapped position handles null layers
      return new WrappedPositionAccessor(pos, accessor);
    } else if (accessor instanceof PositionAccessor) {
      return new Position2Accessor(pos, (PositionAccessor) accessor);
    } else if (accessor instanceof Position2Accessor) {
      return new Position3Accessor(pos, (Position2Accessor) accessor);
    } else {
      return new WrappedPositionAccessor(pos, accessor);
    }
  }
}
