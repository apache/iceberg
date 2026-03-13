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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Position2Accessor and Position3Accessor here is an optimization. For a nested schema like:
 *
 * <pre>
 * root
 *  |-- a: struct (nullable = false)
 *  |    |-- b: struct (nullable = false)
 *  |        | -- c: string (containsNull = false)
 * </pre>
 *
 * Then we will use Position3Accessor to access nested field 'c'. It can be accessed like this:
 * {@code row.get(p0, StructLike.class).get(p1, StructLike.class).get(p2, javaClass)}. Commonly,
 * Nested fields with depth=1 or 2 or 3 are the fields that will be accessed frequently, so this
 * optimization will help to access this kind of schema. For schema whose depth is deeper than 3,
 * then we will use the {@link WrappedPositionAccessor} to access recursively.
 */
public class Accessors {
  private Accessors() {}

  public static Integer toPosition(Accessor<StructLike> accessor) {
    if (accessor instanceof PositionAccessor) {
      return ((PositionAccessor) accessor).position();
    }
    throw new IllegalArgumentException("Cannot convert nested accessor to position");
  }

  static Map<Integer, Accessor<StructLike>> forSchema(Schema schema) {
    return TypeUtil.visit(schema, new BuildPositionAccessors());
  }

  private static class PositionAccessor implements Accessor<StructLike> {
    private final int position;
    private final Type type;
    private final Class<?> javaClass;

    PositionAccessor(int pos, Type type) {
      this.position = pos;
      this.type = type;
      this.javaClass = type.typeId().javaClass();
    }

    @Override
    public Object get(StructLike row) {
      return row.get(position, javaClass);
    }

    @Override
    public Type type() {
      return type;
    }

    public int position() {
      return position;
    }

    public Class<?> javaClass() {
      return javaClass;
    }

    @Override
    public String toString() {
      return "Accessor(positions=[" + position + "], type=" + type + ")";
    }
  }

  private static class Position2Accessor implements Accessor<StructLike> {
    private final int p0;
    private final int p1;
    private final Type type;
    private final Class<?> javaClass;

    Position2Accessor(int pos, PositionAccessor wrapped) {
      this.p0 = pos;
      this.p1 = wrapped.position();
      this.type = wrapped.type();
      this.javaClass = wrapped.javaClass();
    }

    @Override
    public Object get(StructLike row) {
      return row.get(p0, StructLike.class).get(p1, javaClass);
    }

    @Override
    public Type type() {
      return type;
    }

    public Class<?> javaClass() {
      return javaClass;
    }

    @Override
    public String toString() {
      return "Accessor(positions=[" + p0 + ", " + p1 + "], type=" + type + ")";
    }
  }

  private static class Position3Accessor implements Accessor<StructLike> {
    private final int p0;
    private final int p1;
    private final int p2;
    private final Type type;
    private final Class<?> javaClass;

    Position3Accessor(int pos, Position2Accessor wrapped) {
      this.p0 = pos;
      this.p1 = wrapped.p0;
      this.p2 = wrapped.p1;
      this.type = wrapped.type();
      this.javaClass = wrapped.javaClass();
    }

    @Override
    public Object get(StructLike row) {
      return row.get(p0, StructLike.class).get(p1, StructLike.class).get(p2, javaClass);
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public String toString() {
      return "Accessor(positions=[" + p0 + ", " + p1 + ", " + p2 + "], type=" + type + ")";
    }
  }

  private static class WrappedPositionAccessor implements Accessor<StructLike> {
    private final int position;
    private final Accessor<StructLike> accessor;

    WrappedPositionAccessor(int pos, Accessor<StructLike> accessor) {
      this.position = pos;
      this.accessor = accessor;
    }

    @Override
    public Object get(StructLike row) {
      StructLike inner = row.get(position, StructLike.class);
      if (inner != null) {
        return accessor.get(inner);
      }
      return null;
    }

    @Override
    public Type type() {
      return accessor.type();
    }

    @Override
    public String toString() {
      return "WrappedAccessor(position=" + position + ", wrapped=" + accessor + ")";
    }
  }

  private static Accessor<StructLike> newAccessor(int pos, Type type) {
    return new PositionAccessor(pos, type);
  }

  private static Accessor<StructLike> newAccessor(
      int pos, boolean isOptional, Accessor<StructLike> accessor) {
    if (isOptional) {
      // the wrapped position handles null layers
      return new WrappedPositionAccessor(pos, accessor);
    } else if (accessor.getClass() == PositionAccessor.class) {
      return new Position2Accessor(pos, (PositionAccessor) accessor);
    } else if (accessor instanceof Position2Accessor) {
      return new Position3Accessor(pos, (Position2Accessor) accessor);
    } else {
      return new WrappedPositionAccessor(pos, accessor);
    }
  }

  private static class BuildPositionAccessors
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
          // Add accessors for nested fields.
          for (Map.Entry<Integer, Accessor<StructLike>> entry : result.entrySet()) {
            accessors.put(entry.getKey(), newAccessor(i, field.isOptional(), entry.getValue()));
          }
        }

        // Add an accessor for this field as an Object (may or may not be primitive).
        accessors.put(field.fieldId(), newAccessor(i, field.type()));
      }

      return accessors;
    }

    @Override
    public Map<Integer, Accessor<StructLike>> variant(Types.VariantType variant) {
      return null;
    }

    @Override
    public Map<Integer, Accessor<StructLike>> field(
        Types.NestedField field, Map<Integer, Accessor<StructLike>> fieldResult) {
      return fieldResult;
    }
  }
}
