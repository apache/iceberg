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

package org.apache.iceberg.expressions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import com.google.common.collect.Maps;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class BoundReference<T> implements Reference {
  private final int fieldId;
  private final Accessor<StructLike> accessor;
  private final int pos;

  BoundReference(Schema schema, int fieldId) {
    this.fieldId = fieldId;

    Map<Integer, Accessor<StructLike>> accessors = lazyIdToAccessor(schema);

    this.accessor = accessors.get(fieldId);

    // only look for top level field position
    this.pos = findTopFieldPos(fieldId, schema.asStruct());

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

  public Type type() {
    return accessor.type();
  }

  public int fieldId() {
    return fieldId;
  }

  public int pos() {
    if (pos == -1) {
      throw new ValidationException("Cannot find position for non-primitive field id %d", fieldId);
    }
    return pos;
  }

  public T get(StructLike struct) {
    return (T) accessor.get(struct);
  }

  @Override
  public String toString() {
    return String.format("ref(id=%d, accessor=%s)", fieldId, accessor);
  }

  private interface Accessor<T> extends Serializable {
    Object get(T container);

    Type type();
  }

  private static class PositionAccessor implements Accessor<StructLike> {
    private int p;
    private final Type type;
    private final Class<?> javaClass;

    private PositionAccessor(int p, Type type) {
      this.p = p;
      this.type = type;
      this.javaClass = type.typeId().javaClass();
    }

    @Override
    public Object get(StructLike row) {
      return row.get(p, javaClass);
    }

    @Override
    public Type type() {
      return type;
    }
  }

  private static class Position2Accessor implements Accessor<StructLike> {
    private final int p0;
    private final int p1;
    private final Type type;
    private final Class<?> javaClass;

    private Position2Accessor(int p, PositionAccessor wrapped) {
      this.p0 = p;
      this.p1 = wrapped.p;
      this.type = wrapped.type;
      this.javaClass = wrapped.javaClass;
    }

    @Override
    public Object get(StructLike row) {
      return row.get(p0, StructLike.class).get(p1, javaClass);
    }

    @Override
    public Type type() {
      return type;
    }
  }

  private static class Position3Accessor implements Accessor<StructLike> {
    private final int p0;
    private final int p1;
    private final int p2;
    private final Type type;
    private final Class<?> javaClass;

    private Position3Accessor(int p, Position2Accessor wrapped) {
      this.p0 = p;
      this.p1 = wrapped.p0;
      this.p2 = wrapped.p1;
      this.type = wrapped.type;
      this.javaClass = wrapped.javaClass;
    }

    @Override
    public Object get(StructLike row) {
      return row.get(p0, StructLike.class).get(p1, StructLike.class).get(p2, javaClass);
    }

    @Override
    public Type type() {
      return type;
    }
  }

  private static class WrappedPositionAccessor implements Accessor<StructLike> {
    private final int p;
    private final Accessor<StructLike> accessor;

    private WrappedPositionAccessor(int p, Accessor<StructLike> accessor) {
      this.p = p;
      this.accessor = accessor;
    }

    @Override
    public Object get(StructLike row) {
      StructLike inner = row.get(p, StructLike.class);
      if (inner != null) {
        return accessor.get(inner);
      }
      return null;
    }

    @Override
    public Type type() {
      return accessor.type();
    }
  }

  private transient Map<Integer, Accessor<StructLike>> idToAccessor = null;

  private Map<Integer, Accessor<StructLike>> lazyIdToAccessor(Schema schema) {

    if (idToAccessor == null) {
      idToAccessor = TypeUtil.visit(schema, new BuildPositionAccessors());
    }
    return idToAccessor;
  }

  private static Accessor<StructLike> newAccessor(int p, Type type) {
    return new PositionAccessor(p, type);
  }

  private static Accessor<StructLike> newAccessor(int p, boolean isOptional,
                                                  Accessor<StructLike> accessor) {
    if (isOptional) {
      // the wrapped position handles null layers
      return new WrappedPositionAccessor(p, accessor);
    } else if (accessor instanceof PositionAccessor) {
      return new Position2Accessor(p, (PositionAccessor) accessor);
    } else if (accessor instanceof Position2Accessor) {
      return new Position3Accessor(p, (Position2Accessor) accessor);
    } else {
      return new WrappedPositionAccessor(p, accessor);
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
  }

}
