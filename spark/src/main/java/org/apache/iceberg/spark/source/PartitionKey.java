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

package org.apache.iceberg.spark.source;

import com.google.common.collect.Maps;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

class PartitionKey implements StructLike {

  private final PartitionSpec spec;
  private final int size;
  private final Object[] partitionTuple;
  private final Transform[] transforms;
  private final Accessor<InternalRow>[] accessors;

  @SuppressWarnings("unchecked")
  PartitionKey(PartitionSpec spec) {
    this.spec = spec;

    List<PartitionField> fields = spec.fields();
    this.size = fields.size();
    this.partitionTuple = new Object[size];
    this.transforms = new Transform[size];
    this.accessors = (Accessor<InternalRow>[]) Array.newInstance(Accessor.class, size);

    Schema schema = spec.schema();
    Map<Integer, Accessor<InternalRow>> newAccessors = buildAccessors(schema);
    for (int i = 0; i < size; i += 1) {
      PartitionField field = fields.get(i);
      Accessor<InternalRow> accessor = newAccessors.get(field.sourceId());
      if (accessor == null) {
        throw new RuntimeException(
            "Cannot build accessor for field: " + schema.findField(field.sourceId()));
      }
      this.accessors[i] = accessor;
      this.transforms[i] = field.transform();
    }
  }

  private PartitionKey(PartitionKey toCopy) {
    this.spec = toCopy.spec;
    this.size = toCopy.size;
    this.partitionTuple = new Object[toCopy.partitionTuple.length];
    this.transforms = toCopy.transforms;
    this.accessors = toCopy.accessors;

    for (int i = 0; i < partitionTuple.length; i += 1) {
      this.partitionTuple[i] = defensiveCopyIfNeeded(toCopy.partitionTuple[i]);
    }
  }

  private Object defensiveCopyIfNeeded(Object obj) {
    if (obj instanceof UTF8String) {
      // bytes backing the UTF8 string might be reused
      byte[] bytes = ((UTF8String) obj).getBytes();
      return UTF8String.fromBytes(Arrays.copyOf(bytes, bytes.length));
    }
    return obj;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < partitionTuple.length; i += 1) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(partitionTuple[i]);
    }
    sb.append("]");
    return sb.toString();
  }

  PartitionKey copy() {
    return new PartitionKey(this);
  }

  String toPath() {
    return spec.partitionToPath(this);
  }

  @SuppressWarnings("unchecked")
  void partition(InternalRow row) {
    for (int i = 0; i < partitionTuple.length; i += 1) {
      Transform<Object, Object> transform = transforms[i];
      partitionTuple[i] = transform.apply(accessors[i].get(row));
    }
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(partitionTuple[pos]);
  }

  @Override
  public <T> void set(int pos, T value) {
    partitionTuple[pos] = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionKey that = (PartitionKey) o;
    return Arrays.equals(partitionTuple, that.partitionTuple);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(partitionTuple);
  }

  private interface Accessor<T> {
    Object get(T container);
  }

  private static Map<Integer, Accessor<InternalRow>> buildAccessors(Schema schema) {
    return TypeUtil.visit(schema, new BuildPositionAccessors());
  }

  private static Accessor<InternalRow> newAccessor(int position, Type type) {
    switch (type.typeId()) {
      case STRING:
        return new StringAccessor(position, SparkSchemaUtil.convert(type));
      case DECIMAL:
        return new DecimalAccessor(position, SparkSchemaUtil.convert(type));
      case BINARY:
        return new BytesAccessor(position, SparkSchemaUtil.convert(type));
      default:
        return new PositionAccessor(position, SparkSchemaUtil.convert(type));
    }
  }

  private static Accessor<InternalRow> newAccessor(int position, boolean isOptional, Types.StructType type,
                                                   Accessor<InternalRow> accessor) {
    int size = type.fields().size();
    if (isOptional) {
      // the wrapped position handles null layers
      return new WrappedPositionAccessor(position, size, accessor);
    } else if (accessor instanceof PositionAccessor) {
      return new Position2Accessor(position, size, (PositionAccessor) accessor);
    } else if (accessor instanceof Position2Accessor) {
      return new Position3Accessor(position, size, (Position2Accessor) accessor);
    } else {
      return new WrappedPositionAccessor(position, size, accessor);
    }
  }

  private static class BuildPositionAccessors
      extends TypeUtil.SchemaVisitor<Map<Integer, Accessor<InternalRow>>> {
    @Override
    public Map<Integer, Accessor<InternalRow>> schema(
        Schema schema, Map<Integer, Accessor<InternalRow>> structResult) {
      return structResult;
    }

    @Override
    public Map<Integer, Accessor<InternalRow>> struct(
        Types.StructType struct, List<Map<Integer, Accessor<InternalRow>>> fieldResults) {
      Map<Integer, Accessor<InternalRow>> accessors = Maps.newHashMap();
      List<Types.NestedField> fields = struct.fields();
      for (int i = 0; i < fieldResults.size(); i += 1) {
        Types.NestedField field = fields.get(i);
        Map<Integer, Accessor<InternalRow>> result = fieldResults.get(i);
        if (result != null) {
          for (Map.Entry<Integer, Accessor<InternalRow>> entry : result.entrySet()) {
            accessors.put(entry.getKey(), newAccessor(i, field.isOptional(),
                field.type().asNestedType().asStructType(), entry.getValue()));
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
    public Map<Integer, Accessor<InternalRow>> field(
        Types.NestedField field, Map<Integer, Accessor<InternalRow>> fieldResult) {
      return fieldResult;
    }
  }

  private static class PositionAccessor implements Accessor<InternalRow> {
    private final DataType type;
    private int position;

    private PositionAccessor(int position, DataType type) {
      this.position = position;
      this.type = type;
    }

    @Override
    public Object get(InternalRow row) {
      if (row.isNullAt(position)) {
        return null;
      }
      return row.get(position, type);
    }

    DataType type() {
      return type;
    }

    int position() {
      return position;
    }
  }

  private static class StringAccessor extends PositionAccessor {
    private StringAccessor(int position, DataType type) {
      super(position, type);
    }

    @Override
    public Object get(InternalRow row) {
      if (row.isNullAt(position())) {
        return null;
      }
      return row.get(position(), type()).toString();
    }
  }

  private static class DecimalAccessor extends PositionAccessor {
    private DecimalAccessor(int position, DataType type) {
      super(position, type);
    }

    @Override
    public Object get(InternalRow row) {
      if (row.isNullAt(position())) {
        return null;
      }
      return ((Decimal) row.get(position(), type())).toJavaBigDecimal();
    }
  }

  private static class BytesAccessor extends PositionAccessor {
    private BytesAccessor(int position, DataType type) {
      super(position, type);
    }

    @Override
    public Object get(InternalRow row) {
      if (row.isNullAt(position())) {
        return null;
      }
      return ByteBuffer.wrap((byte[]) row.get(position(), type()));
    }
  }

  private static class Position2Accessor implements Accessor<InternalRow> {
    private final int p0;
    private final int size0;
    private final int p1;
    private final DataType type;

    private Position2Accessor(int position, int size, PositionAccessor wrapped) {
      this.p0 = position;
      this.size0 = size;
      this.p1 = wrapped.position;
      this.type = wrapped.type;
    }

    @Override
    public Object get(InternalRow row) {
      return row.getStruct(p0, size0).get(p1, type);
    }
  }

  private static class Position3Accessor implements Accessor<InternalRow> {
    private final int p0;
    private final int size0;
    private final int p1;
    private final int size1;
    private final int p2;
    private final DataType type;

    private Position3Accessor(int position, int size, Position2Accessor wrapped) {
      this.p0 = position;
      this.size0 = size;
      this.p1 = wrapped.p0;
      this.size1 = wrapped.size0;
      this.p2 = wrapped.p1;
      this.type = wrapped.type;
    }

    @Override
    public Object get(InternalRow row) {
      return row.getStruct(p0, size0).getStruct(p1, size1).get(p2, type);
    }
  }

  private static class WrappedPositionAccessor implements Accessor<InternalRow> {
    private final int position;
    private final int size;
    private final Accessor<InternalRow> accessor;

    private WrappedPositionAccessor(int position, int size, Accessor<InternalRow> accessor) {
      this.position = position;
      this.size = size;
      this.accessor = accessor;
    }

    @Override
    public Object get(InternalRow row) {
      InternalRow inner = row.getStruct(position, size);
      if (inner != null) {
        return accessor.get(inner);
      }
      return null;
    }
  }
}
