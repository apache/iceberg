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

package org.apache.iceberg.flink.connector.sink;

import com.google.common.collect.Maps;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

@SuppressWarnings({"checkstyle:ParameterName", "checkstyle:MemberName"})
class RecordPartitioner extends AbstractPartitioner<Record> {

  private final Accessor<Record>[] accessors;

  RecordPartitioner(PartitionSpec spec) {
    super(spec);

    final Schema schema = spec.schema();
    final List<PartitionField> fields = spec.fields();
    final int numFields = fields.size();
    this.accessors = (Accessor<Record>[]) Array.newInstance(Accessor.class, numFields);
    final Map<Integer, Accessor<Record>> idToAccessorMap = buildAccessors(schema);
    for (int i = 0; i < numFields; i += 1) {
      PartitionField field = fields.get(i);
      Accessor<Record> accessor = idToAccessorMap.get(field.sourceId());
      if (accessor == null) {
        throw new RuntimeException(
            "Cannot build accessor for field: " + schema.findField(field.sourceId()));
      }
      this.accessors[i] = accessor;
    }
  }

  RecordPartitioner(RecordPartitioner toCopy) {
    super(toCopy);
    accessors = toCopy.accessors;
  }

  @Override
  public RecordPartitioner copy() {
    return new RecordPartitioner(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GenericRecordPartitioner[");
    sb.append(super.toString());
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void partition(Record record) {
    for (int i = 0; i < partitionTuple.length; i += 1) {
      Transform<Object, Object> transform = transforms[i];
      partitionTuple[i] = transform.apply(accessors[i].get(record));
    }
  }

  private interface Accessor<T> {
    Object get(T container);
  }

  private static Map<Integer, Accessor<Record>> buildAccessors(Schema schema) {
    return TypeUtil.visit(schema, new BuildPositionAccessors());
  }

  private static Accessor<Record> newAccessor(int p) {
    return new PositionAccessor(p);
  }

  private static Accessor<Record> newAccessor(int p, boolean isOptional,
                                              Accessor<Record> accessor) {
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
      extends TypeUtil.SchemaVisitor<Map<Integer, Accessor<Record>>> {
    @Override
    public Map<Integer, Accessor<Record>> schema(
        Schema schema, Map<Integer, Accessor<Record>> structResult) {
      return structResult;
    }

    @Override
    public Map<Integer, Accessor<Record>> struct(
        Types.StructType struct, List<Map<Integer, Accessor<Record>>> fieldResults) {
      Map<Integer, Accessor<Record>> accessors = Maps.newHashMap();
      List<Types.NestedField> fields = struct.fields();
      for (int i = 0; i < fieldResults.size(); i += 1) {
        Types.NestedField field = fields.get(i);
        Map<Integer, Accessor<Record>> result = fieldResults.get(i);
        if (result != null) {
          for (Map.Entry<Integer, Accessor<Record>> entry : result.entrySet()) {
            accessors.put(entry.getKey(), newAccessor(i, field.isOptional(), entry.getValue()));
          }
        } else {
          accessors.put(field.fieldId(), newAccessor(i));
        }
      }

      if (accessors.isEmpty()) {
        return null;
      }

      return accessors;
    }

    @Override
    public Map<Integer, Accessor<Record>> field(
        Types.NestedField field, Map<Integer, Accessor<Record>> fieldResult) {
      return fieldResult;
    }
  }

  private static class PositionAccessor implements Accessor<Record> {
    private int p;

    private PositionAccessor(int p) {
      this.p = p;
    }

    @Override
    public Object get(Record record) {
      return record.get(p);
    }
  }

  private static class Position2Accessor implements Accessor<Record> {
    private final int p0;
    private final int p1;

    private Position2Accessor(int p, PositionAccessor wrapped) {
      this.p0 = p;
      this.p1 = wrapped.p;
    }

    @Override
    public Object get(Record record) {
      Record inner = (Record) record.get(p0);
      return inner.get(p1);
    }
  }

  private static class Position3Accessor implements Accessor<Record> {
    private final int p0;
    private final int p1;
    private final int p2;

    private Position3Accessor(int p, Position2Accessor wrapped) {
      this.p0 = p;
      this.p1 = wrapped.p0;
      this.p2 = wrapped.p1;
    }

    @Override
    public Object get(Record record) {
      Record inner = (Record) record.get(p0);
      Record inner2 = (Record) inner.get(p1);
      return inner2.get(p2);
    }
  }

  private static class WrappedPositionAccessor implements Accessor<Record> {
    private final int p;
    private final Accessor<Record> accessor;

    private WrappedPositionAccessor(int p, Accessor<Record> accessor) {
      this.p = p;
      this.accessor = accessor;
    }

    @Override
    public Object get(Record record) {
      Record inner = (Record) record.get(p);
      if (inner != null) {
        return accessor.get(inner);
      }
      return null;
    }
  }
}
