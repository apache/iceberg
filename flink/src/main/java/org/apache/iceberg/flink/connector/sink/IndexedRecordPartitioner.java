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
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

@SuppressWarnings({"checkstyle:ParameterName", "checkstyle:MemberName"})
class IndexedRecordPartitioner extends AbstractPartitioner<IndexedRecord> {

  private final Accessor<IndexedRecord>[] accessors;

  IndexedRecordPartitioner(PartitionSpec spec) {
    super(spec);

    final Schema schema = spec.schema();
    final List<PartitionField> fields = spec.fields();
    final int numFields = fields.size();
    this.accessors = (Accessor<IndexedRecord>[]) Array.newInstance(Accessor.class, numFields);
    final Map<Integer, Accessor<IndexedRecord>> idToAccessorMap = buildAccessors(schema);
    for (int i = 0; i < numFields; i += 1) {
      PartitionField field = fields.get(i);
      Accessor<IndexedRecord> accessor = idToAccessorMap.get(field.sourceId());
      if (accessor == null) {
        throw new RuntimeException(
            "Cannot build accessor for field: " + schema.findField(field.sourceId()));
      }
      this.accessors[i] = accessor;
    }
  }

  IndexedRecordPartitioner(IndexedRecordPartitioner toCopy) {
    super(toCopy);
    accessors = toCopy.accessors;
  }

  @Override
  public IndexedRecordPartitioner copy() {
    return new IndexedRecordPartitioner(this);
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
  public void partition(IndexedRecord record) {
    for (int i = 0; i < partitionTuple.length; i += 1) {
      Transform<Object, Object> transform = transforms[i];
      partitionTuple[i] = transform.apply(accessors[i].get(record));
    }
  }

  private interface Accessor<T> {
    Object get(T container);
  }

  private static Map<Integer, Accessor<IndexedRecord>> buildAccessors(Schema schema) {
    return TypeUtil.visit(schema, new BuildPositionAccessors());
  }

  private static Accessor<IndexedRecord> newAccessor(int p) {
    return new PositionAccessor(p);
  }

  private static Accessor<IndexedRecord> newAccessor(int p, boolean isOptional,
                                                     Accessor<IndexedRecord> accessor) {
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
      extends TypeUtil.SchemaVisitor<Map<Integer, Accessor<IndexedRecord>>> {
    @Override
    public Map<Integer, Accessor<IndexedRecord>> schema(
        Schema schema, Map<Integer, Accessor<IndexedRecord>> structResult) {
      return structResult;
    }

    @Override
    public Map<Integer, Accessor<IndexedRecord>> struct(
        Types.StructType struct, List<Map<Integer, Accessor<IndexedRecord>>> fieldResults) {
      Map<Integer, Accessor<IndexedRecord>> accessors = Maps.newHashMap();
      List<Types.NestedField> fields = struct.fields();
      for (int i = 0; i < fieldResults.size(); i += 1) {
        Types.NestedField field = fields.get(i);
        Map<Integer, Accessor<IndexedRecord>> result = fieldResults.get(i);
        if (result != null) {
          for (Map.Entry<Integer, Accessor<IndexedRecord>> entry : result.entrySet()) {
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
    public Map<Integer, Accessor<IndexedRecord>> field(
        Types.NestedField field, Map<Integer, Accessor<IndexedRecord>> fieldResult) {
      return fieldResult;
    }
  }

  private static class PositionAccessor implements Accessor<IndexedRecord> {
    private int p;

    private PositionAccessor(int p) {
      this.p = p;
    }

    @Override
    public Object get(IndexedRecord record) {
      return record.get(p);
    }
  }

  private static class Position2Accessor implements Accessor<IndexedRecord> {
    private final int p0;
    private final int p1;

    private Position2Accessor(int p, PositionAccessor wrapped) {
      this.p0 = p;
      this.p1 = wrapped.p;
    }

    @Override
    public Object get(IndexedRecord record) {
      IndexedRecord inner = (IndexedRecord) record.get(p0);
      return inner.get(p1);
    }
  }

  private static class Position3Accessor implements Accessor<IndexedRecord> {
    private final int p0;
    private final int p1;
    private final int p2;

    private Position3Accessor(int p, Position2Accessor wrapped) {
      this.p0 = p;
      this.p1 = wrapped.p0;
      this.p2 = wrapped.p1;
    }

    @Override
    public Object get(IndexedRecord record) {
      IndexedRecord inner = (IndexedRecord) record.get(p0);
      IndexedRecord inner2 = (IndexedRecord) inner.get(p1);
      return inner2.get(p2);
    }
  }

  private static class WrappedPositionAccessor implements Accessor<IndexedRecord> {
    private final int p;
    private final Accessor<IndexedRecord> accessor;

    private WrappedPositionAccessor(int p, Accessor<IndexedRecord> accessor) {
      this.p = p;
      this.accessor = accessor;
    }

    @Override
    public Object get(IndexedRecord record) {
      IndexedRecord inner = (IndexedRecord) record.get(p);
      if (inner != null) {
        return accessor.get(inner);
      }
      return null;
    }
  }
}
