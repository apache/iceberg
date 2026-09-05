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
package org.apache.iceberg.flink.source;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.flink.FlinkRowData;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Applies a Flink (possibly nested) projection pushed down into the Iceberg source.
 *
 * <p>A single projection has two sides, both derived here from the source {@link RowType} and
 * Flink's {@code int[][]} projection paths:
 *
 * <ul>
 *   <li>{@link #readSchema()} — the schema handed to the reader: pruned to the projected fields, in
 *       table-schema order with nested structs left intact.
 *   <li>{@link #project(DataStream)} — projects the reader's rows into the produced (SELECT-list)
 *       order, extracting nested leaves into top-level columns, by adding a map step to the stream.
 *       The map is added only when the projection is nested or reorders top-level fields; an
 *       in-order projection is returned unchanged.
 * </ul>
 *
 * <p>Projection paths descend only through structs ({@link RowType}); they never descend through
 * the element of a list or the key/value of a map. Flink guarantees this: a subfield reference into
 * a list/map element (for example {@code SELECT people[1].name}) reads the whole element struct
 * rather than pushing a path into it, so a repeated type is always projected in full.
 */
final class Projector implements Serializable {

  private final RowType readSchema;
  private final boolean projectionNeeded;
  private final RowType producedRowType;
  private final RowProjection rowProjection;

  static Projector of(RowType sourceRowType, int[][] projectedFields, RowType producedRowType) {
    return new Projector(sourceRowType, projectedFields, producedRowType);
  }

  private Projector(RowType sourceRowType, int[][] projectedFields, RowType producedRowType) {
    this.readSchema = prune(sourceRowType, projectedFields);
    this.projectionNeeded = isProjectionNeeded(projectedFields);
    this.producedRowType = producedRowType;

    RowData.FieldGetter[] getters = new RowData.FieldGetter[projectedFields.length];
    for (int col = 0; col < projectedFields.length; col++) {
      getters[col] = getter(sourceRowType, readSchema, projectedFields[col]);
    }
    this.rowProjection = new RowProjection(getters);
  }

  /**
   * The schema handed to the reader: pruned to the projected fields, preserving original field
   * names, nesting, and table-schema order.
   */
  RowType readSchema() {
    return readSchema;
  }

  /**
   * Adds a map step projecting the reader's rows into the produced (SELECT-list) shape, or returns
   * the stream unchanged when no projection is needed (a non-nested, in-order projection).
   */
  DataStream<RowData> project(DataStream<RowData> stream) {
    if (!projectionNeeded) {
      return stream;
    }

    return stream
        .map(rowProjection)
        .setParallelism(stream.getParallelism())
        .returns(InternalTypeInfo.of(producedRowType));
  }

  private static boolean isProjectionNeeded(int[][] projectedFields) {
    int previousFieldIndex = -1;
    for (int[] path : projectedFields) {
      if (path.length > 1 || path[0] <= previousFieldIndex) {
        return true;
      }
      previousFieldIndex = path[0];
    }

    return false;
  }

  private static RowType prune(RowType rowType, int[][] projectedFields) {
    List<RowType.RowField> fields =
        Arrays.stream(projectedFields)
            .collect(
                Collectors.groupingBy(
                    path -> path[0],
                    TreeMap::new,
                    Collectors.mapping(
                        path -> Arrays.copyOfRange(path, 1, path.length), Collectors.toList())))
            .entrySet()
            .stream()
            .map(
                entry -> {
                  int fieldIndex = entry.getKey();
                  int[][] nestedFieldPaths = entry.getValue().toArray(new int[0][]);
                  RowType.RowField field = rowType.getFields().get(fieldIndex);
                  return prune(field, nestedFieldPaths);
                })
            .collect(Collectors.toList());

    return new RowType(rowType.isNullable(), fields);
  }

  private static RowType.RowField prune(RowType.RowField field, int[][] projectedFields) {
    boolean selectedWholeField =
        Arrays.stream(projectedFields).anyMatch(nestedFieldPath -> nestedFieldPath.length == 0);

    final LogicalType type;
    if (selectedWholeField) {
      type = field.getType();
    } else {
      Preconditions.checkArgument(
          field.getType() instanceof RowType,
          "Cannot project subfields of non-struct field <%s> of type <%s>",
          field.getName(),
          field.getType());
      type = prune((RowType) field.getType(), projectedFields);
    }

    return new RowType.RowField(field.getName(), type, field.getDescription().orElse(null));
  }

  private static RowData.FieldGetter getter(
      RowType originalRowType, RowType prunedRowType, int[] originalFieldPath) {
    int originalFieldIndex = originalFieldPath[0];
    String fieldName = originalRowType.getFieldNames().get(originalFieldIndex);
    int prunedFieldIndex = prunedRowType.getFieldNames().indexOf(fieldName);
    LogicalType prunedFieldType = prunedRowType.getTypeAt(prunedFieldIndex);

    if (originalFieldPath.length == 1) {
      return FlinkRowData.createFieldGetter(prunedFieldType, prunedFieldIndex);
    } else {
      LogicalType originalChildType = originalRowType.getTypeAt(originalFieldIndex);
      Preconditions.checkArgument(
          originalChildType instanceof RowType,
          "Cannot descend into non-struct field <%s> of type <%s> for nested projection",
          fieldName,
          originalChildType);

      RowType originalChildRowType = (RowType) originalChildType;
      RowType prunedChildRowType = (RowType) prunedFieldType;
      int childArity = prunedChildRowType.getFieldCount();
      RowData.FieldGetter childGetter =
          getter(
              originalChildRowType,
              prunedChildRowType,
              Arrays.copyOfRange(originalFieldPath, 1, originalFieldPath.length));
      return row ->
          row.isNullAt(prunedFieldIndex)
              ? null
              : childGetter.getFieldOrNull(row.getRow(prunedFieldIndex, childArity));
    }
  }

  private static final class RowProjection implements MapFunction<RowData, RowData> {

    private final RowData.FieldGetter[] getters;

    RowProjection(RowData.FieldGetter[] getters) {
      this.getters = getters;
    }

    @Override
    public RowData map(RowData row) {
      GenericRowData output = new GenericRowData(getters.length);
      for (int col = 0; col < getters.length; col++) {
        output.setField(col, getters[col].getFieldOrNull(row));
      }

      return output;
    }
  }
}
