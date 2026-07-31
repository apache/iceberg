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
package org.apache.iceberg.data;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.functions.ReplaceWithNull;
import org.apache.iceberg.functions.SaltedFunction;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;

/**
 * Applies server-provided {@link ReadRestrictions} (row filter + column masks) to a stream of
 * {@link Record}s.
 *
 * <p>The row filter is evaluated per-record against the original column values before any mask is
 * applied, as required by the spec:
 *
 * <blockquote>
 *
 * Row filters MUST be evaluated against the original, untransformed column values. Required
 * projections MUST be applied only after row filters are applied.
 *
 * </blockquote>
 *
 * <p>Callers that also push the row filter into {@link org.apache.iceberg.TableScan#filter} get
 * partition/stats-level pruning for free; this applier re-evaluates the filter at the row level so
 * correctness does not depend on whether the surrounding reader honors residual evaluation.
 *
 * <p>Currently supports top-level fields only. Masks on nested fieldIds fail closed at bind time so
 * unmasked nested data cannot leak.
 *
 * <p>Projections for columns that are not being read are skipped, as required by the spec:
 *
 * <blockquote>
 *
 * A reader must enforce projections on the columns it is actually reading. Projections referencing
 * columns that are not being read do not apply.
 *
 * </blockquote>
 */
class ReadRestrictionsApplier {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final int SALT_LENGTH = 16;

  private ReadRestrictionsApplier() {}

  static CloseableIterable<Record> apply(
      CloseableIterable<Record> records, ReadRestrictions restrictions, Schema projection) {
    CloseableIterable<Record> filtered = filterRows(records, restrictions.rowFilter(), projection);
    return maskColumns(filtered, restrictions.columnProjections(), projection);
  }

  private static CloseableIterable<Record> filterRows(
      CloseableIterable<Record> records, Expression rowFilter, Schema projection) {
    if (rowFilter == null || rowFilter.op() == Expression.Operation.TRUE) {
      return records;
    }

    Types.StructType struct = projection.asStruct();
    Evaluator evaluator = new Evaluator(struct, rowFilter, true);
    InternalRecordWrapper wrapper = new InternalRecordWrapper(struct);
    return CloseableIterable.filter(records, record -> evaluator.eval(wrapper.wrap(record)));
  }

  private static CloseableIterable<Record> maskColumns(
      CloseableIterable<Record> records, List<IcebergFunction<?, ?>> actions, Schema projection) {
    if (actions.isEmpty()) {
      return records;
    }

    Map<String, SerializableFunction<Object, Object>> masksByName = bindMasks(actions, projection);
    if (masksByName.isEmpty()) {
      return records;
    }

    return CloseableIterable.transform(records, record -> mask(record, masksByName));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, SerializableFunction<Object, Object>> bindMasks(
      List<IcebergFunction<?, ?>> actions, Schema projection) {
    ImmutableMap.Builder<String, SerializableFunction<Object, Object>> builder =
        ImmutableMap.builder();
    byte[] querySalt = null;

    for (IcebergFunction<?, ?> action : actions) {
      int fieldId = action.fieldId();
      Types.NestedField field = projection.asStruct().field(fieldId);
      if (field == null) {
        // A fieldId that resolves inside the projection but is not top-level is nested. Fail
        // closed so unmasked nested values cannot leak.
        String nestedPath = projection.findColumnName(fieldId);
        Preconditions.checkState(
            nestedPath == null,
            "ReadRestrictions on nested fields are not yet supported (fieldId=%s, path='%s')",
            fieldId,
            nestedPath);

        // The column is not being read. Per spec, projections referencing columns that are not
        // being read do not apply. Field ids are validated against the table's schemas when the
        // restrictions are attached to it (ReadRestrictions#validate), so reaching here means the
        // column exists and was projected away rather than being unknown.
        continue;
      }

      // ReplaceWithNull cannot check nullability itself because Type does not carry the field's
      // required/optional flag, so the caller must reject required fields.
      if (action instanceof ReplaceWithNull) {
        Preconditions.checkState(
            field.isOptional(),
            "Cannot apply replace-with-null to required field: %s (fieldId=%s)",
            field.name(),
            fieldId);
      }

      SerializableFunction<Object, Object> bound;
      if (action instanceof SaltedFunction) {
        if (querySalt == null) {
          querySalt = new byte[SALT_LENGTH];
          RANDOM.nextBytes(querySalt);
        }
        bound =
            (SerializableFunction<Object, Object>)
                ((SaltedFunction<?, ?>) action).bind(field.type(), querySalt);
      } else {
        bound = (SerializableFunction<Object, Object>) action.bind(field.type());
      }
      builder.put(field.name(), bound);
    }

    return builder.build();
  }

  private static Record mask(
      Record record, Map<String, SerializableFunction<Object, Object>> masksByName) {
    GenericRecord out = GenericRecord.create(record.struct());
    for (int i = 0; i < record.size(); i++) {
      out.set(i, record.get(i, Object.class));
    }
    for (Map.Entry<String, SerializableFunction<Object, Object>> entry : masksByName.entrySet()) {
      Object original = out.getField(entry.getKey());
      Object masked = entry.getValue().apply(original);
      out.setField(entry.getKey(), masked);
    }
    return out;
  }
}
