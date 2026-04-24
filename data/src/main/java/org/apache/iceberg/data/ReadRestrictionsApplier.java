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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.restrictions.Action;
import org.apache.iceberg.rest.restrictions.Actions;
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
    if (rowFilter == null || rowFilter == Expressions.alwaysTrue()) {
      return records;
    }

    Types.StructType struct = projection.asStruct();
    Evaluator evaluator = new Evaluator(struct, rowFilter, true);
    InternalRecordWrapper wrapper = new InternalRecordWrapper(struct);
    return CloseableIterable.filter(records, record -> evaluator.eval(wrapper.wrap(record)));
  }

  private static CloseableIterable<Record> maskColumns(
      CloseableIterable<Record> records, List<Action> actions, Schema projection) {
    if (actions.isEmpty()) {
      return records;
    }

    Map<String, SerializableFunction<Object, Object>> masksByName = bindMasks(actions, projection);
    return CloseableIterable.transform(records, record -> mask(record, masksByName));
  }

  private static Map<String, SerializableFunction<Object, Object>> bindMasks(
      List<Action> actions, Schema projection) {
    ImmutableMap.Builder<String, SerializableFunction<Object, Object>> builder =
        ImmutableMap.builder();
    byte[] querySalt = null;
    List<Types.NestedField> topLevelFields = projection.asStruct().fields();

    for (Action action : actions) {
      int fieldId = action.fieldId();
      Types.NestedField field = findTopLevel(topLevelFields, fieldId);
      if (field == null) {
        // Fail closed: nested masks, unknown fieldIds, or fields projected away all reach here.
        // Skipping them silently would either leak unmasked values (nested case) or surprise the
        // caller (typo case). The latter is acceptable noise since this surfaces at bind time.
        String path = projection.findColumnName(fieldId);
        if (path == null) {
          throw new IllegalStateException(
              "ReadRestrictions references unknown field id: " + fieldId);
        }
        throw new IllegalStateException(
            "ReadRestrictions on nested fields are not yet supported "
                + "(fieldId="
                + fieldId
                + ", path='"
                + path
                + "')");
      }

      byte[] salt = null;
      if (action instanceof Action.Sha256QueryLocal) {
        if (querySalt == null) {
          querySalt = new byte[SALT_LENGTH];
          RANDOM.nextBytes(querySalt);
        }
        salt = querySalt;
      }

      builder.put(field.name(), Actions.bind(action, field.type(), salt));
    }

    return builder.build();
  }

  private static Types.NestedField findTopLevel(List<Types.NestedField> fields, int fieldId) {
    for (Types.NestedField field : fields) {
      if (field.fieldId() == fieldId) {
        return field;
      }
    }
    return null;
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
