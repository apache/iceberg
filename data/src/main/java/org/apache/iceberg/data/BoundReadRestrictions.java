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

import java.util.Map;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;

/**
 * Read restrictions bound to a projection by {@link ReadRestrictionsApplier#bind}, ready to wrap a
 * stream of {@link Record}s.
 *
 * <p>Binding is separate from application so that everything that can fail happens before the
 * caller acquires a scan: a failure while wrapping an already-open iterable would leak it, since
 * the caller never receives a reference to close.
 */
class BoundReadRestrictions {

  private final Types.StructType struct;
  private final Evaluator rowFilter;
  private final Map<String, SerializableFunction<Object, Object>> masksByName;

  BoundReadRestrictions(
      Types.StructType struct,
      Evaluator rowFilter,
      Map<String, SerializableFunction<Object, Object>> masksByName) {
    this.struct = struct;
    this.rowFilter = rowFilter;
    this.masksByName = masksByName;
  }

  /**
   * Filters and masks the given records. The row filter is evaluated against the original values
   * before any mask is applied.
   */
  CloseableIterable<Record> apply(CloseableIterable<Record> records) {
    CloseableIterable<Record> result = records;

    if (rowFilter != null) {
      // the wrapper is stateful, so each stream gets its own
      InternalRecordWrapper wrapper = new InternalRecordWrapper(struct);
      result = CloseableIterable.filter(result, record -> rowFilter.eval(wrapper.wrap(record)));
    }

    if (!masksByName.isEmpty()) {
      result = CloseableIterable.transform(result, this::mask);
    }

    return result;
  }

  private Record mask(Record record) {
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
