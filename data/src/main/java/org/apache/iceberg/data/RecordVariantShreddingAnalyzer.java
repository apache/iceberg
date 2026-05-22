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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.VariantShreddingAnalyzer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantValue;

/**
 * Generic {@link Record} implementation that extracts variant values from {@link Record#get(int)}
 * using positional indices aligned with {@link Schema#columns()}.
 *
 * <p>Buffered rows must be laid out against the same {@link Schema} passed as {@code engineSchema};
 * otherwise {@link Record#get(int)} positions will not match the resolved column indices.
 */
class RecordVariantShreddingAnalyzer extends VariantShreddingAnalyzer<Record, Schema> {

  private final Map<Schema, Map<String, Integer>> columnIndicesBySchema = Maps.newHashMap();

  RecordVariantShreddingAnalyzer() {}

  @Override
  protected int resolveColumnIndex(Schema engineSchema, String columnName) {
    Preconditions.checkNotNull(engineSchema, "Invalid engine schema: null");

    Map<String, Integer> indices =
        columnIndicesBySchema.computeIfAbsent(
            engineSchema, RecordVariantShreddingAnalyzer::indexByName);
    Integer index = indices.get(columnName);
    return index != null ? index : -1;
  }

  private static Map<String, Integer> indexByName(Schema schema) {
    List<NestedField> cols = schema.columns();
    Map<String, Integer> indices = Maps.newHashMapWithExpectedSize(cols.size());
    for (int i = 0; i < cols.size(); i++) {
      indices.put(cols.get(i).name(), i);
    }
    return indices;
  }

  @Override
  protected List<VariantValue> extractVariantValues(
      List<Record> bufferedRows, int variantFieldIndex) {
    List<VariantValue> values = Lists.newArrayList();
    for (Record record : bufferedRows) {
      Object fieldValue = record.get(variantFieldIndex);
      if (fieldValue == null) {
        continue;
      }

      Preconditions.checkArgument(
          fieldValue instanceof Variant,
          "Expected Variant at index %s but was: %s",
          variantFieldIndex,
          fieldValue.getClass().getName());
      values.add(((Variant) fieldValue).value());
    }
    return values;
  }
}
