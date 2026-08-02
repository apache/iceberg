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
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.VariantShreddingAnalyzer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic {@link Record} analyzer that resolves variant columns by their position in {@link
 * Schema#columns()}; buffered rows must be built against the same {@link Schema} passed as the
 * engine schema so {@link Record#get(int)} positions match the resolved indices.
 */
class RecordVariantShreddingAnalyzer extends VariantShreddingAnalyzer<Record, Schema> {
  private static final Logger LOG = LoggerFactory.getLogger(RecordVariantShreddingAnalyzer.class);

  RecordVariantShreddingAnalyzer() {}

  @Override
  protected int resolveColumnIndex(Schema engineSchema, String columnName) {
    Preconditions.checkArgument(engineSchema != null, "Invalid engine schema: null");
    List<NestedField> columns = engineSchema.columns();
    for (int index = 0; index < columns.size(); index++) {
      if (columns.get(index).name().equals(columnName)) {
        return index;
      }
    }

    LOG.warn("Variant column {} not found in engine schema; skipping shredding", columnName);
    return -1;
  }

  @Override
  protected List<VariantValue> extractVariantValues(
      List<Record> bufferedRows, int variantFieldIndex) {
    List<VariantValue> values = Lists.newArrayList();
    for (Record row : bufferedRows) {
      Object fieldValue = row.get(variantFieldIndex);
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
