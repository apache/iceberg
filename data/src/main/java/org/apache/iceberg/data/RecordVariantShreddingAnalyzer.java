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
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantValue;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Record} analyzer for variant columns. An engine schema, if supplied, must be identical to
 * the Iceberg schema, since rows are read positionally against it.
 */
class RecordVariantShreddingAnalyzer extends VariantShreddingAnalyzer<Record, Schema> {
  private static final Logger LOG = LoggerFactory.getLogger(RecordVariantShreddingAnalyzer.class);

  RecordVariantShreddingAnalyzer() {}

  @Override
  public Map<Integer, Type> analyzeVariantColumns(
      List<Record> bufferedRows, Schema icebergSchema, Schema engineSchema) {
    // Resolve against the Iceberg schema (rows are positional); an engine schema must match it.
    Preconditions.checkArgument(
        engineSchema == null || engineSchema.sameSchema(icebergSchema),
        "Engine schema must match the Iceberg schema to shred variants: %s vs %s",
        engineSchema,
        icebergSchema);
    return super.analyzeVariantColumns(bufferedRows, icebergSchema, icebergSchema);
  }

  @Override
  protected int resolveColumnIndex(Schema engineSchema, String columnName) {
    List<NestedField> columns = engineSchema.columns();
    for (int index = 0; index < columns.size(); index++) {
      if (columns.get(index).name().equals(columnName)) {
        return index;
      }
    }

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

      if (!(fieldValue instanceof Variant variant)) {
        LOG.warn(
            "Skipping variant shredding for column at index {}: expected Variant but was {}",
            variantFieldIndex,
            fieldValue.getClass().getName());
        return Lists.newArrayList();
      }

      values.add(variant.value());
    }

    return values;
  }
}
