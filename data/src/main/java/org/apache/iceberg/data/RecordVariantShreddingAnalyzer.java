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
 * {@link Record} analyzer that resolves variant columns by position in {@link Schema#columns()}.
 * Rows are read positionally, so an engine schema, if supplied, must place each variant column at
 * its Iceberg-schema position.
 */
class RecordVariantShreddingAnalyzer extends VariantShreddingAnalyzer<Record, Schema> {
  private static final Logger LOG = LoggerFactory.getLogger(RecordVariantShreddingAnalyzer.class);

  RecordVariantShreddingAnalyzer() {}

  @Override
  public Map<Integer, Type> analyzeVariantColumns(
      List<Record> bufferedRows, Schema icebergSchema, Schema engineSchema) {
    // Record rows are built against the Iceberg schema; use it when no engine schema is supplied.
    if (engineSchema == null) {
      return super.analyzeVariantColumns(bufferedRows, icebergSchema, icebergSchema);
    }

    checkVariantColumnPositionsAligned(icebergSchema, engineSchema);
    return super.analyzeVariantColumns(bufferedRows, icebergSchema, engineSchema);
  }

  @Override
  protected int resolveColumnIndex(Schema engineSchema, String columnName) {
    Preconditions.checkArgument(engineSchema != null, "Invalid engine schema: null");
    int index = positionOf(engineSchema, columnName);
    if (index < 0) {
      LOG.warn("Variant column {} not found in engine schema; skipping shredding", columnName);
    }

    return index;
  }

  // Rows are read positionally, so a variant column must sit at the same index in both schemas.
  private static void checkVariantColumnPositionsAligned(
      Schema icebergSchema, Schema engineSchema) {
    List<NestedField> icebergColumns = icebergSchema.columns();
    for (int icebergIndex = 0; icebergIndex < icebergColumns.size(); icebergIndex++) {
      NestedField col = icebergColumns.get(icebergIndex);
      if (!col.type().isVariantType()) {
        continue;
      }

      int engineIndex = positionOf(engineSchema, col.name());
      Preconditions.checkArgument(
          engineIndex < 0 || engineIndex == icebergIndex,
          "Variant column %s position mismatch between Iceberg and engine schemas: %s vs %s",
          col.name(),
          icebergIndex,
          engineIndex);
    }
  }

  private static int positionOf(Schema schema, String columnName) {
    List<NestedField> columns = schema.columns();
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

      if (!(fieldValue instanceof Variant)) {
        LOG.warn(
            "Skipping variant shredding for column at index {}: expected Variant but was {}",
            variantFieldIndex,
            fieldValue.getClass().getName());
        return Lists.newArrayList();
      }

      values.add(((Variant) fieldValue).value());
    }

    return values;
  }
}
