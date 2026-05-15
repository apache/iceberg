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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;

/**
 * Variant shredding analyzer for generic {@link Record} types.
 *
 * <p>This analyzer extracts {@link Variant} values from {@link Record} objects and determines
 * optimal shredding schemas by analyzing data distributions across buffered rows. The analyzer is
 * used by Kafka Connect and other tools that work with generic Record types to enable automatic
 * variant shredding for Parquet writes.
 *
 * <p>Shredding extracts frequently-occurring fields from variant data into typed Parquet columns
 * for improved query performance while maintaining the full variant data in the raw value field.
 */
class RecordVariantShreddingAnalyzer extends VariantShreddingAnalyzer<Record, Schema> {

  @Override
  protected List<VariantValue> extractVariantValues(
      List<Record> bufferedRows, int variantFieldIndex) {
    List<VariantValue> values = Lists.newArrayList();
    for (Record record : bufferedRows) {
      Object fieldValue = record.get(variantFieldIndex);
      if (fieldValue instanceof Variant) {
        Variant variant = (Variant) fieldValue;
        VariantValue value = Variants.value(variant.metadata(), variant.value());
        values.add(value);
      }
    }
    return values;
  }

  @Override
  protected int resolveColumnIndex(Schema engineSchema, String columnName) {
    NestedField field = engineSchema.findField(columnName);
    return field != null ? engineSchema.columns().indexOf(field) : -1;
  }
}
