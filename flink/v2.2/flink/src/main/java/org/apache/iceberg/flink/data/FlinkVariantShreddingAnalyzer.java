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
package org.apache.iceberg.flink.data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.Variant;
import org.apache.iceberg.parquet.VariantShreddingAnalyzer;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;

/**
 * Analyzes Variant fields in Flink {@link RowData} and converts Flink's binary Variant
 * representation to Iceberg {@link VariantValue} instances for Variant shredding.
 */
public class FlinkVariantShreddingAnalyzer extends VariantShreddingAnalyzer<RowData, RowType> {

  @Override
  protected List<VariantValue> extractVariantValues(
      List<RowData> bufferedRows, int variantFieldIndex) {
    List<VariantValue> values = Lists.newArrayList();

    for (RowData row : bufferedRows) {
      if (!row.isNullAt(variantFieldIndex)) {
        Variant flinkVariant = row.getVariant(variantFieldIndex);
        if (flinkVariant != null) {
          if (flinkVariant instanceof BinaryVariant binaryVariant) {
            VariantValue variantValue =
                VariantValue.from(
                    VariantMetadata.from(
                        ByteBuffer.wrap(binaryVariant.getMetadata())
                            .order(ByteOrder.LITTLE_ENDIAN)),
                    ByteBuffer.wrap(binaryVariant.getValue()).order(ByteOrder.LITTLE_ENDIAN));

            values.add(variantValue);
          } else {
            throw new UnsupportedOperationException(
                "Not a supported type: " + flinkVariant.getClass());
          }
        }
      }
    }

    return values;
  }

  @Override
  protected int resolveColumnIndex(RowType flinkSchema, String columnName) {
    return flinkSchema.getFieldIndex(columnName);
  }
}
