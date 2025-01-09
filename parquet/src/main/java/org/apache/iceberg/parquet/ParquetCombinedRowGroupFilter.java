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
package org.apache.iceberg.parquet;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

public class ParquetCombinedRowGroupFilter {

  private final Schema schema;
  private final Expression expr;
  private final boolean caseSensitive;

  public ParquetCombinedRowGroupFilter(Schema schema, Expression expr, boolean caseSensitive) {
    this.schema = schema;
    Types.StructType struct = schema.asStruct();
    if (Binder.isBound(expr)) {
      this.expr = Expressions.rewriteNot(expr);
    } else {
      this.expr = Binder.bind(struct, Expressions.rewriteNot(expr), caseSensitive);
    }

    this.caseSensitive = caseSensitive;
  }

  /**
   * Test whether the file may contain records that match the expression.
   *
   * @param fileSchema schema for the Parquet file
   * @param rowGroup metadata for a row group
   * @param reader file reader for the Parquet file
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean shouldRead(
      MessageType fileSchema, BlockMetaData rowGroup, ParquetFileReader reader) {
    return residualFor(fileSchema, rowGroup, reader) != Expressions.alwaysFalse();
  }

  public Expression residualFor(
      MessageType fileSchema, BlockMetaData rowGroup, ParquetFileReader reader) {
    ParquetMetricsRowGroupFilter metricFilter =
        new ParquetMetricsRowGroupFilter(schema, expr, caseSensitive);
    Expression metricResidual = metricFilter.residualFor(fileSchema, rowGroup);
    if (metricResidual == Expressions.alwaysFalse() || metricResidual == Expressions.alwaysTrue()) {
      return metricResidual;
    }

    ParquetDictionaryRowGroupFilter dictFilter =
        new ParquetDictionaryRowGroupFilter(schema, metricResidual, caseSensitive);

    Expression dictResidual =
        dictFilter.residualFor(fileSchema, rowGroup, reader.getDictionaryReader(rowGroup));

    if (dictResidual == Expressions.alwaysFalse() || dictResidual == Expressions.alwaysTrue()) {
      return dictResidual;
    }

    ParquetBloomRowGroupFilter bloomFilter =
        new ParquetBloomRowGroupFilter(schema, dictResidual, caseSensitive);
    return bloomFilter.residualFor(fileSchema, rowGroup, reader.getBloomFilterDataReader(rowGroup));
  }
}
