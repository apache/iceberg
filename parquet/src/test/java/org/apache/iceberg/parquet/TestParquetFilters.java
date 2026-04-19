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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.junit.jupiter.api.Test;

public class TestParquetFilters {

  @Test
  public void testDecimalPredicateDoesNotThrow() {
    Schema schema = new Schema(required(1, "col", Types.DecimalType.of(10, 2)));
    Expression expr = Expressions.equal("col", new BigDecimal("12.34"));
    FilterCompat.Filter result = ParquetFilters.convert(schema, expr, true);
    // Should gracefully skip the filter instead of throwing UnsupportedOperationException
    assertThat(result).isEqualTo(FilterCompat.NOOP);
  }

  @Test
  public void testUuidPredicateDoesNotThrow() {
    Schema schema = new Schema(required(1, "col", Types.UUIDType.get()));
    Expression expr = Expressions.equal("col", UUID.randomUUID());
    FilterCompat.Filter result = ParquetFilters.convert(schema, expr, true);
    // Should gracefully skip the filter instead of throwing UnsupportedOperationException
    assertThat(result).isEqualTo(FilterCompat.NOOP);
  }
}
