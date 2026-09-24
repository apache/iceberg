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

import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.junit.jupiter.api.Test;

class TestParquetFilters {

  @Test
  void convertsUuidLiterals() {
    Schema schema =
        new Schema(
            ImmutableList.of(required(1, "id", Types.UUIDType.get())), ImmutableMap.of("id", 1));
    UUID id = UUID.fromString("f79c3e09-677c-4cf5-8c4d-b8d77e78c919");

    FilterCompat.Filter filter = ParquetFilters.convert(schema, Expressions.equal("id", id), true);

    assertThat(filter).isInstanceOf(FilterCompat.FilterPredicateCompat.class);
  }
}
