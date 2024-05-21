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

import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestParquetFilters {

  private static final java.util.Map<String, Integer> aliases =
      new java.util.HashMap<String, Integer>() {
        {
          put("id", 1);
          put("age", 2);
        }
      };

  private static final Schema SCHEMA =
      new Schema(
          aliases, required(1, "id", IntegerType.get()), required(2, "age", DoubleType.get()));

  @Parameters(name = "writerVersion={0}")
  public static Collection<WriterVersion> parameters() {
    return Arrays.asList(WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0);
  }

  @TestTemplate
  public void testIntegerInFilter() {
    FilterCompat.Filter filter = (FilterCompat.Filter)  ParquetFilters.convert(SCHEMA, in("id", 1, 2, 3), true);

    try {
      java.lang.reflect.Field privateField = filter.getClass().getDeclaredField("filterPredicate");
      privateField.setAccessible(true);
      assertThat(privateField.get(filter).toString().equalsIgnoreCase("in(id, 1, 2, 3)")).isTrue();
    } catch (Exception e) {
      assertThat(true).isFalse();
    }
  }

  @TestTemplate
  public void testDoubleNotInFilter() {
    FilterCompat.Filter filter = (FilterCompat.Filter)  ParquetFilters.convert(SCHEMA, notIn("age", 1.0, 2.0, 3.0), true);

    try {
      java.lang.reflect.Field privateField = filter.getClass().getDeclaredField("filterPredicate");
      privateField.setAccessible(true);
      assertThat(privateField.get(filter).toString().equalsIgnoreCase("notin(age, 1.0, 2.0, 3.0)"))
          .isTrue();
    } catch (Exception e) {
      assertThat(true).isFalse();
    }
  }
}
