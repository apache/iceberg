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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestParquetDataFrameWrite extends DataFrameWriteTestBase {
  @Override
  protected boolean supportsVariant() {
    return true;
  }

  @Override
  protected void configureTable(Table table) {
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.toString())
        .commit();
  }

  @Test
  @Override
  public void testUnknownListType() {
    assertThatThrownBy(super::testUnknownListType)
        .isInstanceOf(SparkException.class)
        .cause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot convert element Parquet: unknown");
  }

  @Test
  @Override
  public void testUnknownMapType() {
    assertThatThrownBy(super::testUnknownMapType)
        .isInstanceOf(SparkException.class)
        .cause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot convert value Parquet: unknown");
  }

  @ParameterizedTest
  @FieldSource("SIMPLE_TYPES")
  public void testTypeSchema(Type type) throws IOException {
    assumeThat(supportsTime() || TypeUtil.find(type, t -> t.typeId() == Type.TypeID.TIME) == null)
        .as("Spark does not support time fields")
        .isTrue();
    assumeThat(
            supportsTimestampNanos()
                || TypeUtil.find(type, t -> t.typeId() == Type.TypeID.TIMESTAMP_NANO) == null)
        .as("timestamp_ns is not yet implemented")
        .isTrue();
    assumeThat(
            supportsVariant()
                || TypeUtil.find(type, t -> t.typeId() == Type.TypeID.VARIANT) == null)
        .as("variant is not yet implemented")
        .isTrue();
    if (!supportsGeospatial()) {
      assumeThat(TypeUtil.find(type, t -> t.typeId() == Type.TypeID.GEOMETRY) == null)
          .as("geometry is not yet implemented")
          .isTrue();
      assumeThat(TypeUtil.find(type, t -> t.typeId() == Type.TypeID.GEOGRAPHY) == null)
          .as("geography is not yet implemented")
          .isTrue();
    }

    writeAndValidate(
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "test_type", type),
            required(3, "trailing_data", Types.StringType.get())));
  }
}
