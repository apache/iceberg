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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Int96TestUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestFlinkInt96Reader {
  private static final int ROW_COUNT = 32;

  @TempDir private Path temp;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rejectsUnsupportedInt96Files(boolean dictionary) throws Exception {
    File file =
        Int96TestUtil.write(
            temp.resolve("timestamps.parquet"),
            dictionary,
            Collections.nCopies(ROW_COUNT, BigInteger.valueOf(1_000)));
    Schema schema = new Schema(Types.NestedField.optional(2, "ts", Types.TimestampType.withZone()));

    assertThatThrownBy(
            () -> {
              try (CloseableIterable<RowData> rows =
                  Parquet.read(Files.localInput(file))
                      .project(schema)
                      .createReaderFunc(type -> FlinkParquetReaders.buildReader(schema, type))
                      .build()) {
                rows.iterator().next();
              }
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Unsupported type:")
        .hasMessageContaining("int96");
  }
}
