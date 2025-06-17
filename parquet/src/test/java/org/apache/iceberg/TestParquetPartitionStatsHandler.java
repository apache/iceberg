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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestParquetPartitionStatsHandler extends PartitionStatsHandlerTestBase {

  public FileFormat format() {
    return FileFormat.PARQUET;
  }

  @Test
  public void testReadingStatsWithInvalidSchema() throws Exception {
    Table testTable =
        TestTables.create(tempDir("old_schema"), "old_schema", SCHEMA, SPEC, 2, fileFormatProperty);
    Schema schema = PartitionStatsHandler.schema(Partitioning.partitionType(testTable));

    String invalidSchema =
        getClass()
            .getClassLoader()
            .getResource("org/apache/iceberg/PartitionStatsInvalidSchema.parquet")
            .toString();

    try (CloseableIterable<PartitionStats> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            schema, testTable.io().newInputFile(invalidSchema))) {
      assertThatThrownBy(() -> Lists.newArrayList(recordIterator))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Not a primitive type: struct");
    }
  }
}
