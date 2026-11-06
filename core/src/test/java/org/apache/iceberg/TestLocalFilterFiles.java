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

import static org.apache.iceberg.TestHelpers.ALL_VERSIONS;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.Variants;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestLocalFilterFiles
    extends FilterFilesTestBase<TableScan, FileScanTask, CombinedScanTask> {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> formatVersions() {
    return ALL_VERSIONS;
  }

  @Override
  protected TableScan newScan(Table table) {
    return table.newScan();
  }

  @TestTemplate
  public void testFilterFilesByVariantExtract() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "var", Types.VariantType.get()));
    Table table =
        TestTables.create(
            temp.toFile(), "test", schema, PartitionSpec.unpartitioned(), formatVersion);

    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    lowerBounds.put(2, VariantTestUtil.variantBuffer(Map.of("$['event_id']", Variants.of(1L))));
    upperBounds.put(2, VariantTestUtil.variantBuffer(Map.of("$['event_id']", Variants.of(2L))));

    Metrics metrics =
        new Metrics(
            2L,
            Maps.newHashMap(),
            Maps.newHashMap(),
            Maps.newHashMap(),
            null,
            lowerBounds,
            upperBounds);

    DataFile file =
        DataFiles.builder(table.spec())
            .withPath("/path/to/file.parquet")
            .withFileSizeInBytes(0)
            .withMetrics(metrics)
            .build();

    table.newAppend().appendFile(file).commit();

    table.refresh();

    TableScan emptyScan =
        newScan(table)
            .filter(Expressions.equal(Expressions.extract("var", "$.event_id", "long"), 5L));
    assertThat(emptyScan.planFiles()).isEmpty();

    TableScan nonEmptyScan =
        newScan(table)
            .filter(Expressions.equal(Expressions.extract("var", "$.event_id", "long"), 1L));
    assertThat(nonEmptyScan.planFiles()).hasSize(1);
  }
}
