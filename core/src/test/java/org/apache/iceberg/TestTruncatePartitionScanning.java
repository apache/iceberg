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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestTruncatePartitionScanning {
  @Test
  void largeWidthRetainsMatchingPartition() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("id", Integer.MAX_VALUE).build();
    PartitionKey key = new PartitionKey(spec, schema);
    key.partition(TestHelpers.Row.of(Integer.MAX_VALUE - 1));

    try (InMemoryCatalog catalog = new InMemoryCatalog()) {
      catalog.initialize("test", Map.of());
      catalog.createNamespace(Namespace.of("db"));
      Table table = catalog.createTable(TableIdentifier.of("db", "table"), schema, spec);
      DataFile file =
          DataFiles.builder(spec)
              .withPath("/data/file.parquet")
              .withPartition(key)
              .withFileSizeInBytes(100)
              .withRecordCount(1)
              .build();
      table.newAppend().appendFile(file).commit();

      try (CloseableIterable<FileScanTask> tasks =
          table.newScan().filter(Expressions.greaterThanOrEqual("id", 0)).planFiles()) {
        assertThat(tasks)
            .extracting(task -> task.file().location())
            .containsExactly(file.location());
      }
    }
  }
}
