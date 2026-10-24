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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TestMetadataPartitionNameConflicts {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "data", Types.StringType.get()),
          required(2, "category", Types.StringType.get()));

  private final InMemoryCatalog catalog = new InMemoryCatalog();

  @BeforeEach
  void initializeCatalog() {
    catalog.initialize("test", Map.of());
    catalog.createNamespace(Namespace.of("db"));
  }

  @AfterEach
  void closeCatalog() throws IOException {
    catalog.close();
  }

  @ParameterizedTest
  @CsvSource({"2, FILES", "3, FILES", "2, PARTITIONS", "3, PARTITIONS", "2, ENTRIES", "3, ENTRIES"})
  void scanConflictingPartitionNames(int formatVersion, MetadataTableType metadataType)
      throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table =
        catalog.createTable(
            TableIdentifier.of("db", "test"),
            SCHEMA,
            spec,
            Map.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)));
    appendFile(table, "old.parquet", TestHelpers.Row.of("old"));

    table.updateSpec().removeField("data").commit();
    table.updateSpec().addField("data_alias", Expressions.ref("data")).commit();
    table
        .updateSpec()
        .renameField("data_alias", "data")
        .addField(Expressions.bucket("category", 8))
        .commit();
    appendFile(table, "new.parquet", TestHelpers.Row.of("new", 3));

    Table metadata = MetadataTableUtils.createMetadataTableInstance(table, metadataType);
    String prefix = metadataType == MetadataTableType.ENTRIES ? "data_file.partition" : "partition";

    assertThat(partitions(metadata, prefix, Expressions.alwaysTrue()))
        .containsExactlyInAnyOrder(Arrays.asList("old", null), Arrays.asList(null, "new"));
    assertThat(partitions(metadata, prefix, Expressions.equal(prefix + ".data_1000", "old")))
        .containsExactly(Arrays.asList("old", null));
    assertThat(partitions(metadata, prefix, Expressions.equal(prefix + ".data_1001", "new")))
        .containsExactly(Arrays.asList(null, "new"));
  }

  private static void appendFile(Table table, String location, StructLike partition) {
    DataFile file =
        DataFiles.builder(table.spec())
            .withPath(location)
            .withPartition(partition)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newFastAppend().appendFile(file).commit();
  }

  private static List<List<String>> partitions(Table metadata, String prefix, Expression filter)
      throws IOException {
    TableScan scan =
        metadata.newScan().select(prefix + ".data_1000", prefix + ".data_1001").filter(filter);
    Evaluator evaluator = new Evaluator(scan.schema().asStruct(), filter, true);
    Accessor<StructLike> oldField = scan.schema().accessorForField(1000);
    Accessor<StructLike> newField = scan.schema().accessorForField(1001);
    List<List<String>> partitions = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
          for (StructLike row : rows) {
            if (evaluator.eval(row)) {
              partitions.add(
                  Arrays.asList(
                      Objects.toString(oldField.get(row), null),
                      Objects.toString(newField.get(row), null)));
            }
          }
        }
      }
    }

    return partitions;
  }
}
