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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures the generic read path through {@link DeleteFilter} when applying position and equality
 * deletes, plus the no-delete baseline.
 *
 * <p>Two views are measured per scenario:
 *
 * <ul>
 *   <li>{@code scan} - end-to-end read via {@link IcebergGenerics} (Parquet decode + delete filter)
 *   <li>{@code filterOnly} - the delete filter applied to pre-materialized in-memory records,
 *       isolating the per-row iterator/predicate cost from file decode
 * </ul>
 *
 * The {@code deleteType} parameter covers NONE (plain scan), POSITION (position deletes, the common
 * merge-on-read / deletion-vector shape) and EQUALITY (a control where the equality-delete path
 * does real work).
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class DeleteFilterBenchmark {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "intCol", Types.IntegerType.get()),
          optional(3, "doubleCol", Types.DoubleType.get()),
          optional(4, "stringCol", Types.StringType.get()));

  private static final int NUM_ROWS = 2_500_000;
  private static final long SEED = -1L;

  // delete every 20th row -> 5% delete ratio
  private static final int DELETE_EVERY = 20;

  @Param({"NONE", "POSITION", "EQUALITY"})
  private String deleteType;

  private final HadoopTables tables = new HadoopTables();

  private File tableDir;
  private Table table;
  private List<Record> filterInput;
  private GenericDeleteFilter filter;

  @Setup
  public void setupBenchmark() throws IOException {
    this.tableDir = java.nio.file.Files.createTempDirectory("delete-filter-bench").toFile();
    this.table =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"),
            tableDir.toURI().toString());

    List<Record> rows = RandomGenericData.generate(SCHEMA, NUM_ROWS, SEED);
    // make ids sequential so position == id, giving deterministic deletes
    for (int i = 0; i < rows.size(); i++) {
      rows.get(i).setField("id", (long) i);
    }

    DataFile dataFile =
        FileHelpers.writeDataFile(
            table, Files.localOutput(new File(tableDir, "data.parquet")), rows);
    table.newAppend().appendFile(dataFile).commit();

    switch (deleteType) {
      case "POSITION":
        List<Pair<CharSequence, Long>> posDeletes = Lists.newArrayList();
        for (long pos = 0; pos < NUM_ROWS; pos += DELETE_EVERY) {
          posDeletes.add(Pair.of(dataFile.location(), pos));
        }
        DeleteFile posDeleteFile =
            FileHelpers.writeDeleteFile(
                    table, Files.localOutput(new File(tableDir, "pos-deletes.parquet")), posDeletes)
                .first();
        table.newRowDelta().addDeletes(posDeleteFile).commit();
        break;

      case "EQUALITY":
        Schema deleteRowSchema = table.schema().select("id");
        Record keyTemplate = GenericRecord.create(deleteRowSchema);
        List<Record> eqDeletes = Lists.newArrayList();
        for (long id = 0; id < NUM_ROWS; id += DELETE_EVERY) {
          eqDeletes.add(keyTemplate.copy(ImmutableMap.of("id", id)));
        }
        DeleteFile eqDeleteFile =
            FileHelpers.writeDeleteFile(
                table,
                Files.localOutput(new File(tableDir, "eq-deletes.parquet")),
                eqDeletes,
                deleteRowSchema);
        table.newRowDelta().addDeletes(eqDeleteFile).commit();
        break;

      default:
        // NONE: no delete files
        break;
    }

    // build the reusable filter and its pre-materialized input for the filterOnly benchmark
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(planned);
    }
    FileScanTask task = Iterables.getOnlyElement(tasks);
    this.filter = new GenericDeleteFilter(table.io(), task, table.schema(), table.schema());
    this.filterInput = materializeFilterInput(rows, filter.requiredSchema());
  }

  // The filter reads the _pos metadata column for position deletes, so when the required schema
  // includes it we augment each record with its row position. For NONE/EQUALITY the required schema
  // equals the table schema and the generated rows are used directly.
  private List<Record> materializeFilterInput(List<Record> rows, Schema requiredSchema) {
    boolean hasPositionColumn =
        requiredSchema.findField(MetadataColumns.ROW_POSITION.fieldId()) != null;
    if (!hasPositionColumn) {
      return rows;
    }

    List<Record> withPositions = Lists.newArrayListWithCapacity(rows.size());
    for (int i = 0; i < rows.size(); i++) {
      Record base = rows.get(i);
      GenericRecord withPos = GenericRecord.create(requiredSchema);
      for (Types.NestedField field : table.schema().columns()) {
        withPos.setField(field.name(), base.getField(field.name()));
      }
      withPos.setField(MetadataColumns.ROW_POSITION.name(), (long) i);
      withPositions.add(withPos);
    }
    return withPositions;
  }

  @TearDown
  public void tearDownBenchmark() {
    if (table != null) {
      tables.dropTable(tableDir.toURI().toString());
    }
    deleteRecursively(tableDir);
  }

  @Benchmark
  @Threads(1)
  public void scan(Blackhole blackhole) throws IOException {
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).reuseContainers().build()) {
      for (Record record : reader) {
        blackhole.consume(record);
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void filterOnly(Blackhole blackhole) throws IOException {
    CloseableIterable<Record> source = CloseableIterable.withNoopClose(filterInput);
    try (CloseableIterable<Record> filtered = filter.filter(source)) {
      for (Record record : filtered) {
        blackhole.consume(record);
      }
    }
  }

  private static void deleteRecursively(File file) {
    if (file == null) {
      return;
    }
    File[] children = file.listFiles();
    if (children != null) {
      for (File child : children) {
        deleteRecursively(child);
      }
    }
    file.delete();
  }
}
