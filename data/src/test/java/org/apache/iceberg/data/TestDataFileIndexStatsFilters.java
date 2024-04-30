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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDataFileIndexStatsFilters {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.required(3, "category", Types.StringType.get()));

  @TempDir private Path temp;

  private Table table;
  private List<Record> records = null;
  private List<Record> oddRecords = null;
  private List<Record> evenRecords = null;
  private DataFile dataFile = null;
  private DataFile dataFileWithoutNulls = null;
  private DataFile dataFileOnlyNulls = null;

  @BeforeEach
  public void createTableAndData() throws IOException {
    File location = File.createTempFile("junit", null, temp.toFile());
    this.table = TestTables.create(location, "test", SCHEMA, PartitionSpec.unpartitioned(), 2);

    this.records = Lists.newArrayList();

    Record record = GenericRecord.create(table.schema());
    records.add(record.copy("id", 1, "data", "a", "category", "odd"));
    records.add(record.copy("id", 2, "data", "b", "category", "even"));
    records.add(record.copy("id", 3, "data", "c", "category", "odd"));
    records.add(record.copy("id", 4, "data", "d", "category", "even"));
    records.add(record.copy("id", 5, "data", "e", "category", "odd"));
    records.add(record.copy("id", 6, "data", "f", "category", "even"));
    records.add(record.copy("id", 7, "data", "g", "category", "odd"));
    records.add(record.copy("id", 8, "data", null, "category", "even"));

    this.oddRecords =
        records.stream()
            .filter(rec -> rec.getField("category").equals("odd"))
            .collect(Collectors.toList());
    this.evenRecords =
        records.stream()
            .filter(rec -> rec.getField("category").equals("even"))
            .collect(Collectors.toList());

    this.dataFile = FileHelpers.writeDataFile(table, Files.localOutput(temp.toFile()), records);
    this.dataFileWithoutNulls =
        FileHelpers.writeDataFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            records.stream()
                .filter(rec -> rec.getField("data") != null)
                .collect(Collectors.toList()));
    this.dataFileOnlyNulls =
        FileHelpers.writeDataFile(
            table,
                Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            records.stream()
                .filter(rec -> rec.getField("data") == null)
                .collect(Collectors.toList()));
  }

  @AfterEach
  public void dropTable() {
    TestTables.clearTables();
  }

  @Test
  public void testPositionDeletePlanningPath() throws IOException {
    table.newAppend().appendFile(dataFile).commit();

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    deletes.add(Pair.of(dataFile.path(), 0L));
    deletes.add(Pair.of(dataFile.path(), 1L));

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(File.createTempFile("junit", null, temp.toFile())), deletes);
    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assertions.assertThat(tasks).as("Should produce one task").hasSize(1);
    FileScanTask task = tasks.get(0);
    Assertions.assertThat(task.deletes()).as("Should have one delete file, file_path matches").hasSize(1);
  }

  @Test
  public void testPositionDeletePlanningPathFilter() throws IOException {
    table.newAppend().appendFile(dataFile).commit();

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    deletes.add(Pair.of("some-other-file.parquet", 0L));
    deletes.add(Pair.of("some-other-file.parquet", 1L));

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(File.createTempFile("junit", null, temp.toFile())), deletes);
    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assertions.assertThat(tasks).as("Should produce one task").hasSize(1);
    FileScanTask task = tasks.get(0);
    Assertions.assertThat(task.deletes()).as("Should not have delete file, filtered by file_path stats").hasSize(0);
  }

  @Test
  public void testEqualityDeletePlanningStats() throws IOException {
    table.newAppend().appendFile(dataFile).commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = SCHEMA.select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    deletes.add(delete.copy("data", "d"));

    DeleteFile posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(File.createTempFile("junit", null, temp.toFile())), deletes, deleteRowSchema);

    table.newRowDelta().addDeletes(posDeletes).commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assertions.assertThat(1).isEqualTo(tasks.size()).as("Should produce one task");
    FileScanTask task = tasks.get(0);
    Assertions.assertThat(task.deletes())
        .as("Should have one delete file, data contains a matching value").hasSize(1);
  }

  @Test
  public void testEqualityDeletePlanningStatsFilter() throws IOException {
    table.newAppend().appendFile(dataFile).commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = table.schema().select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    deletes.add(delete.copy("data", "x"));
    deletes.add(delete.copy("data", "y"));
    deletes.add(delete.copy("data", "z"));

    DeleteFile posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(File.createTempFile("junit", null, temp.toFile())), deletes, deleteRowSchema);

    table.newRowDelta().addDeletes(posDeletes).commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assertions.assertThat(tasks.size()).isEqualTo(1).as("Should produce one task");
    FileScanTask task = tasks.get(0);
    Assertions.assertThat(task.deletes()).as(..).isEmpty();`
        .isEqualTo(0)
        .as("Should not have delete file, filtered by data column stats");
  }

  @Test
  public void testEqualityDeletePlanningStatsNullValueWithAllNullDeletes() throws IOException {
    table.newAppend().appendFile(dataFile).commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = SCHEMA.select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    deletes.add(delete.copy("data", null));

    DeleteFile posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(File.createTempFile("junit", null, temp.toFile())), deletes, deleteRowSchema);

    table.newRowDelta().addDeletes(posDeletes).commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assertions.assertThat(tasks.size()).isEqualTo(1).as("Should produce one task");
    FileScanTask task = tasks.get(0);
    Assertions.assertThat(task.deletes().size())
        .isEqualTo(1)
        .as("Should have delete file, data contains a null value");
  }

  @Test
  public void testEqualityDeletePlanningStatsNoNullValuesWithAllNullDeletes() throws IOException {
    table
        .newAppend()
        .appendFile(dataFileWithoutNulls) // note that there are no nulls in the data column
        .commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = SCHEMA.select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    deletes.add(delete.copy("data", null));

    DeleteFile posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(File.createTempFile("junit", null, temp.toFile())), deletes, deleteRowSchema);

    table.newRowDelta().addDeletes(posDeletes).commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assertions.assertThat(tasks.size()).isEqualTo(1).as("Should produce one task");
    FileScanTask task = tasks.get(0);
    Assertions.assertThat(task.deletes().size())
        .isEqualTo(0)
        .as("Should have no delete files, data contains no null values");
  }

  @Test
  public void testEqualityDeletePlanningStatsAllNullValuesWithNoNullDeletes() throws IOException {
    table
        .newAppend()
        .appendFile(dataFileOnlyNulls) // note that there are only nulls in the data column
        .commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = SCHEMA.select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    deletes.add(delete.copy("data", "d"));

    DeleteFile posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(File.createTempFile("junit", null, temp.toFile())), deletes, deleteRowSchema);

    table.newRowDelta().addDeletes(posDeletes).commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assertions.assertThat(tasks.size()).isEqualTo(1).as("Should produce one task");
    FileScanTask task = tasks.get(0);
    Assertions.assertThat(task.deletes().size())
        .isEqualTo(0)
        .as("Should have no delete files, data contains no null values");
  }

  @Test
  public void testEqualityDeletePlanningStatsSomeNullValuesWithSomeNullDeletes()
      throws IOException {
    table
        .newAppend()
        .appendFile(dataFile) // note that there are some nulls in the data column
        .commit();

    List<Record> deletes = Lists.newArrayList();
    Schema deleteRowSchema = SCHEMA.select("data");
    Record delete = GenericRecord.create(deleteRowSchema);
    // the data and delete ranges do not overlap, but both contain null
    deletes.add(delete.copy("data", null));
    deletes.add(delete.copy("data", "x"));

    DeleteFile posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(File.createTempFile("junit", null, temp.toFile())), deletes, deleteRowSchema);

    table.newRowDelta().addDeletes(posDeletes).commit();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(tasksIterable);
    }

    Assertions.assertThat(tasks.size()).isEqualTo(1).as("Should produce one task");
    FileScanTask task = tasks.get(0);
    Assertions.assertThat(task.deletes().size())
        .isEqualTo(1)
        .as("Should have one delete file, data and deletes have null values");
  }

  @Test
  public void testDifferentDeleteTypes() throws IOException {
    // init the table with an unpartitioned data file
    table.newAppend().appendFile(dataFile).commit();

    // add a matching global equality delete
    DeleteFile globalEqDeleteFile1 = writeEqDeletes("id", 7, 8);
    table.newRowDelta().addDeletes(globalEqDeleteFile1).commit();

    // evolve the spec to partition by category
    table.updateSpec().addField("category").commit();

    StructLike evenPartition = Row.of("even");
    StructLike oddPartition = Row.of("odd");

    // add 2 data files to "even" and "odd" partitions
    DataFile dataFileWithEvenRecords = writeData(evenPartition, evenRecords);
    DataFile dataFileWithOddRecords = writeData(oddPartition, oddRecords);
    table
        .newFastAppend()
        .appendFile(dataFileWithEvenRecords)
        .appendFile(dataFileWithOddRecords)
        .commit();

    // add 2 matching and 1 filterable partition-scoped equality delete files for "even" partition
    DeleteFile partitionEqDeleteFile1 = writeEqDeletes(evenPartition, "id", 2);
    DeleteFile partitionEqDeleteFile2 = writeEqDeletes(evenPartition, "id", 4);
    DeleteFile partitionEqDeleteFile3 = writeEqDeletes(evenPartition, "id", 25);
    table
        .newRowDelta()
        .addDeletes(partitionEqDeleteFile1)
        .addDeletes(partitionEqDeleteFile2)
        .addDeletes(partitionEqDeleteFile3)
        .commit();

    // add 1 matching partition-scoped position delete file for "even" partition
    Pair<DeleteFile, CharSequenceSet> partitionPosDeletes =
        writePosDeletes(
            evenPartition,
            ImmutableList.of(
                Pair.of(dataFileWithEvenRecords.path(), 0L),
                Pair.of("some-other-file.parquet", 0L)));
    table
        .newRowDelta()
        .addDeletes(partitionPosDeletes.first())
        .validateDataFilesExist(partitionPosDeletes.second())
        .commit();

    // add 1 path-scoped position delete file for dataFileWithEvenRecords
    Pair<DeleteFile, CharSequenceSet> pathPosDeletes =
        writePosDeletes(
            evenPartition,
            ImmutableList.of(
                Pair.of(dataFileWithEvenRecords.path(), 1L),
                Pair.of(dataFileWithEvenRecords.path(), 2L)));
    table
        .newRowDelta()
        .addDeletes(pathPosDeletes.first())
        .validateDataFilesExist(pathPosDeletes.second())
        .commit();

    // switch back to the unpartitioned spec
    table.updateSpec().removeField("category").commit();

    // add another global equality delete file that can be filtered using stats
    DeleteFile globalEqDeleteFile2 = writeEqDeletes("id", 20, 21);
    table.newRowDelta().addDeletes(globalEqDeleteFile2);

    List<FileScanTask> tasks = planTasks();

    assertThat(tasks).hasSize(3);

    for (FileScanTask task : tasks) {
      if (coversDataFile(task, dataFile)) {
        assertDeletes(task, globalEqDeleteFile1);

      } else if (coversDataFile(task, dataFileWithEvenRecords)) {
        assertDeletes(
            task,
            partitionEqDeleteFile1,
            partitionEqDeleteFile2,
            pathPosDeletes.first(),
            partitionPosDeletes.first());

      } else if (coversDataFile(task, dataFileWithOddRecords)) {
        assertThat(task.deletes()).isEmpty();

      } else {
        fail("Unexpected task: " + task);
      }
    }
  }

  private boolean coversDataFile(FileScanTask task, DataFile file) {
    return task.file().path().toString().equals(file.path().toString());
  }

  private void assertDeletes(FileScanTask task, DeleteFile... expectedDeleteFiles) {
    CharSequenceSet actualDeletePaths = deletePaths(task);

    assertThat(actualDeletePaths.size()).isEqualTo(expectedDeleteFiles.length);

    for (DeleteFile expectedDeleteFile : expectedDeleteFiles) {
      assertThat(actualDeletePaths.contains(expectedDeleteFile.path())).isTrue();
    }
  }

  private CharSequenceSet deletePaths(FileScanTask task) {
    return CharSequenceSet.of(Iterables.transform(task.deletes(), ContentFile::path));
  }

  private List<FileScanTask> planTasks() throws IOException {
    try (CloseableIterable<FileScanTask> tasksIterable = table.newScan().planFiles()) {
      return Lists.newArrayList(tasksIterable);
    }
  }

  private DataFile writeData(StructLike partition, List<Record> data) throws IOException {
    return FileHelpers.writeDataFile(table, Files.localOutput(File.createTempFile("junit", null, temp.toFile())), partition, data);
  }

  private DeleteFile writeEqDeletes(String col, Object... values) throws IOException {
    return writeEqDeletes(null /* unpartitioned */, col, values);
  }

  private DeleteFile writeEqDeletes(StructLike partition, String col, Object... values)
      throws IOException {
    Schema deleteSchema = SCHEMA.select(col);

    Record delete = GenericRecord.create(deleteSchema);
    List<Record> deletes = Lists.newArrayList();
    for (Object value : values) {
      deletes.add(delete.copy(col, value));
    }

    OutputFile out = Files.localOutput(File.createTempFile("junit", null, temp.toFile()));
    return FileHelpers.writeDeleteFile(table, out, partition, deletes, deleteSchema);
  }

  private Pair<DeleteFile, CharSequenceSet> writePosDeletes(
      StructLike partition, List<Pair<CharSequence, Long>> deletes) throws IOException {
    OutputFile out = Files.localOutput(File.createTempFile("junit", null, temp.toFile()));
    return FileHelpers.writeDeleteFile(table, out, partition, deletes);
  }
}
