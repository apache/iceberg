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

import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.Files.localOutput;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestBaseReader {

  @TempDir
  private Path temp;

  private Table table;

  // Simulates the closeable iterator of data to be read
  private static class CloseableIntegerRange implements CloseableIterator<Integer> {
    boolean closed;
    Iterator<Integer> iter;

    CloseableIntegerRange(long range) {
      this.closed = false;
      this.iter = IntStream.range(0, (int) range).iterator();
    }

    @Override
    public void close() {
      this.closed = true;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public Integer next() {
      return iter.next();
    }
  }

  // Main reader class to test base class iteration logic.
  // Keeps track of iterator closure.
  private static class ClosureTrackingReader extends BaseReader<Integer, FileScanTask> {
    private Map<String, CloseableIntegerRange> tracker = Maps.newHashMap();

    ClosureTrackingReader(Table table, List<FileScanTask> tasks) {
      super(table, new BaseCombinedScanTask(tasks), null, null, false);
    }

    @Override
    protected Stream<ContentFile<?>> referencedFiles(FileScanTask task) {
      return Stream.of();
    }

    @Override
    protected CloseableIterator<Integer> open(FileScanTask task) {
      CloseableIntegerRange intRange = new CloseableIntegerRange(task.file().recordCount());
      tracker.put(getKey(task), intRange);
      return intRange;
    }

    public Boolean isIteratorClosed(FileScanTask task) {
      return tracker.get(getKey(task)).closed;
    }

    public Boolean hasIterator(FileScanTask task) {
      return tracker.containsKey(getKey(task));
    }

    private String getKey(FileScanTask task) {
      return task.file().path().toString();
    }
  }

  @Test
  public void testClosureOnDataExhaustion() throws IOException {
    Integer totalTasks = 10;
    Integer recordPerTask = 10;
    List<FileScanTask> tasks = createFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(table, tasks);

    int countRecords = 0;
    while (reader.next()) {
      countRecords += 1;
      assertThat(reader.get()).as("Reader should return non-null value").isNotNull();
    }

    assertThat(totalTasks * recordPerTask).as("Reader returned incorrect number of records").isEqualTo(countRecords);
    tasks.forEach(
        t ->
            assertThat(reader.isIteratorClosed(t)).as("All iterators should be closed after read exhausion").isTrue());
  }

  @Test
  public void testClosureDuringIteration() throws IOException {
    Integer totalTasks = 2;
    Integer recordPerTask = 1;
    List<FileScanTask> tasks = createFileScanTasks(totalTasks, recordPerTask);
    assertThat(tasks).hasSize(2);
    FileScanTask firstTask = tasks.get(0);
    FileScanTask secondTask = tasks.get(1);

    ClosureTrackingReader reader = new ClosureTrackingReader(table, tasks);

    // Total of 2 elements
    assertThat(reader.next()).isTrue();
    assertThat(reader.isIteratorClosed(firstTask)).as("First iter should not be closed on its last element").isFalse();

    assertThat(reader.next()).isTrue();
    assertThat(reader.isIteratorClosed(firstTask)).as("First iter should be closed after moving to second iter").isTrue();
    assertThat(reader.isIteratorClosed(secondTask)).as("Second iter should not be closed on its last element").isFalse();

    assertThat(reader.next()).isFalse();
    assertThat(reader.isIteratorClosed(firstTask)).isTrue();
    assertThat(reader.isIteratorClosed(secondTask)).isTrue();
  }

  @Test
  public void testClosureWithoutAnyRead() throws IOException {
    Integer totalTasks = 10;
    Integer recordPerTask = 10;
    List<FileScanTask> tasks = createFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(table, tasks);

    reader.close();

    tasks.forEach(
        t ->
            assertThat(reader.hasIterator(t)).as("Iterator should not be created eagerly for tasks").isFalse());
  }

  @Test
  public void testExplicitClosure() throws IOException {
    Integer totalTasks = 10;
    Integer recordPerTask = 10;
    List<FileScanTask> tasks = createFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(table, tasks);

    Integer halfDataSize = (totalTasks * recordPerTask) / 2;
    for (int i = 0; i < halfDataSize; i++) {
      assertThat(reader.next()).as("Reader should have some element").isTrue();
      assertThat(reader.get()).as("Reader should return non-null value").isNotNull();
    }

    reader.close();

    // Some tasks might have not been opened yet, so we don't have corresponding tracker for it.
    // But all that have been created must be closed.
    tasks.forEach(
        t -> {
          if (reader.hasIterator(t)) {
            assertThat(reader.isIteratorClosed(t)).as("Iterator should be closed after read exhausion").isTrue();
          }
        });
  }

  @Test
  public void testIdempotentExplicitClosure() throws IOException {
    Integer totalTasks = 10;
    Integer recordPerTask = 10;
    List<FileScanTask> tasks = createFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(table, tasks);

    // Total 100 elements, only 5 iterators have been created
    for (int i = 0; i < 45; i++) {
      assertThat(reader.next()).as("Reader should have some element").isTrue();
      assertThat(reader.get()).as("Reader should return non-null value").isNotNull();
    }

    for (int closeAttempt = 0; closeAttempt < 5; closeAttempt++) {
      reader.close();
      for (int i = 0; i < 5; i++) {
        assertThat(reader.isIteratorClosed(tasks.get(i))).as("Iterator should be closed after read exhausion").isTrue();
      }
      for (int i = 5; i < 10; i++) {
        assertThat(reader.hasIterator(tasks.get(i))).as("Iterator should not be created eagerly for tasks").isFalse();
      }
    }
  }

  private List<FileScanTask> createFileScanTasks(Integer totalTasks, Integer recordPerTask)
      throws IOException {
    String desc = "make_scan_tasks";
    File parent = temp.resolve(desc).toFile();
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    assertThat(dataFolder.mkdirs()).as("mkdirs should succeed").isTrue();

    Schema schema = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    try {
      this.table = TestTables.create(location, desc, schema, PartitionSpec.unpartitioned());
      // Important: use the table's schema for the rest of the test
      // When tables are created, the column ids are reassigned.
      Schema tableSchema = table.schema();
      List<GenericData.Record> expected = RandomData.generateList(tableSchema, recordPerTask, 1L);

      AppendFiles appendFiles = table.newAppend();
      for (int i = 0; i < totalTasks; i++) {
        File parquetFile = new File(dataFolder, PARQUET.addExtension(UUID.randomUUID().toString()));
        try (FileAppender<GenericData.Record> writer =
            Parquet.write(localOutput(parquetFile)).schema(tableSchema).build()) {
          writer.addAll(expected);
        }
        DataFile file =
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withFileSizeInBytes(parquetFile.length())
                .withPath(parquetFile.toString())
                .withRecordCount(recordPerTask)
                .build();
        appendFiles.appendFile(file);
      }
      appendFiles.commit();

      return StreamSupport.stream(table.newScan().planFiles().spliterator(), false)
          .collect(Collectors.toList());
    } finally {
      TestTables.clearTables();
    }
  }
}
