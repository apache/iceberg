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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

public abstract class TestSparkBaseDataReader {

  private static final Configuration CONF = new Configuration();

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

  // Provides and keeps track of task and data iterator
  // Tracking allows query whether the iterator has been closed in the end
  private static class CloseableIteratorProvider {
    private Map<String, CloseableIntegerRange> closureTrack;

    CloseableIteratorProvider() {
      this.closureTrack = new HashMap<>();
    }

    public CloseableIntegerRange get(FileScanTask task) {
      CloseableIntegerRange intRange = new CloseableIntegerRange(task.file().recordCount());
      closureTrack.put(getKey(task), intRange);
      return intRange;
    }

    private String getKey(FileScanTask task) {
      return task.file().path().toString();
    }

    public Boolean isIteratorClosed(FileScanTask task) {
      return closureTrack.get(getKey(task)).closed;
    }

    public Boolean hasIterator(FileScanTask task) {
      return closureTrack.containsKey(getKey(task));
    }
  }

  // Main test class to test its iteration logic
  private static class ClosureTrackingReader extends BaseDataReader<Integer> {
    private CloseableIteratorProvider iterProvider = new CloseableIteratorProvider();

    ClosureTrackingReader(List<FileScanTask> tasks) {
      super(new BaseCombinedScanTask(tasks),
          new HadoopFileIO(CONF),
          new PlaintextEncryptionManager());
    }

    @Override
    CloseableIterator<Integer> open(FileScanTask task) {
      return iterProvider.get(task);
    }

    public Boolean isIteratorClosed(FileScanTask task) {
      return iterProvider.isIteratorClosed(task);
    }

    public Boolean hasIterator(FileScanTask task) {
      return iterProvider.hasIterator(task);
    }
  }

  // Subclass of `BaseDataReader` mostly care about the FileScanTask as it opens and returns
  // iterable data. Here, we also have dummy test reader, so we only need a data file as
  // a unique identifier.
  private static class MockScanTask implements FileScanTask {
    DataFile file;

    MockScanTask(DataFile file) {
      this.file = file;
    }

    @Override
    public DataFile file() {
      return file;
    }

    @Override
    public PartitionSpec spec() {
      throw new IllegalStateException("Don't expect invocation on mock task");
    }

    @Override
    public long start() {
      throw new IllegalStateException("Don't expect invocation on mock task");
    }

    @Override
    public long length() {
      throw new IllegalStateException("Don't expect invocation on mock task");
    }

    @Override
    public Expression residual() {
      throw new IllegalStateException("Don't expect invocation on mock task");
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      throw new IllegalStateException("Don't expect invocation on mock task");
    }
  }

  @Test
  public void testClosureOnDataExhaustion() throws IOException {
    Integer totalTasks = 100;
    Integer recordPerTask = 9;
    List<FileScanTask> tasks = mockFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(tasks);

    int countRecords = 0;
    while (reader.next()) {
      countRecords += 1;
      Assert.assertNotNull("Reader should return non-null value", reader.get());
    }

    Assert.assertEquals("Reader returned incorrect number of records",
        totalTasks * recordPerTask,
        countRecords
    );
    tasks.forEach(t ->
        Assert.assertTrue("All iterators should be closed after read exhausion",
            reader.isIteratorClosed(t))
    );
  }

  @Test
  public void testClosureDuringIteration() throws IOException {
    Integer totalTasks = 2;
    Integer recordPerTask = 1;
    List<FileScanTask> tasks = mockFileScanTasks(totalTasks, recordPerTask);
    Assert.assertEquals(2, tasks.size());
    FileScanTask firstTask = tasks.get(0);
    FileScanTask secondTask = tasks.get(1);

    ClosureTrackingReader reader = new ClosureTrackingReader(tasks);

    // Total of 2 elements
    Assert.assertTrue(reader.next());
    Assert.assertFalse("First iter should not be closed on its last element",
        reader.isIteratorClosed(firstTask));

    Assert.assertTrue(reader.next());
    Assert.assertTrue("First iter should be closed after moving to second iter",
        reader.isIteratorClosed(firstTask));
    Assert.assertFalse("Second iter should not be closed on its last element",
        reader.isIteratorClosed(secondTask));

    Assert.assertFalse(reader.next());
    Assert.assertTrue(reader.isIteratorClosed(firstTask));
    Assert.assertTrue(reader.isIteratorClosed(secondTask));
  }

  @Test
  public void testClosureWithoutAnyRead() throws IOException {
    Integer totalTasks = 100;
    Integer recordPerTask = 100;
    List<FileScanTask> tasks = mockFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(tasks);

    reader.close();

    tasks.forEach(t ->
        Assert.assertFalse("Iterator should not be created eagerly for tasks",
            reader.hasIterator(t))
    );
  }

  @Test
  public void testExplicitClosure() throws IOException {
    Integer totalTasks = 100;
    Integer recordPerTask = 9;
    List<FileScanTask> tasks = mockFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(tasks);

    Integer halfDataSize = (totalTasks * recordPerTask) / 2;
    for (int i = 0; i < halfDataSize; i++) {
      Assert.assertTrue("Reader should have some element", reader.next());
      Assert.assertNotNull("Reader should return non-null value", reader.get());
    }

    reader.close();

    // Some tasks might have not been opened yet, so we don't have corresponding tracker for it.
    // But all that have been created must be closed.
    tasks.forEach(t -> {
      if (reader.hasIterator(t)) {
        Assert.assertTrue("Iterator should be closed after read exhausion",
            reader.isIteratorClosed(t));
      }
    });
  }

  @Test
  public void testIdempotentExplicitClosure() throws IOException {
    Integer totalTasks = 10;
    Integer recordPerTask = 10;
    List<FileScanTask> tasks = mockFileScanTasks(totalTasks, recordPerTask);

    ClosureTrackingReader reader = new ClosureTrackingReader(tasks);

    // Total 100 elements, only 5 iterators have been created
    for (int i = 0; i < 45; i++) {
      Assert.assertTrue("eader should have some element", reader.next());
      Assert.assertNotNull("Reader should return non-null value", reader.get());
    }

    for (int closeAttempt = 0; closeAttempt < 5; closeAttempt++) {
      reader.close();
      for (int i = 0; i < 5; i++) {
        Assert.assertTrue("Iterator should be closed after read exhausion",
            reader.isIteratorClosed(tasks.get(i)));
      }
      for (int i = 5; i < 10; i++) {
        Assert.assertFalse("Iterator should not be created eagerly for tasks",
            reader.hasIterator(tasks.get(i)));
      }
    }
  }

  private List<FileScanTask> mockFileScanTasks(Integer totalTasks, Integer recordPerTask) {
    return IntStream.range(0, totalTasks).mapToObj(i ->
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()))
            .withFileSizeInBytes(123)
            .withRecordCount(recordPerTask)
            .build())
        .map(MockScanTask::new)
        .collect(Collectors.toList());
  }
}
