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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/** Test providers, registered in META-INF/services and used only when selected by name. */
public class TestReaderFactoryProviders {

  private TestReaderFactoryProviders() {}

  /**
   * Passes every partition to the delegate and counts them, checking that each is an {@link
   * IcebergInputPartition} of file scan tasks.
   */
  public static class Counting implements PartitionReaderFactoryProvider {
    static final String NAME = "test-counting";
    static final AtomicInteger FACTORIES = new AtomicInteger();
    static final AtomicInteger ROW_READERS = new AtomicInteger();
    static final AtomicInteger COLUMNAR_READERS = new AtomicInteger();
    static final AtomicInteger DELETE_FILES = new AtomicInteger();

    static void reset() {
      FACTORIES.set(0);
      ROW_READERS.set(0);
      COLUMNAR_READERS.set(0);
      DELETE_FILES.set(0);
    }

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public PartitionReaderFactory create(Table table, PartitionReaderFactory delegate) {
      FACTORIES.incrementAndGet();
      return new CountingFactory(delegate);
    }
  }

  /** Leaves every scan to Iceberg. */
  public static class Declining implements PartitionReaderFactoryProvider {
    static final String NAME = "test-declining";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public PartitionReaderFactory create(Table table, PartitionReaderFactory delegate) {
      return delegate;
    }
  }

  private static class CountingFactory implements PartitionReaderFactory {
    private final PartitionReaderFactory delegate;

    CountingFactory(PartitionReaderFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      Counting.ROW_READERS.incrementAndGet();
      inspect(partition);
      return delegate.createReader(partition);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      Counting.COLUMNAR_READERS.incrementAndGet();
      inspect(partition);
      return delegate.createColumnarReader(partition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return delegate.supportColumnarReads(partition);
    }

    private static void inspect(InputPartition partition) {
      IcebergInputPartition iceberg = (IcebergInputPartition) partition;
      if (!iceberg.allTasksOfType(FileScanTask.class) || iceberg.projection() == null) {
        throw new IllegalStateException("Unexpected partition: " + partition);
      }

      iceberg.<FileScanTask>taskGroup().tasks().stream()
          .mapToInt(task -> task.deletes().size())
          .forEach(Counting.DELETE_FILES::addAndGet);
    }
  }
}
