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
package org.apache.iceberg.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end regression test for #16640 against a real Avro write path.
 *
 * <p>It writes an Avro position-delete file through {@link PositionDeleteWriter}, which - like
 * {@code DataWriter} and the other data/delete writers - retains the {@link
 * org.apache.iceberg.io.FileAppender} and a location string, but NOT the {@link OutputFile}. So
 * once the caller drops its own reference, the appender is the only thing that can keep the write's
 * Hadoop {@code FileSystem} reachable.
 *
 * <p>{@link TrackingFileSystem} mimics {@code AzureBlobFileSystem}: its {@code finalize()} shuts
 * down a thread pool that the open output stream submits to, and that stream (like {@code
 * AbfsOutputStream}) references the pool rather than the FileSystem. With the Hadoop FileSystem
 * cache disabled, if nothing keeps the FileSystem reachable it is garbage-collected mid-write,
 * {@code finalize()} terminates the pool, and the next write fails - the exact symptom reported in
 * #16640. {@code AvroFileAppender} retaining its {@link OutputFile} keeps the FileSystem reachable,
 * so the write completes.
 */
class TestAvroWriteFileSystemReachability {

  @TempDir private File tempDir;

  @BeforeEach
  void clearTrackedFileSystems() {
    TrackingFileSystem.INSTANCES.clear();
  }

  @Test
  void positionDeleteWriteSucceedsWhenFileSystemCacheDisabled() throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", TrackingFileSystem.class, FileSystem.class);
    // Reproduce the reported configuration: with the cache disabled, every FileSystem.get(...)
    // returns a fresh instance that has no shared strong referrer.
    conf.setBoolean("fs.file.impl.disable.cache", true);

    File target = new File(tempDir, UUID.randomUUID() + ".avro").getAbsoluteFile();
    OutputFile output = HadoopOutputFile.fromLocation(target.toString(), conf);

    PositionDeleteWriter<Object> writer =
        Avro.writeDeletes(output).withSpec(PartitionSpec.unpartitioned()).buildPositionWriter();

    // The single FileSystem resolved for this write. PositionDeleteWriter keeps only the appender
    // and a location string, so dropping our own reference leaves the appender as its only holder.
    assertThat(TrackingFileSystem.INSTANCES).hasSize(1);
    WeakReference<FileSystem> writeFileSystem = TrackingFileSystem.INSTANCES.get(0);
    output = null;

    PositionDelete<Object> positionDelete = PositionDelete.create();
    writer.write(positionDelete.set("/data/file-0.parquet", 0L));

    // Force the unreferenced write FileSystem to be collected and finalized before the flush.
    // On fixed code the appender retains the OutputFile, so it stays reachable and the loop just
    // exhausts; on buggy code it is collected and its finalize() terminates the stream's pool.
    for (int i = 0; i < 30 && writeFileSystem.get() != null; i++) {
      System.gc();
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    writer.write(positionDelete.set("/data/file-0.parquet", 1L));
    // Flushes the Avro file to the stream. On buggy code the FileSystem was finalized above and its
    // pool is terminated, so this fails with the simulated "thread pool was terminated" error - the
    // #16640 symptom. On fixed code the FileSystem is still alive, so the write completes.
    writer.close();

    assertThat(target).exists();
    Reference.reachabilityFence(writer);
  }

  /**
   * A local FileSystem that mimics {@code AzureBlobFileSystem}: it owns a "thread pool" that its
   * {@code finalize()} terminates, and its output streams depend on that pool but do not reference
   * the FileSystem itself.
   */
  static class TrackingFileSystem extends RawLocalFileSystem {
    static final List<WeakReference<FileSystem>> INSTANCES = Lists.newCopyOnWriteArrayList();
    private final AtomicBoolean poolTerminated = new AtomicBoolean(false);

    TrackingFileSystem() {
      INSTANCES.add(new WeakReference<>(this));
    }

    @Override
    public FSDataOutputStream create(
        Path f,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      File file = pathToFile(f);
      File parent = file.getParentFile();
      if (parent != null && !parent.exists() && !parent.mkdirs()) {
        throw new IOException("Failed to create parent directory: " + parent);
      }
      // The stream holds only the shared pool flag, not this FileSystem; it is a static class so
      // it cannot capture this FileSystem, which would keep it reachable and mask the bug.
      return new FSDataOutputStream(
          new PoolBackedOutputStream(new FileOutputStream(file), poolTerminated), null);
    }

    @SuppressWarnings({"checkstyle:NoFinalizer", "Finalize", "deprecation", "removal"})
    @Override
    protected void finalize() throws Throwable {
      // Mimics AzureBlobFileSystem.finalize(): collecting the FileSystem terminates the thread pool
      // that its open output streams depend on.
      poolTerminated.set(true);
      super.finalize();
    }

    /** Output stream that fails once the FileSystem's pool has been terminated. */
    private static class PoolBackedOutputStream extends OutputStream {
      private final OutputStream delegate;
      private final AtomicBoolean poolTerminated;

      private PoolBackedOutputStream(OutputStream delegate, AtomicBoolean poolTerminated) {
        this.delegate = delegate;
        this.poolTerminated = poolTerminated;
      }

      private void checkPool() throws IOException {
        if (poolTerminated.get()) {
          throw new IOException(
              "Could not submit task to executor: thread pool was terminated (simulated #16640)");
        }
      }

      @Override
      public void write(int b) throws IOException {
        checkPool();
        delegate.write(b);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        checkPool();
        delegate.write(b, off, len);
      }

      @Override
      public void flush() throws IOException {
        checkPool();
        delegate.flush();
      }

      @Override
      public void close() throws IOException {
        checkPool();
        delegate.close();
      }
    }
  }
}
