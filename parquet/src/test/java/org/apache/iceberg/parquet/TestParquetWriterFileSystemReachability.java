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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for #16640: when the Hadoop FileSystem cache is disabled, the FileSystem resolved
 * for a Parquet write must stay reachable for the writer's lifetime. Otherwise it can be
 * garbage-collected mid-write and its finalizer shuts down the thread pool backing the open stream
 * (observed with {@code AzureBlobFileSystem}), failing the write.
 */
class TestParquetWriterFileSystemReachability {

  private static final Schema SCHEMA = new Schema(optional(1, "intCol", Types.IntegerType.get()));

  @TempDir private File tempDir;

  @BeforeEach
  void clearTrackedFileSystems() {
    TrackingFileSystem.INSTANCES.clear();
  }

  @Test
  void writerKeepsWriteFileSystemReachable() throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", TrackingFileSystem.class, FileSystem.class);
    // Reproduce the reported configuration: with the cache disabled, every FileSystem.get(...)
    // returns a fresh instance that has no shared strong referrer.
    conf.setBoolean("fs.file.impl.disable.cache", true);

    File target = new File(tempDir, UUID.randomUUID() + ".parquet").getAbsoluteFile();
    OutputFile output = HadoopOutputFile.fromLocation(target.toString(), conf);

    GenericData.Record record = new GenericData.Record(AvroSchemaUtil.convert(SCHEMA.asStruct()));
    record.put("intCol", 42);

    FileAppender<GenericData.Record> writer =
        Parquet.write(output)
            .schema(SCHEMA)
            .createWriterFunc(ParquetAvroWriter::buildWriter)
            .build();
    writer.add(record);
    // close() lazily initializes the underlying ParquetFileWriter, which resolves the
    // Parquet-native
    // write FileSystem, and flushes the footer - the operation that fails in the bug.
    writer.close();

    // The write resolves two FileSystems when the cache is disabled: one for Iceberg's
    // HadoopOutputFile and a separate one for the Parquet-native HadoopOutputFile that streams the
    // bytes. Both are TrackingFileSystem instances.
    assertThat(TrackingFileSystem.INSTANCES.size())
        .as("the write should resolve at least two FileSystem instances with the cache disabled")
        .isGreaterThanOrEqualTo(2);

    for (int i = 0; i < 10; i++) {
      System.gc();
    }

    // While the writer and its output file are still referenced, none of the FileSystems resolved
    // for the write may be collected. In buggy code the Parquet write FileSystem has no strong
    // referrer once ParquetFileWriter is built, so GC reclaims it here; the fix keeps it reachable
    // through ParquetWriter#parquetOutputFile.
    long reachable = TrackingFileSystem.INSTANCES.stream().filter(ref -> ref.get() != null).count();
    assertThat(reachable)
        .as("every FileSystem resolved for the write must stay reachable while the writer is open")
        .isEqualTo(TrackingFileSystem.INSTANCES.size());

    // Keep writer and output strongly reachable through the assertion above so the JIT cannot treat
    // them (and the FileSystems they transitively hold) as unreachable early.
    Reference.reachabilityFence(writer);
    Reference.reachabilityFence(output);

    // Sanity check: once the writer and its output file are dropped, the FileSystems become
    // collectible. This proves the WeakReference plus GC mechanism observes collection in this JVM,
    // so the assertion above is a meaningful regression guard rather than vacuous.
    writer = null;
    output = null;
    boolean allCollected = false;
    for (int i = 0; i < 50 && !allCollected; i++) {
      System.gc();
      allCollected = TrackingFileSystem.INSTANCES.stream().allMatch(ref -> ref.get() == null);
      if (!allCollected) {
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    assertThat(allCollected)
        .as("FileSystems should be collectible once the writer and output file are dropped")
        .isTrue();
  }

  /** A local FileSystem that records every instance it creates, for reachability assertions. */
  static class TrackingFileSystem extends RawLocalFileSystem {
    static final List<WeakReference<FileSystem>> INSTANCES = Lists.newCopyOnWriteArrayList();

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
      // Return a stream that does NOT reference this FileSystem, unlike RawLocalFileSystem's
      // inner-class LocalFSFileOutputStream. This mirrors cloud output streams (such as
      // AbfsOutputStream) that hold their backing store rather than the FileSystem object, which is
      // what allows the FileSystem to be garbage-collected mid-write when the cache is disabled.
      return new FSDataOutputStream(new FileOutputStream(file), null);
    }
  }
}
