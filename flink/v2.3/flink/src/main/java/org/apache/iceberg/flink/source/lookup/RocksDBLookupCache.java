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
package org.apache.iceberg.flink.source.lookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
class RocksDBLookupCache implements IcebergLookupCache {

  private static final Logger LOG = LoggerFactory.getLogger(RocksDBLookupCache.class);
  private static final int KEY_LENGTH_BYTES = Integer.BYTES;
  private static final int INITIAL_BUFFER_SIZE = 64;
  private static final int WRITE_BATCH_SIZE = 10_000;

  private final RocksDB db;
  private final Options dbOptions;
  private final ReadOptions readOptions;
  private final WriteOptions writeOptions;
  private final WriteBatch writeBatch;
  private final RowDataSerializer keySerializer;
  private final RowDataSerializer rowSerializer;
  private final DataOutputSerializer output = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
  private final DataInputDeserializer input = new DataInputDeserializer();
  private final Path directory;

  private long sequence = 0L;
  private int pendingWrites = 0;

  private RocksDBLookupCache(
      RocksDB db, Options dbOptions, Path directory, RowType keyType, RowType rowType) {
    this.db = db;
    this.dbOptions = dbOptions;
    this.directory = directory;
    this.readOptions = new ReadOptions();
    this.writeOptions = new WriteOptions();
    this.writeBatch = new WriteBatch();
    this.keySerializer = InternalSerializers.create(keyType);
    this.rowSerializer = InternalSerializers.create(rowType);
  }

  static RocksDBLookupCache create(Path directory, RowType keyType, RowType rowType)
      throws IOException {
    Options dbOptions =
        new Options().setCreateIfMissing(true).setCompressionType(CompressionType.LZ4_COMPRESSION);
    try {
      Files.createDirectories(directory);
      RocksDB db = RocksDB.open(dbOptions, directory.toString());
      return new RocksDBLookupCache(db, dbOptions, directory, keyType, rowType);
    } catch (RocksDBException e) {
      dbOptions.close();
      throw new IOException("Failed to open RocksDB lookup cache in " + directory, e);
    }
  }

  @Override
  public @Nullable List<RowData> get(RowData key) {
    byte[] prefix = keyPrefix(key);
    List<RowData> rows = Lists.newArrayList();
    try (RocksIterator iterator = db.newIterator(readOptions)) {
      iterator.seek(prefix);
      while (iterator.isValid() && startsWith(iterator.key(), prefix)) {
        rows.add(deserialize(iterator.value()));
        iterator.next();
      }
    }

    return rows.isEmpty() ? null : rows;
  }

  @Override
  public void add(RowData key, RowData row) {
    byte[] prefix = keyPrefix(key);
    byte[] fullKey = Arrays.copyOf(prefix, prefix.length + Long.BYTES);
    ByteBuffer.wrap(fullKey, prefix.length, Long.BYTES).putLong(sequence++);

    try {
      writeBatch.put(fullKey, serialize(rowSerializer, row));
    } catch (RocksDBException e) {
      throw new UncheckedIOException(
          "Failed to add row to RocksDB lookup cache", new IOException(e));
    }

    if (++pendingWrites >= WRITE_BATCH_SIZE) {
      flush();
    }
  }

  @Override
  public void completeLoad() {
    flush();
  }

  @Override
  public void close() {
    try {
      flush();
    } catch (RuntimeException e) {
      LOG.warn("Failed to flush RocksDB lookup cache before closing", e);
    }

    writeBatch.close();
    readOptions.close();
    writeOptions.close();
    db.close();
    dbOptions.close();
    deleteDirectory();
  }

  private void flush() {
    if (pendingWrites == 0) {
      return;
    }

    try {
      db.write(writeOptions, writeBatch);
    } catch (RocksDBException e) {
      throw new UncheckedIOException("Failed to flush RocksDB lookup cache", new IOException(e));
    } finally {
      writeBatch.clear();
      pendingWrites = 0;
    }
  }

  private byte[] keyPrefix(RowData key) {
    byte[] keyBytes = serialize(keySerializer, key);
    byte[] prefix = new byte[KEY_LENGTH_BYTES + keyBytes.length];
    ByteBuffer.wrap(prefix).putInt(keyBytes.length).put(keyBytes);
    return prefix;
  }

  private byte[] serialize(RowDataSerializer serializer, RowData row) {
    output.clear();
    try {
      serializer.serialize(row, output);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize lookup row", e);
    }

    return output.getCopyOfBuffer();
  }

  private RowData deserialize(byte[] value) {
    input.setBuffer(value);
    try {
      return rowSerializer.deserialize(input);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to deserialize lookup row", e);
    }
  }

  private static boolean startsWith(byte[] key, byte[] prefix) {
    if (key.length < prefix.length) {
      return false;
    }

    for (int i = 0; i < prefix.length; i++) {
      if (key[i] != prefix[i]) {
        return false;
      }
    }

    return true;
  }

  private void deleteDirectory() {
    if (!Files.exists(directory)) {
      return;
    }

    try {
      Files.walkFileTree(
          directory,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              Files.deleteIfExists(file);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
              if (exc != null) {
                throw exc;
              }

              // Children have already been deleted, so the directory is empty now.
              Files.deleteIfExists(dir);
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      LOG.warn("Failed to delete RocksDB lookup cache directory {}", directory, e);
    }
  }
}
