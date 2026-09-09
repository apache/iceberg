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
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestBaseDeleteLoader {
  private static final int KEY_COUNT = 128;
  private static final Types.StructType KEY_TYPE =
      Types.StructType.of(
          required(3, "id", Types.LongType.get()),
          required(4, "day", Types.DateType.get()),
          required(5, "ts", Types.TimestampType.withoutZone()),
          required(6, "bytes", Types.BinaryType.get()));
  private static final Types.StructType PARENT_TYPE =
      Types.StructType.of(optional(2, "key", KEY_TYPE));
  private static final Schema SCHEMA = new Schema(optional(1, "parent", PARENT_TYPE));

  @TempDir private Path temp;

  static Stream<Arguments> formatsAndCaching() {
    return Stream.of(FileFormat.AVRO, FileFormat.PARQUET, FileFormat.ORC)
        .flatMap(format -> Stream.of(Arguments.of(format, false), Arguments.of(format, true)));
  }

  @ParameterizedTest
  @MethodSource("formatsAndCaching")
  void nestedDeleteKeysRemainStable(FileFormat format, boolean cached) throws Exception {
    List<Record> records = Lists.newArrayList();
    for (int i = 0; i < KEY_COUNT; i++) {
      records.add(row(i * 2L));
    }
    records.add(GenericRecord.create(SCHEMA));
    DeleteFile file = writeDeletes(format, records);
    Map<String, Object> cache = Maps.newConcurrentMap();
    StructLikeSet first =
        loader(file, cached, cache).loadEqualityDeletes(Collections.singletonList(file), SCHEMA);
    assertThat(first).hasSize(KEY_COUNT + 1);
    checkKeys(first);

    // Holding a nested key must not expose a mutable view of a different cached row.
    Iterator<StructLike> rows = first.iterator();
    StructLike heldKey = null;
    Long heldId = null;
    while (rows.hasNext()) {
      StructLike parent = rows.next().get(0, StructLike.class);
      if (parent != null) {
        StructLike key = parent.get(0, StructLike.class);
        if (heldKey == null) {
          heldKey = key;
          heldId = key.get(0, Long.class);
        } else {
          assertThat(heldKey.get(0, Long.class)).isEqualTo(heldId);
        }
      }
    }

    ExecutorService threads = Executors.newFixedThreadPool(8);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> futures = Lists.newArrayList();
    try {
      for (int i = 0; i < 8; i++) {
        futures.add(
            threads.submit(
                () -> {
                  assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                  for (int round = 0; round < 10; round++) {
                    StructLikeSet keys =
                        loader(file, cached, cache)
                            .loadEqualityDeletes(Collections.singletonList(file), SCHEMA);
                    assertThat(keys).hasSize(KEY_COUNT + 1);
                    checkKeys(keys);
                  }
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> future : futures) {
        future.get(60, TimeUnit.SECONDS);
      }
    } finally {
      start.countDown();
      threads.shutdownNow();
    }
  }

  @ParameterizedTest
  @MethodSource("formatsAndCaching")
  void nestedKeysWithHashCollisionsRemainDistinct(FileFormat format, boolean cached)
      throws Exception {
    Record first = row(1L);
    Record second = row(1L);
    second.get(0, Record.class).get(0, Record.class).setField("id", 1L << 32);
    assertThat(Long.hashCode(1L)).isEqualTo(Long.hashCode(1L << 32));
    DeleteFile file = writeDeletes(format, List.of(first, second));
    StructLikeSet keys =
        loader(file, cached, Maps.newConcurrentMap())
            .loadEqualityDeletes(Collections.singletonList(file), SCHEMA);
    assertThat(keys).hasSize(2);
    InternalRecordWrapper wrapper = new InternalRecordWrapper(SCHEMA.asStruct());
    assertThat(keys.contains(wrapper.wrap(first))).isTrue();
    assertThat(keys.contains(wrapper.wrap(second))).isTrue();
    Record absent = row(1L);
    absent.get(0, Record.class).get(0, Record.class).setField("id", (2L << 32) | 3L);
    assertThat(keys.contains(wrapper.wrap(absent))).isFalse();
  }

  private DeleteFile writeDeletes(FileFormat format, List<Record> rows) throws Exception {
    EqualityDeleteWriter<Record> writer =
        FormatModelRegistry.equalityDeleteWriteBuilder(
                format,
                Record.class,
                EncryptedFiles.plainAsEncryptedOutput(
                    Files.localOutput(temp.resolve(format.addExtension("deletes")).toFile())))
            .schema(SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(3, 4, 5, 6)
            .build();
    try (EqualityDeleteWriter<Record> closeableWriter = writer) {
      for (Record row : rows) {
        closeableWriter.write(row);
      }
    }
    return writer.toDeleteFile();
  }

  private static void checkKeys(StructLikeSet keys) {
    InternalRecordWrapper wrapper = new InternalRecordWrapper(SCHEMA.asStruct());
    for (int i = 0; i < KEY_COUNT * 2; i++) {
      assertThat(keys.contains(wrapper.wrap(row(i)))).as("delete key %s", i).isEqualTo(i % 2 == 0);
    }
    assertThat(keys.contains(wrapper.wrap(GenericRecord.create(SCHEMA)))).isTrue();
  }

  private static Record row(long id) {
    Record key = GenericRecord.create(KEY_TYPE);
    key.setField("id", id);
    key.setField("day", LocalDate.of(2020, 1, 1).plusDays(id));
    key.setField("ts", LocalDateTime.of(2020, 1, 1, 0, 0).plusNanos(id * 1000));
    key.setField("bytes", ByteBuffer.allocate(Long.BYTES).putLong(0, id));
    Record parent = GenericRecord.create(PARENT_TYPE);
    parent.setField("key", key);
    Record row = GenericRecord.create(SCHEMA);
    row.setField("parent", parent);
    return row;
  }

  private static BaseDeleteLoader loader(
      DeleteFile file, boolean cached, Map<String, Object> cache) {
    return new BaseDeleteLoader(ignored -> Files.localInput(file.location())) {
      @Override
      protected boolean canCache(long size) {
        return cached;
      }

      @Override
      @SuppressWarnings("unchecked")
      protected <V> V getOrLoad(String key, Supplier<V> supplier, long size) {
        return (V) cache.computeIfAbsent(key, ignored -> supplier.get());
      }
    };
  }
}
