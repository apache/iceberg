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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The merged equality delete set of {@link BaseDeleteLoader} is built once per group of files. */
public class TestBaseDeleteLoaderEqualityDeleteCache {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tableDir;
  @TempDir private File deleteDir;

  private Table table;
  private Schema idSchema;

  @BeforeEach
  public void createTable() {
    this.table = TestTables.create(tableDir, "eq_cache", SCHEMA, PartitionSpec.unpartitioned(), 2);
    this.idSchema = table.schema().select("id");
  }

  @AfterEach
  public void dropTable() {
    TestTables.clearTables();
  }

  @Test
  public void mergedSetIsBuiltOncePerGroupOfFiles() throws IOException {
    DeleteFile first = eqDeletes("first", 1L, 2L, 3L);
    DeleteFile second = eqDeletes("second", 3L, 4L);
    CountingLoader loader = new CountingLoader(true);

    StructLikeSet set1 = loader.loadEqualityDeletes(ImmutableList.of(first, second), idSchema);
    StructLikeSet set2 = loader.loadEqualityDeletes(ImmutableList.of(second, first), idSchema);

    assertThat(set2).as("same files in another order share the cached set").isSameAs(set1);
    assertThat(ids(set1)).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
    assertThat(loader.mergedLoads()).as("merged sets built").isEqualTo(1);
    assertThat(loader.fileLoads()).as("files read directly for the merged set").isZero();
  }

  @Test
  public void differentFilesGetTheirOwnSet() throws IOException {
    DeleteFile first = eqDeletes("first", 1L, 2L);
    DeleteFile second = eqDeletes("second", 3L);
    CountingLoader loader = new CountingLoader(true);

    StructLikeSet both = loader.loadEqualityDeletes(ImmutableList.of(first, second), idSchema);
    StructLikeSet onlyFirst = loader.loadEqualityDeletes(ImmutableList.of(first), idSchema);

    assertThat(onlyFirst).isNotSameAs(both);
    assertThat(ids(both)).containsExactlyInAnyOrder(1L, 2L, 3L);
    assertThat(ids(onlyFirst)).containsExactlyInAnyOrder(1L, 2L);
    assertThat(loader.mergedLoads()).isEqualTo(2);
    assertThat(loader.fileLoads()).isZero();
  }

  @Test
  public void differentEqualityFieldsGetTheirOwnSet() throws IOException {
    // DeleteFilter groups delete files by equality field ids and asks once per group, each group
    // projected on its own fields: the two groups must not share a cache entry
    Schema dataSchema = table.schema().select("data");
    DeleteFile byId = eqDeletes("by-id", 1L);
    DeleteFile byData = dataDeletes("by-data", "a");
    CountingLoader loader = new CountingLoader(true);

    StructLikeSet idSet = loader.loadEqualityDeletes(ImmutableList.of(byId), idSchema);
    StructLikeSet dataSet = loader.loadEqualityDeletes(ImmutableList.of(byData), dataSchema);

    assertThat(dataSet).isNotSameAs(idSet);
    assertThat(ids(idSet)).containsExactly(1L);
    assertThat(dataSet).hasSize(1);
    assertThat(dataSet.iterator().next().get(0, CharSequence.class).toString()).isEqualTo("a");
    assertThat(loader.mergedLoads()).isEqualTo(2);
  }

  @Test
  public void withoutCachingEveryCallBuildsItsOwnSet() throws IOException {
    DeleteFile first = eqDeletes("first", 1L, 2L);
    DeleteFile second = eqDeletes("second", 3L);
    CountingLoader loader = new CountingLoader(false);

    StructLikeSet set1 = loader.loadEqualityDeletes(ImmutableList.of(first, second), idSchema);
    StructLikeSet set2 = loader.loadEqualityDeletes(ImmutableList.of(first, second), idSchema);

    assertThat(set2).isNotSameAs(set1).isEqualTo(set1);
    assertThat(ids(set1)).containsExactlyInAnyOrder(1L, 2L, 3L);
    assertThat(loader.mergedLoads()).isZero();
    assertThat(loader.fileLoads()).isZero();
  }

  @Test
  public void mergedSetTooLargeFallsBackToPerFileEntries() throws IOException {
    DeleteFile first = eqDeletes("first", 1L, 2L);
    DeleteFile second = eqDeletes("second", 3L);
    // each file fits the cache, the merged set of both does not
    long oneFile = Math.max(first.recordCount(), second.recordCount()) * 8;
    CountingLoader loader = new CountingLoader(true, oneFile);

    StructLikeSet set1 = loader.loadEqualityDeletes(ImmutableList.of(first, second), idSchema);
    StructLikeSet set2 = loader.loadEqualityDeletes(ImmutableList.of(first, second), idSchema);

    assertThat(set2).isNotSameAs(set1).isEqualTo(set1);
    assertThat(ids(set1)).containsExactlyInAnyOrder(1L, 2L, 3L);
    assertThat(loader.mergedLoads()).isZero();
    assertThat(loader.fileLoads()).as("per-file entries read once each").isEqualTo(2);
  }

  private DeleteFile eqDeletes(String name, long... ids) throws IOException {
    Record template = GenericRecord.create(idSchema);
    List<Record> deletes = Lists.newArrayList();
    for (long id : ids) {
      deletes.add(template.copy("id", id));
    }

    File file = new File(deleteDir, name + ".parquet");
    return FileHelpers.writeDeleteFile(table, Files.localOutput(file), deletes, idSchema);
  }

  private DeleteFile dataDeletes(String name, String... values) throws IOException {
    Schema dataSchema = table.schema().select("data");
    Record template = GenericRecord.create(dataSchema);
    List<Record> deletes = Lists.newArrayList();
    for (String value : values) {
      deletes.add(template.copy("data", value));
    }

    File file = new File(deleteDir, name + ".parquet");
    return FileHelpers.writeDeleteFile(table, Files.localOutput(file), deletes, dataSchema);
  }

  private static List<Long> ids(StructLikeSet set) {
    List<Long> ids = Lists.newArrayList();
    set.forEach(row -> ids.add(row.get(0, Long.class)));
    return ids;
  }

  /** A loader with an in-memory cache that counts per-file and merged-set loads by key shape. */
  private class CountingLoader extends BaseDeleteLoader {
    private final boolean cacheEnabled;
    private final long maxEntrySize;
    private final Map<String, Object> cache = Maps.newConcurrentMap();
    private final AtomicInteger fileLoads = new AtomicInteger();
    private final AtomicInteger mergedLoads = new AtomicInteger();

    CountingLoader(boolean cacheEnabled) {
      this(cacheEnabled, Long.MAX_VALUE);
    }

    CountingLoader(boolean cacheEnabled, long maxEntrySize) {
      super(deleteFile -> table.io().newInputFile(deleteFile.location()));
      this.cacheEnabled = cacheEnabled;
      this.maxEntrySize = maxEntrySize;
    }

    @Override
    protected boolean canCache(long size) {
      return cacheEnabled && size <= maxEntrySize;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
      return (V)
          cache.computeIfAbsent(
              key,
              k -> {
                if (k.startsWith("eq-delete-set|")) {
                  mergedLoads.incrementAndGet();
                } else {
                  fileLoads.incrementAndGet();
                }

                return valueSupplier.get();
              });
    }

    int fileLoads() {
      return fileLoads.get();
    }

    int mergedLoads() {
      return mergedLoads.get();
    }
  }
}
