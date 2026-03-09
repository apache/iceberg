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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestBaseDeleteLoader {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "category", Types.StringType.get()));

  @TempDir private File tableDir;
  private Table table;

  @BeforeEach
  public void before() throws IOException {
    this.table = TestTables.create(tableDir, "test", SCHEMA, PartitionSpec.unpartitioned(), 2);
  }

  @AfterEach
  public void after() {
    TestTables.clearTables();
  }

  @Test
  public void testEqDeleteCacheKeyIncludesProjection() throws IOException {
    Schema deleteSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    GenericRecord record = GenericRecord.create(deleteSchema);
    List<Record> deletes =
        ImmutableList.of(record.copy("id", 1, "data", "aaa"), record.copy("id", 2, "data", "bbb"));

    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(
            table,
            table
                .io()
                .newOutputFile(table.locationProvider().newDataLocation("eq-deletes.parquet")),
            deletes,
            deleteSchema);

    TrackingCacheDeleteLoader loader =
        new TrackingCacheDeleteLoader(df -> table.io().newInputFile(df.location()));

    // Load with projection on field 1 (id)
    Schema projection1 = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    StructLikeSet result1 = loader.loadEqualityDeletes(ImmutableList.of(deleteFile), projection1);
    assertThat(result1).hasSize(2);

    // Load again with same projection - should be a cache hit
    StructLikeSet result2 = loader.loadEqualityDeletes(ImmutableList.of(deleteFile), projection1);
    assertThat(result2).hasSize(2);

    // Load with projection on field 2 (data) - should be a cache miss
    Schema projection2 = new Schema(Types.NestedField.required(2, "data", Types.StringType.get()));
    StructLikeSet result3 = loader.loadEqualityDeletes(ImmutableList.of(deleteFile), projection2);
    assertThat(result3).hasSize(2);

    // Load with both fields - should also be a different cache key
    Schema projectionBoth =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    StructLikeSet result4 =
        loader.loadEqualityDeletes(ImmutableList.of(deleteFile), projectionBoth);
    assertThat(result4).hasSize(2);

    // Verify cache key behavior:
    // - 4 calls total but only 3 distinct cache keys (projection1 used twice)
    assertThat(loader.loadCount())
        .as("Expected 3 cache loads (projection1, projection2, projectionBoth)")
        .isEqualTo(3);

    // Verify the cache keys include field IDs
    String location = deleteFile.location();
    assertThat(loader.cacheKeys())
        .containsExactlyInAnyOrder(location + "#1", location + "#2", location + "#1,2");
  }

  /**
   * A BaseDeleteLoader subclass that tracks cache keys and load counts for testing. Uses a simple
   * HashMap as the cache backend.
   */
  private static class TrackingCacheDeleteLoader extends BaseDeleteLoader {
    private final Map<String, Object> cache = new HashMap<>();
    private int loadCount = 0;

    TrackingCacheDeleteLoader(Function<DeleteFile, InputFile> loadInputFile) {
      super(loadInputFile);
    }

    @Override
    protected boolean canCache(long size) {
      return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
      if (cache.containsKey(key)) {
        return (V) cache.get(key);
      }
      V value = valueSupplier.get();
      cache.put(key, value);
      loadCount++;
      return value;
    }

    int loadCount() {
      return loadCount;
    }

    java.util.Set<String> cacheKeys() {
      return cache.keySet();
    }
  }
}
