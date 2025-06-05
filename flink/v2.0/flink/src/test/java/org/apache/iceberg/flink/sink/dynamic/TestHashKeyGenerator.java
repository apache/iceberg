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
package org.apache.iceberg.flink.sink.dynamic;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.Cache;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.NonThrowingKeySelector;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestHashKeyGenerator {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final String BRANCH = "main";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("default", "table");

  @Test
  void testRoundRobinWithDistributionModeNone() {
    int writeParallelism = 10;
    int maxWriteParallelism = 2;
    HashKeyGenerator generator = new HashKeyGenerator(1, maxWriteParallelism);
    PartitionSpec spec = PartitionSpec.unpartitioned();

    GenericRowData row = GenericRowData.of(1, StringData.fromString("z"));
    int writeKey1 =
        getWriteKey(
            generator, spec, DistributionMode.NONE, writeParallelism, Collections.emptyList(), row);
    int writeKey2 =
        getWriteKey(
            generator, spec, DistributionMode.NONE, writeParallelism, Collections.emptyList(), row);
    int writeKey3 =
        getWriteKey(
            generator, spec, DistributionMode.NONE, writeParallelism, Collections.emptyList(), row);
    int writeKey4 =
        getWriteKey(
            generator, spec, DistributionMode.NONE, writeParallelism, Collections.emptyList(), row);

    assertThat(writeKey1).isNotEqualTo(writeKey2);
    assertThat(writeKey3).isEqualTo(writeKey1);
    assertThat(writeKey4).isEqualTo(writeKey2);
  }

  @Test
  void testBucketingWithDistributionModeHash() {
    int writeParallelism = 3;
    HashKeyGenerator generator = new HashKeyGenerator(1, 8);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();

    GenericRowData row1 = GenericRowData.of(1, StringData.fromString("a"));
    GenericRowData row2 = GenericRowData.of(1, StringData.fromString("b"));
    GenericRowData row3 = GenericRowData.of(2, StringData.fromString("c"));
    GenericRowData row4 = GenericRowData.of(2, StringData.fromString("d"));

    int writeKey1 =
        getWriteKey(
            generator,
            spec,
            DistributionMode.HASH,
            writeParallelism,
            Collections.emptyList(),
            row1);
    int writeKey2 =
        getWriteKey(
            generator,
            spec,
            DistributionMode.HASH,
            writeParallelism,
            Collections.emptyList(),
            row2);
    int writeKey3 =
        getWriteKey(
            generator,
            spec,
            DistributionMode.HASH,
            writeParallelism,
            Collections.emptyList(),
            row3);
    int writeKey4 =
        getWriteKey(
            generator,
            spec,
            DistributionMode.HASH,
            writeParallelism,
            Collections.emptyList(),
            row4);

    assertThat(writeKey1).isEqualTo(writeKey2);
    assertThat(writeKey3).isNotEqualTo(writeKey1);
    assertThat(writeKey4).isEqualTo(writeKey3);
  }

  @Test
  void testEqualityKeys() {
    int writeParallelism = 2;
    HashKeyGenerator generator = new HashKeyGenerator(16, 8);
    PartitionSpec unpartitioned = PartitionSpec.unpartitioned();

    GenericRowData row1 = GenericRowData.of(1, StringData.fromString("foo"));
    GenericRowData row2 = GenericRowData.of(1, StringData.fromString("bar"));
    GenericRowData row3 = GenericRowData.of(2, StringData.fromString("baz"));
    List<String> equalityColumns = Collections.singletonList("id");

    int writeKey1 =
        getWriteKey(
            generator,
            unpartitioned,
            DistributionMode.NONE,
            writeParallelism,
            equalityColumns,
            row1);
    int writeKey2 =
        getWriteKey(
            generator,
            unpartitioned,
            DistributionMode.NONE,
            writeParallelism,
            equalityColumns,
            row2);
    int writeKey3 =
        getWriteKey(
            generator,
            unpartitioned,
            DistributionMode.NONE,
            writeParallelism,
            equalityColumns,
            row3);

    assertThat(writeKey1).isEqualTo(writeKey2);
    assertThat(writeKey2).isNotEqualTo(writeKey3);
  }

  @Test
  void testCapAtMaxWriteParallelism() {
    int writeParallelism = 10;
    int maxWriteParallelism = 5;
    HashKeyGenerator generator = new HashKeyGenerator(16, maxWriteParallelism);
    PartitionSpec unpartitioned = PartitionSpec.unpartitioned();

    Set<Integer> writeKeys = Sets.newHashSet();
    for (int i = 0; i < 20; i++) {
      GenericRowData row = GenericRowData.of(i, StringData.fromString("z"));
      writeKeys.add(
          getWriteKey(
              generator,
              unpartitioned,
              DistributionMode.NONE,
              writeParallelism,
              Collections.emptyList(),
              row));
    }

    assertThat(writeKeys).hasSize(maxWriteParallelism);
  }

  @Test
  void testHashModeWithoutEqualityFieldsFallsBackToNone() {
    int writeParallelism = 2;
    HashKeyGenerator generator = new HashKeyGenerator(16, 8);
    Schema noIdSchema = new Schema(Types.NestedField.required(1, "x", Types.StringType.get()));
    PartitionSpec unpartitioned = PartitionSpec.unpartitioned();

    DynamicRecord record =
        new DynamicRecord(
            TABLE_IDENTIFIER,
            BRANCH,
            noIdSchema,
            GenericRowData.of(StringData.fromString("v")),
            unpartitioned,
            DistributionMode.HASH,
            writeParallelism);

    int writeKey1 = generator.generateKey(record);
    int writeKey2 = generator.generateKey(record);
    int writeKey3 = generator.generateKey(record);
    assertThat(writeKey1).isNotEqualTo(writeKey2);
    assertThat(writeKey3).isEqualTo(writeKey1);
  }

  @Test
  void testOverrides() {
    int maxCacheSize = 10;
    int writeParallelism = 5;
    int maxWriteParallelism = 10;
    HashKeyGenerator generator = new HashKeyGenerator(maxCacheSize, maxWriteParallelism);

    DynamicRecord record =
        new DynamicRecord(
            TABLE_IDENTIFIER,
            BRANCH,
            SCHEMA,
            GenericRowData.of(1, StringData.fromString("foo")),
            PartitionSpec.unpartitioned(),
            DistributionMode.NONE,
            writeParallelism);

    int writeKey1 = generator.generateKey(record);
    int writeKey2 = generator.generateKey(record);
    // Assert that we are bucketing via NONE (round-robin)
    assertThat(writeKey1).isNotEqualTo(writeKey2);

    // Schema has different id
    Schema overrideSchema = new Schema(42, SCHEMA.columns());
    // Spec has different id
    PartitionSpec overrideSpec = PartitionSpec.builderFor(SCHEMA).withSpecId(42).build();
    RowData overrideData = GenericRowData.of(1L, StringData.fromString("foo"));

    // We get a new key selector for the schema which starts off on the same offset
    assertThat(generator.generateKey(record, overrideSchema, null, null)).isEqualTo(writeKey1);
    // We get a new key selector for the spec which starts off on the same offset
    assertThat(generator.generateKey(record, null, overrideSpec, null)).isEqualTo(writeKey1);
    // We get the same key selector which yields a different result for the overridden data
    assertThat(generator.generateKey(record, null, null, overrideData)).isNotEqualTo(writeKey1);
  }

  @Test
  void testMultipleTables() {
    int maxCacheSize = 10;
    int writeParallelism = 2;
    int maxWriteParallelism = 8;
    HashKeyGenerator generator = new HashKeyGenerator(maxCacheSize, maxWriteParallelism);

    PartitionSpec unpartitioned = PartitionSpec.unpartitioned();

    GenericRowData rowData = GenericRowData.of(1, StringData.fromString("foo"));

    DynamicRecord record1 =
        new DynamicRecord(
            TableIdentifier.of("a", "table"),
            BRANCH,
            SCHEMA,
            rowData,
            unpartitioned,
            DistributionMode.HASH,
            writeParallelism);
    record1.setEqualityFields(Collections.singletonList("id"));
    DynamicRecord record2 =
        new DynamicRecord(
            TableIdentifier.of("other", "table"),
            BRANCH,
            SCHEMA,
            rowData,
            unpartitioned,
            DistributionMode.HASH,
            writeParallelism);
    record2.setEqualityFields(Collections.singletonList("id"));

    // Consistent hashing for the same record due to HASH distribution mode
    int writeKeyRecord1 = generator.generateKey(record1);
    assertThat(writeKeyRecord1).isEqualTo(generator.generateKey(record1));
    int writeKeyRecord2 = generator.generateKey(record2);
    assertThat(writeKeyRecord2).isEqualTo(generator.generateKey(record2));

    // But the write keys are for different tables and should not be equal
    assertThat(writeKeyRecord1).isNotEqualTo(writeKeyRecord2);
  }

  @Test
  void testCaching() {
    int maxCacheSize = 1;
    int writeParallelism = 2;
    int maxWriteParallelism = 8;
    HashKeyGenerator generator = new HashKeyGenerator(maxCacheSize, maxWriteParallelism);
    Cache<HashKeyGenerator.SelectorKey, NonThrowingKeySelector<RowData, Integer>> keySelectorCache =
        generator.getKeySelectorCache();

    PartitionSpec unpartitioned = PartitionSpec.unpartitioned();
    DynamicRecord record =
        new DynamicRecord(
            TABLE_IDENTIFIER,
            BRANCH,
            SCHEMA,
            GenericRowData.of(1, StringData.fromString("foo")),
            unpartitioned,
            DistributionMode.NONE,
            writeParallelism);

    int writeKey1 = generator.generateKey(record);
    assertThat(keySelectorCache.estimatedSize()).isEqualTo(1);

    int writeKey2 = generator.generateKey(record);
    assertThat(writeKey2).isNotEqualTo(writeKey1);
    // Manually clean up because the cleanup is not always triggered
    keySelectorCache.cleanUp();
    assertThat(keySelectorCache.estimatedSize()).isEqualTo(1);

    int writeKey3 = generator.generateKey(record);
    // Manually clean up because the cleanup is not always triggered
    keySelectorCache.cleanUp();
    assertThat(keySelectorCache.estimatedSize()).isEqualTo(1);
    // We create a new key selector which will start off at the same position
    assertThat(writeKey1).isEqualTo(writeKey3);
  }

  private static int getWriteKey(
      HashKeyGenerator generator,
      PartitionSpec spec,
      DistributionMode mode,
      int writeParallelism,
      List<String> equalityFields,
      GenericRowData row) {
    DynamicRecord record =
        new DynamicRecord(TABLE_IDENTIFIER, BRANCH, SCHEMA, row, spec, mode, writeParallelism);
    record.setEqualityFields(equalityFields);
    return generator.generateKey(record);
  }
}
