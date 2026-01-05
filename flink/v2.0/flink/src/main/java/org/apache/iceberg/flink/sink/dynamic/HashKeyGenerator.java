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

import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.EqualityFieldKeySelector;
import org.apache.iceberg.flink.sink.PartitionKeySelector;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HashKeyGenerator is responsible for creating the appropriate hash key for Flink's keyBy
 * operation. The hash key is generated depending on the user-provided DynamicRecord and the table
 * metadata. Under the hood, we maintain a set of Flink {@link KeySelector}s which implement the
 * appropriate Iceberg {@link DistributionMode}. For every table, we randomly select a consistent
 * subset of writer subtasks which receive data via their associated keys, depending on the chosen
 * DistributionMode.
 *
 * <p>Caching ensures that a new key selector is also created when the table metadata (e.g. schema,
 * spec) or the user-provided metadata changes (e.g. distribution mode, write parallelism).
 *
 * <p>Note: The hashing must be deterministic given the same parameters of the KeySelector and the
 * same provided values.
 */
class HashKeyGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(HashKeyGenerator.class);

  private final int maxWriteParallelism;
  private final Map<SelectorKey, KeySelector<RowData, Integer>> keySelectorCache;

  HashKeyGenerator(int maxCacheSize, int maxWriteParallelism) {
    this.maxWriteParallelism = maxWriteParallelism;
    this.keySelectorCache = new LRUCache<>(maxCacheSize);
  }

  int generateKey(DynamicRecord dynamicRecord) throws Exception {
    return generateKey(dynamicRecord, null, null, null);
  }

  int generateKey(
      DynamicRecord dynamicRecord,
      @Nullable Schema tableSchema,
      @Nullable PartitionSpec tableSpec,
      @Nullable RowData overrideRowData) {
    String tableIdent = dynamicRecord.tableIdentifier().toString();
    SelectorKey cacheKey =
        new SelectorKey(
            tableIdent,
            dynamicRecord.branch(),
            tableSchema != null ? tableSchema.schemaId() : null,
            tableSpec != null ? tableSpec.specId() : null,
            dynamicRecord.schema(),
            dynamicRecord.spec(),
            dynamicRecord.equalityFields());
    KeySelector<RowData, Integer> keySelector =
        keySelectorCache.computeIfAbsent(
            cacheKey,
            k ->
                getKeySelector(
                    tableIdent,
                    MoreObjects.firstNonNull(tableSchema, dynamicRecord.schema()),
                    MoreObjects.firstNonNull(tableSpec, dynamicRecord.spec()),
                    MoreObjects.firstNonNull(
                        dynamicRecord.distributionMode(), DistributionMode.NONE),
                    MoreObjects.firstNonNull(
                        dynamicRecord.equalityFields(), Collections.emptySet()),
                    Math.min(dynamicRecord.writeParallelism(), maxWriteParallelism)));
    try {
      return keySelector.getKey(
          overrideRowData != null ? overrideRowData : dynamicRecord.rowData());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private KeySelector<RowData, Integer> getKeySelector(
      String tableName,
      Schema schema,
      PartitionSpec spec,
      DistributionMode mode,
      Set<String> equalityFields,
      int writeParallelism) {
    LOG.debug(
        "Creating new KeySelector for table '{}' with distribution mode '{}'", tableName, mode);
    switch (mode) {
      case NONE:
        if (equalityFields.isEmpty()) {
          return tableKeySelector(tableName, writeParallelism, maxWriteParallelism);
        } else {
          LOG.info(
              "{}: Distribute rows by equality fields, because there are equality fields set",
              tableName);
          return equalityFieldKeySelector(
              tableName, schema, equalityFields, writeParallelism, maxWriteParallelism);
        }

      case HASH:
        if (equalityFields.isEmpty()) {
          if (spec.isUnpartitioned()) {
            LOG.warn(
                "{}: Fallback to use 'none' distribution mode, because there are no equality fields set "
                    + "and table is unpartitioned",
                tableName);
            return tableKeySelector(tableName, writeParallelism, maxWriteParallelism);
          } else {
            return partitionKeySelector(
                tableName, schema, spec, writeParallelism, maxWriteParallelism);
          }
        } else {
          if (spec.isUnpartitioned()) {
            LOG.info(
                "{}: Distribute rows by equality fields, because there are equality fields set "
                    + "and table is unpartitioned",
                tableName);
            return equalityFieldKeySelector(
                tableName, schema, equalityFields, writeParallelism, maxWriteParallelism);
          } else {
            for (PartitionField partitionField : spec.fields()) {
              Preconditions.checkState(
                  equalityFields.contains(partitionField.name()),
                  "%s: In 'hash' distribution mode with equality fields set, partition field '%s' "
                      + "should be included in equality fields: '%s'",
                  tableName,
                  partitionField,
                  schema.columns().stream()
                      .filter(c -> equalityFields.contains(c.name()))
                      .collect(Collectors.toList()));
            }
            return partitionKeySelector(
                tableName, schema, spec, writeParallelism, maxWriteParallelism);
          }
        }

      case RANGE:
        if (schema.identifierFieldIds().isEmpty()) {
          LOG.warn(
              "{}: Fallback to use 'none' distribution mode, because there are no equality fields set "
                  + "and {}='range' is not supported yet in flink",
              tableName,
              WRITE_DISTRIBUTION_MODE);
          return tableKeySelector(tableName, writeParallelism, maxWriteParallelism);
        } else {
          LOG.info(
              "{}: Distribute rows by equality fields, because there are equality fields set "
                  + "and {}='range' is not supported yet in flink",
              tableName,
              WRITE_DISTRIBUTION_MODE);
          return equalityFieldKeySelector(
              tableName, schema, equalityFields, writeParallelism, maxWriteParallelism);
        }

      default:
        throw new IllegalArgumentException(
            tableName + ": Unrecognized " + WRITE_DISTRIBUTION_MODE + ": " + mode);
    }
  }

  private static KeySelector<RowData, Integer> equalityFieldKeySelector(
      String tableName,
      Schema schema,
      Set<String> equalityFields,
      int writeParallelism,
      int maxWriteParallelism) {
    return new TargetLimitedKeySelector(
        new EqualityFieldKeySelector(
            schema,
            FlinkSchemaUtil.convert(schema),
            DynamicSinkUtil.getEqualityFieldIds(equalityFields, schema)),
        tableName,
        writeParallelism,
        maxWriteParallelism);
  }

  private static KeySelector<RowData, Integer> partitionKeySelector(
      String tableName,
      Schema schema,
      PartitionSpec spec,
      int writeParallelism,
      int maxWriteParallelism) {
    KeySelector<RowData, String> inner =
        new PartitionKeySelector(spec, schema, FlinkSchemaUtil.convert(schema));
    return new TargetLimitedKeySelector(
        in -> inner.getKey(in).hashCode(), tableName, writeParallelism, maxWriteParallelism);
  }

  private static KeySelector<RowData, Integer> tableKeySelector(
      String tableName, int writeParallelism, int maxWriteParallelism) {
    return new TargetLimitedKeySelector(
        new RoundRobinKeySelector<>(writeParallelism),
        tableName,
        writeParallelism,
        maxWriteParallelism);
  }

  /**
   * Generates a new key using the salt as a base, and reduces the target key range of the {@link
   * #wrapped} {@link KeySelector} to {@link #writeParallelism}.
   */
  private static class TargetLimitedKeySelector implements KeySelector<RowData, Integer> {
    private final KeySelector<RowData, Integer> wrapped;
    private final int writeParallelism;
    private final int[] distinctKeys;

    @SuppressWarnings("checkstyle:ParameterAssignment")
    TargetLimitedKeySelector(
        KeySelector<RowData, Integer> wrapped,
        String tableName,
        int writeParallelism,
        int maxWriteParallelism) {
      Preconditions.checkArgument(
          writeParallelism > 0,
          "%s: writeParallelism must be > 0 (is: %s)",
          tableName,
          writeParallelism);
      Preconditions.checkArgument(
          writeParallelism <= maxWriteParallelism,
          "%s: writeParallelism (%s) must be <= maxWriteParallelism (%s)",
          tableName,
          writeParallelism,
          maxWriteParallelism);
      this.wrapped = wrapped;
      this.writeParallelism = writeParallelism;
      this.distinctKeys = new int[writeParallelism];

      // Ensures that the generated keys are always result in unique slotId
      Set<Integer> targetSlots = Sets.newHashSetWithExpectedSize(writeParallelism);
      int nextKey = tableName.hashCode();
      for (int i = 0; i < writeParallelism; ++i) {
        int subtaskId = subtaskId(nextKey, writeParallelism, maxWriteParallelism);
        while (targetSlots.contains(subtaskId)) {
          ++nextKey;
          subtaskId = subtaskId(nextKey, writeParallelism, maxWriteParallelism);
        }

        targetSlots.add(subtaskId);
        distinctKeys[i] = nextKey;
        ++nextKey;
      }
    }

    @Override
    public Integer getKey(RowData value) throws Exception {
      return distinctKeys[
          DynamicSinkUtil.safeAbs(wrapped.getKey(value).hashCode()) % writeParallelism];
    }

    private static int subtaskId(int key, int writeParallelism, int maxWriteParallelism) {
      return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
          maxWriteParallelism,
          writeParallelism,
          KeyGroupRangeAssignment.computeKeyGroupForKeyHash(key, maxWriteParallelism));
    }
  }

  /**
   * Generates evenly distributed keys between [0..{@link #maxTarget}) range using round-robin
   * algorithm.
   *
   * @param <T> unused input for key generation
   */
  private static class RoundRobinKeySelector<T> implements KeySelector<T, Integer> {
    private final int maxTarget;
    private int lastTarget = 0;

    RoundRobinKeySelector(int maxTarget) {
      this.maxTarget = maxTarget;
    }

    @Override
    public Integer getKey(T value) {
      lastTarget = (lastTarget + 1) % maxTarget;
      return lastTarget;
    }
  }

  /**
   * Cache key for the {@link KeySelector}. Only contains the {@link Schema} and the {@link
   * PartitionSpec} if their ids are not provided.
   */
  static class SelectorKey {
    private final String tableName;
    private final String branch;
    private final Integer schemaId;
    private final Integer specId;
    private final Schema schema;
    private final PartitionSpec spec;
    private final Set<String> equalityFields;

    SelectorKey(
        String tableName,
        String branch,
        @Nullable Integer tableSchemaId,
        @Nullable Integer tableSpecId,
        Schema schema,
        PartitionSpec spec,
        Set<String> equalityFields) {
      this.tableName = tableName;
      this.branch = branch;
      this.schemaId = tableSchemaId;
      this.specId = tableSpecId;
      this.schema = tableSchemaId == null ? schema : null;
      this.spec = tableSpecId == null ? spec : null;
      this.equalityFields = equalityFields;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      SelectorKey that = (SelectorKey) other;
      return Objects.equals(tableName, that.tableName)
          && Objects.equals(branch, that.branch)
          && Objects.equals(schemaId, that.schemaId)
          && Objects.equals(specId, that.specId)
          && Objects.equals(schema, that.schema)
          && Objects.equals(spec, that.spec)
          && Objects.equals(equalityFields, that.equalityFields);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableName, branch, schemaId, specId, schema, spec, equalityFields);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("tableName", tableName)
          .add("branch", branch)
          .add("schemaId", schemaId)
          .add("specId", specId)
          .add("schema", schema)
          .add("spec", spec)
          .add("equalityFields", equalityFields)
          .toString();
    }
  }

  @VisibleForTesting
  Map<SelectorKey, KeySelector<RowData, Integer>> getKeySelectorCache() {
    return keySelectorCache;
  }
}
