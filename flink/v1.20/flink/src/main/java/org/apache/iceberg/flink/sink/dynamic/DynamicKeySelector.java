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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.EqualityFieldKeySelector;
import org.apache.iceberg.flink.sink.NonThrowingKeySelector;
import org.apache.iceberg.flink.sink.PartitionKeySelector;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
class DynamicKeySelector implements NonThrowingKeySelector<DynamicKeySelector.Input, Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicKeySelector.class);

  private final int maxWriteParallelism;
  private final Cache<SelectorKey, NonThrowingKeySelector<RowData, Integer>> keySelectorCache;

  DynamicKeySelector(int maxCacheSize, int maxWriteParallelism) {
    this.maxWriteParallelism = maxWriteParallelism;
    this.keySelectorCache = Caffeine.newBuilder().maximumSize(maxCacheSize).build();
  }

  @Override
  public Integer getKey(Input input) {
    SelectorKey cacheKey = new SelectorKey(input);
    return keySelectorCache
        .get(
            cacheKey,
            k ->
                getKeySelector(
                    input.tableName,
                    input.schema,
                    input.spec,
                    input.mode,
                    input.equalityFields,
                    input.writeParallelism))
        .getKey(input.rowData);
  }

  public NonThrowingKeySelector<RowData, Integer> getKeySelector(
      String tableName,
      Schema schema,
      PartitionSpec spec,
      DistributionMode mode,
      List<String> equalityFields,
      int writeParallelism) {
    LOG.info("Write distribution mode is '{}'", mode.modeName());
    switch (mode) {
      case NONE:
        if (equalityFields.isEmpty()) {
          return tableKeySelector(tableName, writeParallelism, maxWriteParallelism);
        } else {
          LOG.info("Distribute rows by equality fields, because there are equality fields set");
          return equalityFieldKeySelector(
              tableName, schema, equalityFields, writeParallelism, maxWriteParallelism);
        }

      case HASH:
        if (equalityFields.isEmpty()) {
          if (spec.isUnpartitioned()) {
            LOG.warn(
                "Fallback to use 'none' distribution mode, because there are no equality fields set "
                    + "and table is unpartitioned");
            return tableKeySelector(tableName, writeParallelism, maxWriteParallelism);
          } else {
            return partitionKeySelector(
                tableName, schema, spec, writeParallelism, maxWriteParallelism);
          }
        } else {
          if (spec.isUnpartitioned()) {
            LOG.info(
                "Distribute rows by equality fields, because there are equality fields set "
                    + "and table is unpartitioned");
            return equalityFieldKeySelector(
                tableName, schema, equalityFields, writeParallelism, maxWriteParallelism);
          } else {
            for (PartitionField partitionField : spec.fields()) {
              Preconditions.checkState(
                  equalityFields.contains(partitionField.name()),
                  "In 'hash' distribution mode with equality fields set, partition field '%s' "
                      + "should be included in equality fields: '%s'",
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
              "Fallback to use 'none' distribution mode, because there are no equality fields set "
                  + "and {}=range is not supported yet in flink",
              WRITE_DISTRIBUTION_MODE);
          return tableKeySelector(tableName, writeParallelism, maxWriteParallelism);
        } else {
          LOG.info(
              "Distribute rows by equality fields, because there are equality fields set "
                  + "and{}=range is not supported yet in flink",
              WRITE_DISTRIBUTION_MODE);
          return equalityFieldKeySelector(
              tableName, schema, equalityFields, writeParallelism, maxWriteParallelism);
        }

      default:
        throw new IllegalArgumentException("Unrecognized " + WRITE_DISTRIBUTION_MODE + ": " + mode);
    }
  }

  private static NonThrowingKeySelector<RowData, Integer> equalityFieldKeySelector(
      String tableName,
      Schema schema,
      List<String> equalityFields,
      int writeParallelism,
      int maxWriteParallelism) {
    return new TargetLimitedKeySelector(
        new EqualityFieldKeySelector(
            schema,
            FlinkSchemaUtil.convert(schema),
            DynamicRecordProcessor.getEqualityFieldIds(equalityFields, schema)),
        tableName.hashCode(),
        writeParallelism,
        maxWriteParallelism);
  }

  private static NonThrowingKeySelector<RowData, Integer> partitionKeySelector(
      String tableName,
      Schema schema,
      PartitionSpec spec,
      int writeParallelism,
      int maxWriteParallelism) {
    NonThrowingKeySelector<RowData, String> inner =
        new PartitionKeySelector(spec, schema, FlinkSchemaUtil.convert(schema));
    return new TargetLimitedKeySelector(
        in -> inner.getKey(in).hashCode(),
        tableName.hashCode(),
        writeParallelism,
        maxWriteParallelism);
  }

  private static NonThrowingKeySelector<RowData, Integer> tableKeySelector(
      String tableName, int writeParallelism, int maxWriteParallelism) {
    return new TargetLimitedKeySelector(
        new RoundRobinKeySelector<>(writeParallelism),
        tableName.hashCode(),
        writeParallelism,
        maxWriteParallelism);
  }

  /**
   * Generates a new key using the salt as a base, and reduces the target key range of the {@link
   * #wrapped} {@link NonThrowingKeySelector} to {@link #writeParallelism}.
   */
  private static class TargetLimitedKeySelector
      implements NonThrowingKeySelector<RowData, Integer> {
    private final NonThrowingKeySelector<RowData, Integer> wrapped;
    private final int writeParallelism;
    private final int[] distinctKeys;

    @SuppressWarnings("checkstyle:ParameterAssignment")
    TargetLimitedKeySelector(
        NonThrowingKeySelector<RowData, Integer> wrapped,
        int salt,
        int writeParallelism,
        int maxWriteParallelism) {
      if (writeParallelism > maxWriteParallelism) {
        LOG.warn(
            "writeParallelism {} is greater than maxWriteParallelism {}. Capping writeParallelism at {}",
            writeParallelism,
            maxWriteParallelism,
            maxWriteParallelism);
        writeParallelism = maxWriteParallelism;
      }
      this.wrapped = wrapped;
      this.writeParallelism = writeParallelism;
      this.distinctKeys = new int[writeParallelism];

      // Ensures that the generated keys are always result in unique slotId
      Set<Integer> targetSlots = Sets.newHashSetWithExpectedSize(writeParallelism);
      int nextKey = salt;
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
    public Integer getKey(RowData value) {
      return distinctKeys[Math.abs(wrapped.getKey(value).hashCode()) % writeParallelism];
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
  private static class RoundRobinKeySelector<T> implements NonThrowingKeySelector<T, Integer> {
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

  static class Input {
    private final String tableName;
    private final String branch;
    private final Integer schemaId;
    private final Integer specId;
    private final Schema schema;
    private final PartitionSpec spec;
    private final DistributionMode mode;
    private final int writeParallelism;
    private final List<String> equalityFields;
    private final RowData rowData;

    Input(
        DynamicRecord dynamicRecord,
        Schema schemaOverride,
        PartitionSpec specOverride,
        RowData rowDataOverride) {
      this(
          dynamicRecord.tableIdentifier().toString(),
          dynamicRecord.branch(),
          schemaOverride != null ? schemaOverride.schemaId() : null,
          specOverride != null ? specOverride.specId() : null,
          schemaOverride,
          specOverride,
          dynamicRecord.mode(),
          dynamicRecord.writeParallelism(),
          dynamicRecord.equalityFields() != null
              ? dynamicRecord.equalityFields()
              : Collections.emptyList(),
          rowDataOverride);
    }

    private Input(
        String tableName,
        String branch,
        Integer schemaId,
        Integer specId,
        Schema schema,
        PartitionSpec spec,
        DistributionMode mode,
        int writeParallelism,
        List<String> equalityFields,
        RowData rowData) {
      this.tableName = tableName;
      this.branch = branch;
      this.schemaId = schemaId;
      this.specId = specId;
      this.schema = schema;
      this.spec = spec;
      this.mode = mode;
      this.writeParallelism = writeParallelism;
      this.equalityFields = equalityFields;
      this.rowData = rowData;
    }
  }

  /**
   * Cache key for the {@link NonThrowingKeySelector}. Only contains the {@link Schema} and the
   * {@link PartitionSpec} if the ids are not available.
   */
  private static class SelectorKey {
    private final String tableName;
    private final String branch;
    private final Integer schemaId;
    private final Integer specId;
    private final Schema schema;
    private final PartitionSpec spec;
    private final List<String> equalityFields;

    private SelectorKey(Input input) {
      this.tableName = input.tableName;
      this.branch = input.branch;
      this.schemaId = input.schemaId;
      this.schema = schemaId == null ? input.schema : null;
      this.specId = input.specId;
      this.spec = specId == null ? input.spec : null;
      this.equalityFields = input.equalityFields;
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
          .add("eqalityFields", equalityFields)
          .toString();
    }
  }
}
