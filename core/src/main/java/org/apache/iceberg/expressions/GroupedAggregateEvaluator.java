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
package org.apache.iceberg.expressions;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeMap;

/**
 * Evaluates aggregates for each group of identity partition values, using file metadata.
 *
 * <p>Each group-by field must be the source of an identity partition field in the spec of every
 * file passed to {@link #update(DataFile)}. Otherwise, the grouped result cannot be computed from
 * metadata and {@link #allAggregatorsValid()} returns false.
 */
public class GroupedAggregateEvaluator {

  public static GroupedAggregateEvaluator create(
      Schema schema,
      Map<Integer, PartitionSpec> specs,
      List<Integer> groupByFieldIds,
      List<BoundAggregate<?, ?>> aggregates) {
    return new GroupedAggregateEvaluator(schema, specs, groupByFieldIds, aggregates);
  }

  private final Map<Integer, PartitionSpec> specs;
  private final List<Integer> groupByFieldIds;
  private final List<BoundAggregate<?, ?>> aggregates;
  private final Types.StructType groupType;
  private final Types.StructType resultType;
  private final Map<Integer, int[]> positionsBySpecId = Maps.newHashMap();
  private final StructLikeMap<AggregateEvaluator> evaluatorsByGroup;
  private boolean canGroup = true;

  private GroupedAggregateEvaluator(
      Schema schema,
      Map<Integer, PartitionSpec> specs,
      List<Integer> groupByFieldIds,
      List<BoundAggregate<?, ?>> aggregates) {
    Preconditions.checkArgument(!groupByFieldIds.isEmpty(), "Invalid group by fields: empty");

    List<Types.NestedField> groupFields = Lists.newArrayList();
    for (int pos = 0; pos < groupByFieldIds.size(); pos += 1) {
      int fieldId = groupByFieldIds.get(pos);
      Types.NestedField field = schema.findField(fieldId);
      Preconditions.checkArgument(field != null, "Cannot find group by field with ID %s", fieldId);
      groupFields.add(
          Types.NestedField.optional(pos, schema.findColumnName(fieldId), field.type()));
    }

    List<Types.NestedField> resultFields = Lists.newArrayList(groupFields);
    for (BoundAggregate<?, ?> aggregate : aggregates) {
      resultFields.add(
          Types.NestedField.optional(resultFields.size(), aggregate.describe(), aggregate.type()));
    }

    this.specs = specs;
    this.groupByFieldIds = groupByFieldIds;
    this.aggregates = aggregates;
    this.groupType = Types.StructType.of(groupFields);
    this.resultType = Types.StructType.of(resultFields);
    this.evaluatorsByGroup = StructLikeMap.create(groupType);
  }

  public void update(DataFile file) {
    if (!canGroup) {
      return;
    }

    int[] positions = positionsBySpecId.computeIfAbsent(file.specId(), this::identityPositions);
    if (positions == null) {
      this.canGroup = false;
      return;
    }

    StructLike partition = file.partition();
    GenericRecord group = GenericRecord.create(groupType);
    for (int i = 0; i < positions.length; i += 1) {
      group.set(i, partition.get(positions[i], Object.class));
    }

    evaluatorsByGroup
        .computeIfAbsent(group, ignored -> AggregateEvaluator.create(aggregates))
        .update(file);
  }

  /** Returns the type of result rows: the group-by fields followed by the aggregates. */
  public Types.StructType resultType() {
    return resultType;
  }

  public boolean allAggregatorsValid() {
    return canGroup
        && evaluatorsByGroup.values().stream().allMatch(AggregateEvaluator::allAggregatorsValid);
  }

  /** Returns one row for each group, using the {@link #resultType()}. */
  public List<StructLike> results() {
    int groupSize = groupType.fields().size();
    List<StructLike> results = Lists.newArrayListWithExpectedSize(evaluatorsByGroup.size());
    for (Map.Entry<StructLike, AggregateEvaluator> entry : evaluatorsByGroup.entrySet()) {
      StructLike group = entry.getKey();
      StructLike aggregateResult = entry.getValue().result();
      GenericRecord row = GenericRecord.create(resultType);
      for (int pos = 0; pos < groupSize; pos += 1) {
        row.set(pos, group.get(pos, Object.class));
      }

      for (int pos = 0; pos < aggregateResult.size(); pos += 1) {
        row.set(groupSize + pos, aggregateResult.get(pos, Object.class));
      }

      results.add(row);
    }

    return results;
  }

  private int[] identityPositions(int specId) {
    PartitionSpec spec = specs.get(specId);
    Preconditions.checkArgument(spec != null, "Cannot find partition spec with ID %s", specId);

    int[] positions = new int[groupByFieldIds.size()];
    for (int i = 0; i < positions.length; i += 1) {
      int position = identityPosition(spec, groupByFieldIds.get(i));
      if (position < 0) {
        return null;
      }

      positions[i] = position;
    }

    return positions;
  }

  private static int identityPosition(PartitionSpec spec, int sourceId) {
    List<PartitionField> fields = spec.fields();
    for (int pos = 0; pos < fields.size(); pos += 1) {
      PartitionField field = fields.get(pos);
      if (field.sourceId() == sourceId && field.transform().isIdentity()) {
        return pos;
      }
    }

    return -1;
  }
}
