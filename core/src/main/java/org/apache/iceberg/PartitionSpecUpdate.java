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

package org.apache.iceberg;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transforms;

/**
 * PartitionSpec evolution API implementation.
 */
class PartitionSpecUpdate implements UpdatePartitionSpec {

  private static final String SOFT_DELETE_POSTFIX = "__[removed]";

  private final TableMetadata base;
  private final TableOperations ops;
  private final List<PartitionSpec> specs;
  private final Schema schema;
  private final Map<String, PartitionField> curSpecFields;
  private final List<Consumer<PartitionSpec.Builder>> newSpecFields = Lists.newArrayList();
  private final Map<String, PartitionField> newRemovedFields = Maps.newHashMap();

  PartitionSpecUpdate(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    this.specs = ImmutableList.<PartitionSpec>builder().addAll(base.specs()).add(base.spec()).build();
    this.schema = base.schema();
    this.curSpecFields = base.spec().fields().stream().filter(PartitionSpecUpdate::notSoftDeleted).collect(
        Collectors.toMap(
            PartitionField::name,
            Function.identity()
        )
    );
  }

  /**
   * For testing only.
   */
  @VisibleForTesting
  PartitionSpecUpdate(List<PartitionSpec> partitionSpecs) {
    this.ops = null;
    this.base = null;
    this.specs = partitionSpecs;
    this.schema = partitionSpecs.get(partitionSpecs.size() - 1).schema();
    this.curSpecFields = partitionSpecs.get(partitionSpecs.size() - 1).fields().stream()
        .filter(PartitionSpecUpdate::notSoftDeleted).collect(
            Collectors.toMap(
                PartitionField::name,
                Function.identity()));
  }

  @Override
  public void commit() {
    PartitionSpec newSpec = apply(); // V2 ready partition spec
    if (base.formatVersion() == 1) {
      newSpec = fillGapsByNullFields(newSpec);
    }
    TableMetadata update = base.updatePartitionSpec(newSpec);
    ops.commit(base, update);
  }

  @Override
  public PartitionSpec apply() {
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema).addAll(curSpecFields.values());
    newSpecFields.forEach(c -> {
      c.accept(specBuilder);
      checkIfRemoved(specBuilder.getLastPartitionField());
    });
    return freshSpecFieldIds(specBuilder.build());
  }

  @Override
  public UpdatePartitionSpec addIdentityField(String sourceName, String targetName) {
    newSpecFields.add(builder -> builder.identity(sourceName, targetName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addIdentityField(String sourceName) {
    newSpecFields.add(builder -> builder.identity(sourceName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addYearField(String sourceName, String targetName) {
    newSpecFields.add(builder -> builder.year(sourceName, targetName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addYearField(String sourceName) {
    newSpecFields.add(builder -> builder.year(sourceName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addMonthField(String sourceName, String targetName) {
    newSpecFields.add(builder -> builder.month(sourceName, targetName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addMonthField(String sourceName) {
    newSpecFields.add(builder -> builder.month(sourceName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addDayField(String sourceName, String targetName) {
    newSpecFields.add(builder -> builder.day(sourceName, targetName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addDayField(String sourceName) {
    newSpecFields.add(builder -> builder.day(sourceName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addHourField(String sourceName, String targetName) {
    newSpecFields.add(builder -> builder.hour(sourceName, targetName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addHourField(String sourceName) {
    newSpecFields.add(builder -> builder.hour(sourceName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addBucketField(String sourceName, int numBuckets, String targetName) {
    newSpecFields.add(builder -> builder.bucket(sourceName, numBuckets, targetName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addBucketField(String sourceName, int numBuckets) {
    newSpecFields.add(builder -> builder.bucket(sourceName, numBuckets));
    return this;
  }

  @Override
  public UpdatePartitionSpec addTruncateField(String sourceName, int width, String targetName) {
    newSpecFields.add(builder -> builder.truncate(sourceName, width, targetName));
    return this;
  }

  @Override
  public UpdatePartitionSpec addTruncateField(String sourceName, int width) {
    newSpecFields.add(builder -> builder.truncate(sourceName, width));
    return this;
  }

  @Override
  public UpdatePartitionSpec renameField(String name, String newName) {
    Preconditions.checkArgument(curSpecFields.containsKey(name),
        "Cannot find an existing partition field with the name: %s", name);
    Preconditions.checkArgument(newName != null && !newName.isEmpty(),
        "Cannot use an empty or null partition name: %s", newName);

    PartitionField field = curSpecFields.get(name);
    curSpecFields.put(name,
        new PartitionField(field.sourceId(), field.fieldId(), newName, field.transform()));
    return this;
  }

  @Override
  public UpdatePartitionSpec removeField(String name) {
    Preconditions.checkArgument(curSpecFields.containsKey(name),
        "Cannot find an existing partition field with the name: %s", name);
    PartitionField field = curSpecFields.remove(name);
    newRemovedFields.put(getKey(field), field);
    return this;
  }

  private static boolean notSoftDeleted(PartitionField field) {
    return !(field.name().endsWith(SOFT_DELETE_POSTFIX) && Transforms.alwaysNull().equals(field.transform()));
  }

  private static String getKey(PartitionField field) {
    return field.transform() + "(" + field.sourceId() + ")";
  }

  private void checkIfRemoved(PartitionField addedField) {
    String key = getKey(addedField);
    Preconditions.checkArgument(!newRemovedFields.containsKey(key),
        "Cannot add a partition field (%s) because it is compatible with a previously removed field: %s",
        addedField, newRemovedFields.get(key));
  }

  private PartitionSpec freshSpecFieldIds(PartitionSpec partitionSpec) {
    int lastAssignedFieldId = 0;
    Map<String, Integer> partitionFieldIdByKey = Maps.newHashMap();
    for (PartitionSpec spec : specs) {
      for (PartitionField field : spec.fields()) {
        if (notSoftDeleted(field)) {
          partitionFieldIdByKey.put(getKey(field), field.fieldId());
        }
      }
      lastAssignedFieldId = Math.max(lastAssignedFieldId, spec.lastAssignedFieldId());
    }

    List<PartitionField> partitionFields = Lists.newArrayList();
    for (PartitionField field : partitionSpec.fields()) {
      String key = getKey(field);
      if (!partitionFieldIdByKey.containsKey(key)) {
        lastAssignedFieldId++;
      }
      int assignedFieldId = partitionFieldIdByKey.getOrDefault(key, lastAssignedFieldId);

      partitionFields.add(new PartitionField(
          field.sourceId(),
          assignedFieldId,
          field.name(),
          field.transform()));
    }
    partitionFields.sort(Comparator.comparingInt(PartitionField::fieldId));

    return PartitionSpec.builderFor(schema).withSpecId(partitionSpec.specId()).addAll(partitionFields).build();
  }

  private PartitionSpec fillGapsByNullFields(PartitionSpec partitionSpec) {
    Map<Integer, Integer> sourceIdByFieldId = specs.stream().flatMap(spec -> spec.fields().stream()).collect(
        Collectors.toMap(
            PartitionField::fieldId,
            PartitionField::sourceId,
            (n1, n2) -> n2,
            HashMap::new
        ));

    int startId = PartitionSpec.PARTITION_DATA_ID_START;
    PartitionField[] partitionFields = new PartitionField[partitionSpec.lastAssignedFieldId() - startId + 1];
    partitionSpec.fields().forEach(field -> partitionFields[field.fieldId() - startId] = field);
    for (int i = 0; i < partitionFields.length; ++i) {
      if (partitionFields[i] == null) {
        int fieldId = startId + i;
        ValidationException.check(sourceIdByFieldId.containsKey(fieldId),
            "Invalid partition specs, which miss partition field info for id %s.", fieldId);
        partitionFields[i] = new PartitionField(
            sourceIdByFieldId.get(fieldId),
            fieldId,
            fieldId + SOFT_DELETE_POSTFIX,
            Transforms.alwaysNull());
      }
    }

    return PartitionSpec.builderFor(schema).withSpecId(partitionSpec.specId())
        .addAll(Arrays.asList(partitionFields))
        .build();
  }

}
