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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.Transforms;

/**
 * PartitionSpec evolution API implementation.
 */
class PartitionSpecUpdate implements UpdatePartitionSpec {

  private final TableMetadata base;
  private final TableOperations ops;
  private final List<Consumer<PartitionSpec.Builder>> newSpecFields = new ArrayList<>();
  private final Map<String, PartitionField> curSpecFields;

  PartitionSpecUpdate(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    this.curSpecFields = base.spec().fields().stream().collect(
        Collectors.toMap(
            PartitionField::name,
            Function.identity(),
            (n1, n2) -> {
              throw new IllegalStateException(String.format("Duplicate partition field found: %s", n1));
            },
            LinkedHashMap::new
        )
    );
  }

  @Override
  public PartitionSpec apply() {
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(base.schema());
    curSpecFields.values().forEach(field ->
        specBuilder.add(
            field.sourceId(),
            field.fieldId(),
            field.name(),
            field.transform().toString())
    );
    newSpecFields.forEach(c -> c.accept(specBuilder));
    return specBuilder.build();
  }

  @Override
  public UpdatePartitionSpec clear() {
    newSpecFields.clear();
    curSpecFields.clear();
    return this;
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
        "Cannot use empty or null partition name: %s", newName);
    Preconditions.checkArgument(!curSpecFields.containsKey(newName),
        "Cannot use partition name more than once: %s", newName);

    PartitionField field = curSpecFields.remove(name);
    curSpecFields.put(newName,
        new PartitionField(field.sourceId(), field.fieldId(), newName, field.transform()));
    return this;
  }

  @Override
  public UpdatePartitionSpec removeField(String name) {
    Preconditions.checkArgument(curSpecFields.containsKey(name),
        "Cannot find an existing partition field with the name: %s", name);
    if (base.formatVersion() == 1) {
      PartitionField field = curSpecFields.remove(name);
      String newName = field.name() + "_removed"; // rename it for soft delete
      curSpecFields.put(newName,
          new PartitionField(field.sourceId(), field.fieldId(), newName, Transforms.alwaysNull()));
    } else {
      curSpecFields.remove(name);
    }
    return this;
  }

  @Override
  public void commit() {
    TableMetadata update = base.updatePartitionSpec(freshSpecFieldIds(apply()));
    ops.commit(base, update);
  }

  private PartitionSpec freshSpecFieldIds(PartitionSpec partitionSpec) {
    if (base.formatVersion() == 1) {
      return partitionSpec;
    }

    int lastAssignedFieldId = 0;
    Map<String, Integer> partitionFieldIdByName = new HashMap<>();
    for (PartitionSpec spec : base.specs()) {
      for (PartitionField field : spec.fields()) {
        partitionFieldIdByName.put(getKey(field), field.fieldId());
      }
      lastAssignedFieldId = Math.max(lastAssignedFieldId, spec.lastAssignedFieldId());
    }

    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(base.schema())
        .withSpecId(partitionSpec.specId());

    for (PartitionField field : partitionSpec.fields()) {
      int assignedFieldId = partitionFieldIdByName.containsKey(getKey(field)) ?
          partitionFieldIdByName.get(getKey(field)) : ++lastAssignedFieldId;

      specBuilder.add(
          field.sourceId(),
          assignedFieldId,
          field.name(),
          field.transform().toString());
    }
    return specBuilder.build();
  }

  private static String getKey(PartitionField field) {
    return field.transform() + "(" + field.sourceId() + ")";
  }
}
