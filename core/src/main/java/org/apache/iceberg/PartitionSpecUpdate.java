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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

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
  private final List<PartitionField> fields = Lists.newArrayList();
  private final AtomicInteger lastAssignedFieldId = new AtomicInteger(0);
  private final Map<String, Integer> partitionFieldIdByKey = Maps.newHashMap();

  PartitionSpecUpdate(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    this.specs = ImmutableList.<PartitionSpec>builder().addAll(base.specs()).add(base.spec()).build();
    this.schema = base.schema();
    this.curSpecFields = buildSpecByNameMap(base.spec());
    init();
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
    this.curSpecFields = buildSpecByNameMap(partitionSpecs.get(partitionSpecs.size() - 1));
    init();
  }

  private Map<String, PartitionField> buildSpecByNameMap(PartitionSpec spec) {
    return spec.fields().stream()
        .filter(PartitionSpecUpdate::notSoftDeleted).collect(
            Collectors.toMap(
                PartitionField::name,
                Function.identity()));
  }

  private void init() {
    for (PartitionSpec spec : specs) {
      for (PartitionField field : spec.fields()) {
        if (notSoftDeleted(field)) {
          partitionFieldIdByKey.put(getKey(field.transform(), field.sourceId()), field.fieldId());
        }
      }
      lastAssignedFieldId.getAndAccumulate(spec.lastAssignedFieldId(), Math::max);
    }
  }

  @Override
  public void commit() {
    PartitionSpec newSpec = apply(); // V2 ready partition spec
    if (base.formatVersion() == 1) {
      newSpec = fillGapsByNullFields(newSpec);
    }
    TableMetadata updated = base.updatePartitionSpec(newSpec);

    if (updated == base) {
      // do not commit if the metadata has not changed. For example, this may happen
      // when the committing partition spec is already current. Note that this check uses identity.
      return;
    }

    ops.commit(base, updated);
  }

  @Override
  public PartitionSpec apply() {
    fields.addAll(curSpecFields.values());
    fields.sort(Comparator.comparingInt(PartitionField::fieldId));
    return PartitionSpec.builderFor(schema).addAll(fields).build();
  }

  private UpdatePartitionSpec addFieldWithType(String sourceName,
                                               String targetName,
                                               Function<Type, Transform<?, ?>> func) {
    Types.NestedField sourceColumn = schema.findField(sourceName);
    Preconditions.checkArgument(sourceColumn != null, "Cannot find source column: %s", sourceName);

    Transform<?, ?> transform = func.apply(sourceColumn.type());
    Integer assignedFieldId = partitionFieldIdByKey.get(getKey(transform, sourceColumn.fieldId()));
    if (assignedFieldId == null) {
      assignedFieldId = lastAssignedFieldId.incrementAndGet();
    }

    fields.add(new PartitionField(sourceColumn.fieldId(), assignedFieldId, targetName, transform));
    return this;
  }

  @Override
  public UpdatePartitionSpec addIdentityField(String sourceName, String targetName) {
    return addFieldWithType(sourceName, targetName, Transforms::identity);
  }

  @Override
  public UpdatePartitionSpec addIdentityField(String sourceName) {
    return addIdentityField(sourceName, sourceName);
  }

  @Override
  public UpdatePartitionSpec addYearField(String sourceName, String targetName) {
    return addFieldWithType(sourceName, targetName, Transforms::year);
  }

  @Override
  public UpdatePartitionSpec addYearField(String sourceName) {
    return addYearField(sourceName, sourceName + "_year");
  }

  @Override
  public UpdatePartitionSpec addMonthField(String sourceName, String targetName) {
    return addFieldWithType(sourceName, targetName, Transforms::month);
  }

  @Override
  public UpdatePartitionSpec addMonthField(String sourceName) {
    return addMonthField(sourceName, sourceName + "_month");
  }

  @Override
  public UpdatePartitionSpec addDayField(String sourceName, String targetName) {
    return addFieldWithType(sourceName, targetName, Transforms::day);
  }

  @Override
  public UpdatePartitionSpec addDayField(String sourceName) {
    return addDayField(sourceName, sourceName + "_day");
  }

  @Override
  public UpdatePartitionSpec addHourField(String sourceName, String targetName) {
    return addFieldWithType(sourceName, targetName, Transforms::hour);
  }

  @Override
  public UpdatePartitionSpec addHourField(String sourceName) {
    return addHourField(sourceName, sourceName + "_hour");
  }

  @Override
  public UpdatePartitionSpec addBucketField(String sourceName, int numBuckets, String targetName) {
    return addFieldWithType(sourceName, targetName, type -> Transforms.bucket(type, numBuckets));
  }

  @Override
  public UpdatePartitionSpec addBucketField(String sourceName, int numBuckets) {
    return addBucketField(sourceName, numBuckets, sourceName + "_bucket");
  }

  @Override
  public UpdatePartitionSpec addTruncateField(String sourceName, int width, String targetName) {
    return addFieldWithType(sourceName, targetName, type -> Transforms.truncate(type, width));
  }

  @Override
  public UpdatePartitionSpec addTruncateField(String sourceName, int width) {
    return addTruncateField(sourceName, width, sourceName + "_trunc");
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
    curSpecFields.remove(name);
    return this;
  }

  private static boolean notSoftDeleted(PartitionField field) {
    return !(field.name().endsWith(SOFT_DELETE_POSTFIX) && Transforms.alwaysNull().equals(field.transform()));
  }

  private static String getKey(Transform<?, ?> transform, int sourceId) {
    return transform + "(" + sourceId + ")";
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
