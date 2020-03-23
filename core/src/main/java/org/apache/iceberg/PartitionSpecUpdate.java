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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Map;

/**
 * PartitionSpec evolution API implementation.
 * TODO: https://github.com/apache/incubator-iceberg/issues/281
 */
class PartitionSpecUpdate implements UpdatePartitionSpec {

  private final TableMetadata base;
  private final TableOperations ops;
  private PartitionSpec newSpec = null;

  PartitionSpecUpdate(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public PartitionSpec apply() {
    return Preconditions.checkNotNull(newSpec, "new spec is not set");
  }

  @Override
  public UpdatePartitionSpec update(PartitionSpec partitionSpec) {
    PartitionSpec.checkCompatibility(partitionSpec, base.schema());
    newSpec = partitionSpec;
    return this;
  }

  @Override
  public void commit() {
    TableMetadata update = base.updatePartitionSpec(freshSpecFieldIds(apply()));
    ops.commit(base, update);
  }

  private PartitionSpec freshSpecFieldIds(PartitionSpec partitionSpec) {
    int lastAssignedFieldId = 0;
    Map<String, Integer> partitionFieldIdByName = Maps.newHashMap();
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
