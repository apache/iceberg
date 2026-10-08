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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;

/** Plans the schema that each file of a vertically split data file is read with. */
public class VerticalSplitProjectionPlanner {
  private VerticalSplitProjectionPlanner() {}

  /**
   * Prepares the schemas that can be used for each particular file as a projection for the reads.
   * These schemas are prepared in a way to contain all the required fields to perform a full read
   * including stitching vertical splits.
   *
   * @param file the data file to read, which may reference column files
   * @param projection the schema the read produces
   * @return a schema per file location to read it with, the base data file first
   */
  public static Map<String, Schema> plan(DataFile file, Schema projection) {
    Preconditions.checkArgument(file != null, "Invalid data file: null");
    Preconditions.checkArgument(projection != null, "Invalid projection: null");

    List<ColumnFile> columnFiles = file.columnFiles();
    if (columnFiles == null || columnFiles.isEmpty()) {
      return ImmutableMap.of(file.location(), projection);
    }

    Set<Integer> projectedIdsToAssign = Sets.newLinkedHashSet(TypeUtil.getProjectedIds(projection));
    Map<String, Schema> columnFilePlan = Maps.newLinkedHashMap();
    for (ColumnFile columnFile : columnFiles) {
      Set<Integer> ids = Sets.newLinkedHashSet();
      for (Integer fieldId : columnFile.fieldIds()) {
        if (projectedIdsToAssign.remove(fieldId)) {
          ids.add(fieldId);
        }
      }

      if (!ids.isEmpty()) {
        columnFilePlan.put(columnFile.location(), TypeUtil.select(projection, ids));
      }
    }

    if (columnFilePlan.isEmpty()) {
      return ImmutableMap.of(file.location(), projection);
    }

    return ImmutableMap.<String, Schema>builder()
        .put(file.location(), baseFileProjection(projection, projectedIdsToAssign))
        .putAll(columnFilePlan)
        .build();
  }

  // The base file is read even without projected fields as it defines the rows of the read
  private static Schema baseFileProjection(Schema projection, Set<Integer> ids) {
    if (!ids.isEmpty()) {
      return TypeUtil.select(projection, ids);
    }

    return new Schema(MetadataColumns.ROW_POSITION);
  }
}
