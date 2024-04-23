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
package org.apache.iceberg.util;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.SortOrderVisitor;

public class SortOrderUtil {

  private SortOrderUtil() {}

  public static SortOrder buildSortOrder(Table table) {
    return buildSortOrder(table.schema(), table.spec(), table.sortOrder());
  }

  // builds a sort order using both the table partition spec and the user supplied sort order
  public static SortOrder buildSortOrder(Table table, SortOrder sortOrder) {
    return buildSortOrder(table.schema(), table.spec(), sortOrder);
  }

  /**
   * Build a final sort order that satisfies the clustering required by the partition spec.
   *
   * <p>The incoming sort order may or may not satisfy the clustering needed by the partition spec.
   * This modifies the sort order so that it clusters by partition and still produces the same order
   * within each partition.
   *
   * @param schema a schema
   * @param spec a partition spec
   * @param sortOrder a sort order
   * @return the sort order with additional sort fields to satisfy the clustering required by the
   *     spec
   */
  public static SortOrder buildSortOrder(Schema schema, PartitionSpec spec, SortOrder sortOrder) {
    if (sortOrder.isUnsorted() && spec.isUnpartitioned()) {
      return SortOrder.unsorted();
    }

    // make a map of the partition fields that need to be included in the clustering produced by the
    // sort order
    Map<Pair<String, Integer>, PartitionField> requiredClusteringFields =
        requiredClusteringFields(spec);

    // remove any partition fields that are clustered by the sort order by iterating over a prefix
    // in the sort order.
    // this will stop when a non-partition field is found, or when the sort field only satisfies the
    // partition field.
    for (SortField sortField : sortOrder.fields()) {
      Pair<String, Integer> sourceAndTransform =
          Pair.of(sortField.transform().toString(), sortField.sourceId());
      if (requiredClusteringFields.containsKey(sourceAndTransform)) {
        requiredClusteringFields.remove(sourceAndTransform);
        continue; // keep processing the prefix
      }

      // if the field satisfies the order of any partition fields, also remove them before stopping
      // use a set to avoid concurrent modification
      for (PartitionField field : spec.fields()) {
        if (sortField.sourceId() == field.sourceId()
            && sortField.transform().satisfiesOrderOf(field.transform())) {
          requiredClusteringFields.remove(Pair.of(field.transform().toString(), field.sourceId()));
        }
      }

      break;
    }

    // build a sort prefix of partition fields that are not already in the sort order's prefix
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    for (PartitionField field : requiredClusteringFields.values()) {
      String sourceName = schema.findColumnName(field.sourceId());
      builder.asc(Expressions.transform(sourceName, field.transform()));
    }

    // add the configured sort to the partition spec prefix sort
    SortOrderVisitor.visit(sortOrder, new CopySortOrderFields(builder));

    return builder.build();
  }

  private static Map<Pair<String, Integer>, PartitionField> requiredClusteringFields(
      PartitionSpec spec) {
    Map<Pair<String, Integer>, PartitionField> requiredClusteringFields = Maps.newLinkedHashMap();
    for (PartitionField partField : spec.fields()) {
      if (!partField.transform().toString().equals("void")) {
        requiredClusteringFields.put(
            Pair.of(partField.transform().toString(), partField.sourceId()), partField);
      }
    }

    // remove any partition fields that are satisfied by another partition field, like days(ts) and
    // hours(ts)
    for (PartitionField partField : spec.fields()) {
      for (PartitionField field : spec.fields()) {
        if (!partField.equals(field)
            && partField.sourceId() == field.sourceId()
            && partField.transform().satisfiesOrderOf(field.transform())) {
          requiredClusteringFields.remove(Pair.of(field.transform().toString(), field.sourceId()));
        }
      }
    }

    return requiredClusteringFields;
  }

  public static Set<Integer> orderPreservingSortedColumns(SortOrder sortOrder) {
    if (sortOrder == null) {
      return Collections.emptySet();
    } else {
      return sortOrder.fields().stream()
          .filter(f -> f.transform().preservesOrder())
          .map(SortField::sourceId)
          .filter(sid -> sortOrder.schema().findColumnName(sid) != null)
          .collect(Collectors.toSet());
    }
  }
}
