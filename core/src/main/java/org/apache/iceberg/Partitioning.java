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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

public class Partitioning {
  private Partitioning() {
  }

  /**
   * Check whether the spec contains a bucketed partition field.
   *
   * @param spec a partition spec
   * @return true if the spec has field with a bucket transform
   */
  public static boolean hasBucketField(PartitionSpec spec) {
    List<Boolean> bucketList = PartitionSpecVisitor.visit(spec, new PartitionSpecVisitor<Boolean>() {
      @Override
      public Boolean identity(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean bucket(int fieldId, String sourceName, int sourceId, int width) {
        return true;
      }

      @Override
      public Boolean truncate(int fieldId, String sourceName, int sourceId, int width) {
        return false;
      }

      @Override
      public Boolean year(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean month(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean day(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean hour(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean alwaysNull(int fieldId, String sourceName, int sourceId) {
        return false;
      }

      @Override
      public Boolean unknown(int fieldId, String sourceName, int sourceId, String transform) {
        return false;
      }
    });

    return bucketList.stream().anyMatch(Boolean::booleanValue);
  }

  /**
   * Create a sort order that will group data for a partition spec.
   * <p>
   * If the partition spec contains bucket columns, the sort order will also have a field to sort by a column that is
   * bucketed in the spec. The column is selected by the highest number of buckets in the transform.
   *
   * @param spec a partition spec
   * @return a sort order that will cluster data for the spec
   */
  public static SortOrder sortOrderFor(PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return SortOrder.unsorted();
    }

    SortOrder.Builder builder = SortOrder.builderFor(spec.schema());
    SpecToOrderVisitor converter = new SpecToOrderVisitor(builder);
    PartitionSpecVisitor.visit(spec, converter);

    // columns used for bucketing are high cardinality; add one to the sort at the end
    String bucketColumn = converter.bucketColumn();
    if (bucketColumn != null) {
      builder.asc(bucketColumn);
    }

    return builder.build();
  }

  private static class SpecToOrderVisitor implements PartitionSpecVisitor<Void> {
    private final SortOrder.Builder builder;
    private String bucketColumn = null;
    private int highestNumBuckets = 0;

    private SpecToOrderVisitor(SortOrder.Builder builder) {
      this.builder = builder;
    }

    String bucketColumn() {
      return bucketColumn;
    }

    @Override
    public Void identity(int fieldId, String sourceName, int sourceId) {
      builder.asc(sourceName);
      return null;
    }

    @Override
    public Void bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
      // the column with highest cardinality is usually the one with the highest number of buckets
      if (numBuckets > highestNumBuckets) {
        this.highestNumBuckets = numBuckets;
        this.bucketColumn = sourceName;
      }
      builder.asc(Expressions.bucket(sourceName, numBuckets));
      return null;
    }

    @Override
    public Void truncate(int fieldId, String sourceName, int sourceId, int width) {
      builder.asc(Expressions.truncate(sourceName, width));
      return null;
    }

    @Override
    public Void year(int fieldId, String sourceName, int sourceId) {
      builder.asc(Expressions.year(sourceName));
      return null;
    }

    @Override
    public Void month(int fieldId, String sourceName, int sourceId) {
      builder.asc(Expressions.month(sourceName));
      return null;
    }

    @Override
    public Void day(int fieldId, String sourceName, int sourceId) {
      builder.asc(Expressions.day(sourceName));
      return null;
    }

    @Override
    public Void hour(int fieldId, String sourceName, int sourceId) {
      builder.asc(Expressions.hour(sourceName));
      return null;
    }

    @Override
    public Void alwaysNull(int fieldId, String sourceName, int sourceId) {
      // do nothing for alwaysNull, it doesn't need to be added to the sort
      return null;
    }
  }

  /**
   * Builds a common partition type for all specs in a table.
   * <p>
   * Whenever a table has multiple specs, the partition type is a struct containing
   * all columns that have ever been a part of any spec in the table.
   *
   * @param table a table with one or many specs
   * @return the constructed common partition type
   */
  public static StructType partitionType(Table table) {
    // we currently don't know the output type of unknown transforms
    List<Transform<?, ?>> unknownTransforms = collectUnknownTransforms(table);
    ValidationException.check(unknownTransforms.isEmpty(),
        "Cannot build table partition type, unknown transforms: %s", unknownTransforms);

    if (table.specs().size() == 1) {
      return table.spec().partitionType();
    }

    Map<Integer, PartitionField> fieldMap = Maps.newHashMap();
    List<NestedField> structFields = Lists.newArrayList();

    // sort the spec IDs in descending order to pick up the most recent field names
    List<Integer> specIds = table.specs().keySet().stream()
        .sorted(Collections.reverseOrder())
        .collect(Collectors.toList());

    for (Integer specId : specIds) {
      PartitionSpec spec = table.specs().get(specId);

      for (PartitionField field : spec.fields()) {
        int fieldId = field.fieldId();
        PartitionField existingField = fieldMap.get(fieldId);

        if (existingField == null) {
          fieldMap.put(fieldId, field);
          NestedField structField = spec.partitionType().field(fieldId);
          structFields.add(structField);
        } else {
          // verify the fields are compatible as they may conflict in v1 tables
          ValidationException.check(equivalentIgnoringNames(field, existingField),
              "Conflicting partition fields: ['%s', '%s']",
              field, existingField);
        }
      }
    }

    List<NestedField> sortedStructFields = structFields.stream()
        .sorted(Comparator.comparingInt(NestedField::fieldId))
        .collect(Collectors.toList());
    return StructType.of(sortedStructFields);
  }

  private static List<Transform<?, ?>> collectUnknownTransforms(Table table) {
    List<Transform<?, ?>> unknownTransforms = Lists.newArrayList();

    table.specs().values().forEach(spec -> {
      spec.fields().stream()
          .map(PartitionField::transform)
          .filter(transform -> transform instanceof UnknownTransform)
          .forEach(unknownTransforms::add);
    });

    return unknownTransforms;
  }

  private static boolean equivalentIgnoringNames(PartitionField field, PartitionField anotherField) {
    return field.fieldId() == anotherField.fieldId() &&
        field.sourceId() == anotherField.sourceId() &&
        compatibleTransforms(field.transform(), anotherField.transform());
  }

  private static boolean compatibleTransforms(Transform<?, ?> t1, Transform<?, ?> t2) {
    return t1.equals(t2) || t1.equals(Transforms.alwaysNull()) || t2.equals(Transforms.alwaysNull());
  }
}
