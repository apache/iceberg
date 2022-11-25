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

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

public class Partitioning {
  private Partitioning() {}

  /**
   * Check whether the spec contains a bucketed partition field.
   *
   * @param spec a partition spec
   * @return true if the spec has field with a bucket transform
   */
  public static boolean hasBucketField(PartitionSpec spec) {
    List<Boolean> bucketList =
        PartitionSpecVisitor.visit(
            spec,
            new PartitionSpecVisitor<Boolean>() {
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
              public Boolean unknown(
                  int fieldId, String sourceName, int sourceId, String transform) {
                return false;
              }
            });

    return bucketList.stream().anyMatch(Boolean::booleanValue);
  }

  /**
   * Create a sort order that will group data for a partition spec.
   *
   * <p>If the partition spec contains bucket columns, the sort order will also have a field to sort
   * by a column that is bucketed in the spec. The column is selected by the highest number of
   * buckets in the transform.
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
   * Builds a grouping key type considering all provided specs.
   *
   * <p>A grouping key defines how data is split between files and consists of partition fields with
   * non-void transforms that are present in each provided spec. Iceberg guarantees that records
   * with different values for the grouping key are disjoint and are stored in separate files.
   *
   * <p>If there is only one spec, the grouping key will include all partition fields with non-void
   * transforms from that spec. Whenever there are multiple specs, the grouping key will represent
   * an intersection of all partition fields with non-void transforms. If a partition field is
   * present only in a subset of specs, Iceberg cannot guarantee data distribution on that field.
   * That's why it will not be part of the grouping key. Unpartitioned tables or tables with
   * non-overlapping specs have empty grouping keys.
   *
   * <p>When partition fields are dropped in v1 tables, they are replaced with new partition fields
   * that have the same field ID but use a void transform under the hood. Such fields cannot be part
   * of the grouping key as void transforms always return null.
   *
   * @param specs one or many specs
   * @return the constructed grouping key type
   */
  public static StructType groupingKeyType(Collection<PartitionSpec> specs) {
    return buildPartitionProjectionType("grouping key", specs, commonActiveFieldIds(specs));
  }

  /**
   * Builds a unified partition type considering all specs in a table.
   *
   * <p>If there is only one spec, the partition type is that spec's partition type. Whenever there
   * are multiple specs, the partition type is a struct containing all fields that have ever been a
   * part of any spec in the table. In other words, the struct fields represent a union of all known
   * partition fields.
   *
   * @param table a table with one or many specs
   * @return the constructed unified partition type
   */
  public static StructType partitionType(Table table) {
    Collection<PartitionSpec> specs = table.specs().values();
    return buildPartitionProjectionType("table partition", specs, allFieldIds(specs));
  }

  private static StructType buildPartitionProjectionType(
      String typeName, Collection<PartitionSpec> specs, Set<Integer> projectedFieldIds) {

    // we currently don't know the output type of unknown transforms
    List<Transform<?, ?>> unknownTransforms = collectUnknownTransforms(specs);
    ValidationException.check(
        unknownTransforms.isEmpty(),
        "Cannot build %s type, unknown transforms: %s",
        typeName,
        unknownTransforms);

    Map<Integer, PartitionField> fieldMap = Maps.newHashMap();
    Map<Integer, Type> typeMap = Maps.newHashMap();
    Map<Integer, String> nameMap = Maps.newHashMap();

    // sort specs by ID in descending order to pick up the most recent field names
    List<PartitionSpec> sortedSpecs =
        specs.stream()
            .sorted(Comparator.comparingLong(PartitionSpec::specId).reversed())
            .collect(Collectors.toList());

    for (PartitionSpec spec : sortedSpecs) {
      for (PartitionField field : spec.fields()) {
        int fieldId = field.fieldId();

        if (!projectedFieldIds.contains(fieldId)) {
          continue;
        }

        NestedField structField = spec.partitionType().field(fieldId);
        PartitionField existingField = fieldMap.get(fieldId);

        if (existingField == null) {
          fieldMap.put(fieldId, field);
          typeMap.put(fieldId, structField.type());
          nameMap.put(fieldId, structField.name());

        } else {
          // verify the fields are compatible as they may conflict in v1 tables
          ValidationException.check(
              equivalentIgnoringNames(field, existingField),
              "Conflicting partition fields: ['%s', '%s']",
              field,
              existingField);

          // use the correct type for dropped partitions in v1 tables
          if (isVoidTransform(existingField) && !isVoidTransform(field)) {
            fieldMap.put(fieldId, field);
            typeMap.put(fieldId, structField.type());
          }
        }
      }
    }

    List<NestedField> sortedStructFields =
        fieldMap.keySet().stream()
            .sorted(Comparator.naturalOrder())
            .map(
                fieldId ->
                    NestedField.optional(fieldId, nameMap.get(fieldId), typeMap.get(fieldId)))
            .collect(Collectors.toList());
    return StructType.of(sortedStructFields);
  }

  private static boolean isVoidTransform(PartitionField field) {
    return field.transform().equals(Transforms.alwaysNull());
  }

  private static List<Transform<?, ?>> collectUnknownTransforms(Collection<PartitionSpec> specs) {
    List<Transform<?, ?>> unknownTransforms = Lists.newArrayList();

    for (PartitionSpec spec : specs) {
      spec.fields().stream()
          .map(PartitionField::transform)
          .filter(transform -> transform instanceof UnknownTransform)
          .forEach(unknownTransforms::add);
    }

    return unknownTransforms;
  }

  private static boolean equivalentIgnoringNames(
      PartitionField field, PartitionField anotherField) {
    return field.fieldId() == anotherField.fieldId()
        && field.sourceId() == anotherField.sourceId()
        && compatibleTransforms(field.transform(), anotherField.transform());
  }

  private static boolean compatibleTransforms(Transform<?, ?> t1, Transform<?, ?> t2) {
    return t1.equals(t2)
        || t1.equals(Transforms.alwaysNull())
        || t2.equals(Transforms.alwaysNull());
  }

  // collects IDs of all partition field used across specs
  private static Set<Integer> allFieldIds(Collection<PartitionSpec> specs) {
    return FluentIterable.from(specs)
        .transformAndConcat(PartitionSpec::fields)
        .transform(PartitionField::fieldId)
        .toSet();
  }

  // collects IDs of partition fields with non-void transforms that are present in each spec
  private static Set<Integer> commonActiveFieldIds(Collection<PartitionSpec> specs) {
    Set<Integer> commonActiveFieldIds = Sets.newHashSet();

    int specIndex = 0;
    for (PartitionSpec spec : specs) {
      if (specIndex == 0) {
        commonActiveFieldIds.addAll(activeFieldIds(spec));
      } else {
        commonActiveFieldIds.retainAll(activeFieldIds(spec));
      }

      specIndex++;
    }

    return commonActiveFieldIds;
  }

  private static List<Integer> activeFieldIds(PartitionSpec spec) {
    return spec.fields().stream()
        .filter(field -> !isVoidTransform(field))
        .map(PartitionField::fieldId)
        .collect(Collectors.toList());
  }
}
