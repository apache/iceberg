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

import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class PartitionUtil {
  private PartitionUtil() {}

  public static Map<Integer, ?> constantsMap(ContentScanTask<?> task) {
    return constantsMap(task, null, (type, constant) -> constant);
  }

  public static Map<Integer, ?> constantsMap(
      ContentScanTask<?> task, BiFunction<Type, Object, Object> convertConstant) {
    return constantsMap(task, null, convertConstant);
  }

  public static Map<Integer, ?> constantsMap(
      ContentScanTask<?> task,
      Types.StructType partitionType,
      BiFunction<Type, Object, Object> convertConstant) {
    PartitionSpec spec = task.spec();
    StructLike partitionData = task.file().partition();

    // use java.util.HashMap because partition data may contain null values
    Map<Integer, Object> idToConstant = Maps.newHashMap();

    // add first_row_id as _row_id
    if (task.file().firstRowId() != null) {
      idToConstant.put(
          MetadataColumns.ROW_ID.fieldId(),
          convertConstant.apply(Types.LongType.get(), task.file().firstRowId()));
    }

    idToConstant.put(
        MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
        convertConstant.apply(Types.LongType.get(), task.file().fileSequenceNumber()));

    // add _file
    idToConstant.put(
        MetadataColumns.FILE_PATH.fieldId(),
        convertConstant.apply(Types.StringType.get(), task.file().location()));

    // add _spec_id
    idToConstant.put(
        MetadataColumns.SPEC_ID.fieldId(),
        convertConstant.apply(Types.IntegerType.get(), task.file().specId()));

    // add _partition
    if (partitionType != null) {
      if (!partitionType.fields().isEmpty()) {
        StructLike coercedPartition = coercePartition(partitionType, spec, partitionData);
        idToConstant.put(
            MetadataColumns.PARTITION_COLUMN_ID,
            convertConstant.apply(partitionType, coercedPartition));
      } else {
        // use null as some query engines may not be able to handle empty structs
        idToConstant.put(MetadataColumns.PARTITION_COLUMN_ID, null);
      }
    }

    List<Types.NestedField> partitionFields = spec.partitionType().fields();
    List<PartitionField> fields = spec.fields();
    for (int pos = 0; pos < fields.size(); pos += 1) {
      PartitionField field = fields.get(pos);
      if (field.transform().isIdentity()) {
        Object converted =
            convertConstant.apply(
                partitionFields.get(pos).type(), partitionData.get(pos, Object.class));
        idToConstant.put(field.sourceId(), converted);
      }
    }

    return idToConstant;
  }

  // adapts the provided partition data to match the table partition type
  public static StructLike coercePartition(
      Types.StructType partitionType, PartitionSpec spec, StructLike partition) {
    StructProjection projection =
        StructProjection.createAllowMissing(spec.partitionType(), partitionType);
    projection.wrap(partition);
    return projection;
  }

  public static Map<Integer, PartitionSpec> indexSpecs(List<PartitionSpec> specs) {
    ImmutableMap.Builder<Integer, PartitionSpec> builder = ImmutableMap.builder();
    for (PartitionSpec spec : specs) {
      builder.put(spec.specId(), spec);
    }

    return builder.build();
  }

  // Return a function that extracts partition data from the source spec to the output spec.
  public static UnaryOperator<StructLike> convertPartitionFunc(
      PartitionSpec sourceSpec, PartitionSpec outputSpec) {

    if (sourceSpec.equals(outputSpec)) {
      return partition -> partition;
    }

    List<SimpleEntry<Integer, PartitionField>> sourceFieldsIndexedByPos =
        IntStream.range(0, sourceSpec.fields().size())
            .mapToObj(i -> new SimpleEntry<>(i, sourceSpec.fields().get(i)))
            .collect(Collectors.toList());
    Map<Integer, List<SimpleEntry<Integer, PartitionField>>> bySourceId =
        sourceFieldsIndexedByPos.stream()
            .collect(Collectors.groupingBy(entry -> entry.getValue().sourceId()));

    // a function to calculate the output partition from a source partition
    return (StructLike inPartition) -> {
      StructLike outPartition = GenericRecord.create(outputSpec.partitionType());

      // fill the output partition with the source partition data on a best-effort basis.
      // some fields can be null, and it's ok.
      for (int outIdx = 0; outIdx < outputSpec.fields().size(); outIdx++) {
        PartitionField outField = outputSpec.fields().get(outIdx);
        int finalOutIdx = outIdx;
        bySourceId
            .getOrDefault(outField.sourceId(), Collections.emptyList())
            .forEach(
                entry -> {
                  int pos = entry.getKey();
                  PartitionField inField = entry.getValue();
                  Object inValue = inPartition.get(pos, Object.class);
                  Object outValue =
                      convertPartitionValue(inField.transform(), outField.transform(), inValue);
                  outPartition.set(finalOutIdx, outValue);
                });
      }
      return outPartition;
    };
  }

  // repartition is needed if partition of output spec can not be fully derived from partition of
  // sourceSpec
  public static boolean needRepartition(PartitionSpec sourceSpec, PartitionSpec outputSpec) {
    Map<Integer, List<PartitionField>> sourcePartitionFields =
        sourceSpec.fields().stream().collect(Collectors.groupingBy(PartitionField::sourceId));
    Map<Integer, List<PartitionField>> outputPartitionFields =
        outputSpec.fields().stream().collect(Collectors.groupingBy(PartitionField::sourceId));

    if (!sourcePartitionFields.keySet().containsAll(outputPartitionFields.keySet())) {
      return true;
    }

    boolean canDerivePartition =
        outputSpec.fields().stream()
            .allMatch(
                outField ->
                    sourcePartitionFields.get(outField.sourceId()).stream()
                        .anyMatch(
                            inField ->
                                isSupportedCoarsePartitionTransform(
                                    inField.transform(), outField.transform())));

    return !canDerivePartition;
  }

  private static boolean isSupportedCoarsePartitionTransform(
      Transform<?, ?> inTransform, Transform<?, ?> outTransform) {

    if (!inTransform.satisfiesOrderOf(outTransform)) {
      return false;
    }

    if (inTransform.isIdentity() && outTransform.isIdentity()) {
      return true;
    }
    if (isTimeTransform(inTransform, outTransform)) {
      return true;
    }
    if (isTruncateString(inTransform, outTransform)) {
      return true;
    }

    return false;
  }

  private static Object convertPartitionValue(
      Transform<?, ?> inTransform, Transform<?, ?> outTransform, Object inValue) {

    if (inValue == null) {
      return null;
    }

    if (inTransform.equals(outTransform)) {
      return inValue;
    }
    if (isTimeTransform(inTransform, outTransform)) {
      return convertTimeTransformPartitionValue(inTransform, outTransform, (Integer) inValue);
    }
    if (isTruncateString(inTransform, outTransform)) {
      return convertTruncateStringPartitionValue(outTransform, (String) inValue);
    }

    return null;
  }

  private static boolean isTimeTransform(
      Transform<?, ?> inTransform, Transform<?, ?> outTransform) {
    return Transforms.isTimeTransform(inTransform)
        && Transforms.isTimeTransform(outTransform)
        && inTransform.satisfiesOrderOf(outTransform);
  }

  private static Object convertTimeTransformPartitionValue(
      Transform<?, ?> inTransform, Transform<?, ?> outTransform, Integer inValue) {
    if (inTransform.granularity() == ChronoUnit.HOURS
        && outTransform.granularity() == ChronoUnit.DAYS) {
      return DateTimeUtil.hoursToDays(inValue);
    }

    if (inTransform.granularity() == ChronoUnit.DAYS
        && outTransform.granularity() == ChronoUnit.MONTHS) {
      return DateTimeUtil.daysToMonths(inValue);
    }
    if (inTransform.granularity() == ChronoUnit.DAYS
        && outTransform.granularity() == ChronoUnit.YEARS) {
      return DateTimeUtil.daysToYears(inValue);
    }
    return null;
  }

  private static boolean isTruncateString(
      Transform<?, ?> inTransform, Transform<?, ?> outTransform) {
    // XXX: only TruncateString defined satisfiesOrderOf()
    return Transforms.isTruncate(inTransform)
        && Transforms.isTruncate(outTransform)
        && inTransform.satisfiesOrderOf(outTransform);
  }

  private static Object convertTruncateStringPartitionValue(
      Transform<?, ?> outTransform, String inValue) {
    if (!Transforms.isTruncate(outTransform)) {
      throw new IllegalArgumentException(outTransform.toString() + " is not a truncate transform");
    }
    return UnicodeUtil.truncateString(inValue, outTransform.truncateWidth()).toString();
  }
}
