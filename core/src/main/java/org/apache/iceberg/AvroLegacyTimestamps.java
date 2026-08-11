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
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Replaces the {@code local-timestamp-micros}/{@code local-timestamp-nanos} fields with the {@code
 * timestamp-micros}/{@code timestamp-nanos} plus {@code adjust-to-utc=false} encoding required by
 * the table spec for {@code timestamp}/{@code timestamp_ns} without a time zone in manifests and
 * manifest lists. The only way such a field can reach a manifest schema is through an {@code
 * identity} or {@code void} partition field.
 */
class AvroLegacyTimestamps {
  private AvroLegacyTimestamps() {}

  static Schema convert(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
        return convertRecord(schema);
      case UNION:
        return convertUnion(schema);
      case LONG:
        return legacyTimestamp(schema);
      default:
        return schema;
    }
  }

  @SuppressWarnings("ReferenceEquality")
  private static Schema convertRecord(Schema record) {
    List<Schema.Field> fields = record.getFields();
    Schema[] newFieldSchemas = new Schema[fields.size()];
    boolean changed = false;
    for (int i = 0; i < fields.size(); i += 1) {
      Schema fieldSchema = fields.get(i).schema();
      newFieldSchemas[i] = convert(fieldSchema);
      changed |= newFieldSchemas[i] != fieldSchema;
    }

    if (!changed) {
      return record;
    }

    List<Schema.Field> newFields = Lists.newArrayListWithExpectedSize(fields.size());
    for (int i = 0; i < fields.size(); i += 1) {
      newFields.add(new Schema.Field(fields.get(i), newFieldSchemas[i]));
    }

    Schema newRecord =
        Schema.createRecord(
            record.getName(), record.getDoc(), record.getNamespace(), record.isError(), newFields);
    record.getObjectProps().forEach(newRecord::addProp);
    return newRecord;
  }

  @SuppressWarnings("ReferenceEquality")
  private static Schema convertUnion(Schema union) {
    List<Schema> options = union.getTypes();
    List<Schema> newOptions = Lists.newArrayListWithExpectedSize(options.size());
    boolean changed = false;
    for (Schema option : options) {
      Schema newOption = convert(option);
      changed |= newOption != option;
      newOptions.add(newOption);
    }

    return changed ? Schema.createUnion(newOptions) : union;
  }

  private static Schema legacyTimestamp(Schema schema) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType instanceof LogicalTypes.LocalTimestampMicros) {
      return legacy(LogicalTypes.timestampMicros());
    } else if (logicalType instanceof LogicalTypes.LocalTimestampNanos) {
      return legacy(LogicalTypes.timestampNanos());
    }

    return schema;
  }

  private static Schema legacy(LogicalType logicalType) {
    Schema primitive = logicalType.addToSchema(Schema.create(Schema.Type.LONG));
    primitive.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, false);
    return primitive;
  }
}
