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
package org.apache.iceberg.avro;

import java.util.Deque;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class AvroWithPartnerVisitor<P, R> {
  public interface PartnerAccessors<P> {
    P fieldPartner(P partnerStruct, Integer fieldId, String name);

    P mapKeyPartner(P partnerMap);

    P mapValuePartner(P partnerMap);

    P listElementPartner(P partnerList);
  }

  static class FieldIDAccessors implements AvroWithPartnerVisitor.PartnerAccessors<Type> {
    private static final FieldIDAccessors INSTANCE = new FieldIDAccessors();

    public static FieldIDAccessors get() {
      return INSTANCE;
    }

    @Override
    public Type fieldPartner(Type partner, Integer fieldId, String name) {
      Types.NestedField field = partner.asStructType().field(fieldId);
      return field != null ? field.type() : null;
    }

    @Override
    public Type mapKeyPartner(Type partner) {
      return partner.asMapType().keyType();
    }

    @Override
    public Type mapValuePartner(Type partner) {
      return partner.asMapType().valueType();
    }

    @Override
    public Type listElementPartner(Type partner) {
      return partner.asListType().elementType();
    }
  }

  /** Used to fail on recursive types. */
  private Deque<String> recordLevels = Lists.newLinkedList();

  public R record(P partner, Schema record, List<R> fieldResults) {
    return null;
  }

  public R union(P partner, Schema union, List<R> optionResults) {
    return null;
  }

  public R array(P partner, Schema array, R elementResult) {
    return null;
  }

  public R arrayMap(P partner, Schema map, R keyResult, R valueResult) {
    return null;
  }

  public R map(P partner, Schema map, R valueResult) {
    return null;
  }

  public R primitive(P partner, Schema primitive) {
    return null;
  }

  public static <P, R> R visit(
      P partner,
      Schema schema,
      AvroWithPartnerVisitor<P, R> visitor,
      PartnerAccessors<P> accessors) {
    switch (schema.getType()) {
      case RECORD:
        return visitRecord(partner, schema, visitor, accessors);

      case UNION:
        return visitUnion(partner, schema, visitor, accessors);

      case ARRAY:
        return visitArray(partner, schema, visitor, accessors);

      case MAP:
        return visitor.map(
            partner,
            schema,
            visit(
                partner != null ? accessors.mapValuePartner(partner) : null,
                schema.getValueType(),
                visitor,
                accessors));

      default:
        return visitor.primitive(partner, schema);
    }
  }

  private static <P, R> R visitRecord(
      P partnerStruct,
      Schema record,
      AvroWithPartnerVisitor<P, R> visitor,
      PartnerAccessors<P> accessors) {
    // check to make sure this hasn't been visited before
    String recordName = record.getFullName();
    Preconditions.checkState(
        !visitor.recordLevels.contains(recordName),
        "Cannot process recursive Avro record %s",
        recordName);
    visitor.recordLevels.push(recordName);

    List<Schema.Field> fields = record.getFields();
    List<R> results = Lists.newArrayListWithExpectedSize(fields.size());
    for (int pos = 0; pos < fields.size(); pos += 1) {
      Schema.Field field = fields.get(pos);
      Integer fieldId = AvroSchemaUtil.fieldId(field);

      P fieldPartner =
          partnerStruct != null && fieldId != null
              ? accessors.fieldPartner(partnerStruct, fieldId, field.name())
              : null;
      results.add(visit(fieldPartner, field.schema(), visitor, accessors));
    }

    visitor.recordLevels.pop();

    return visitor.record(partnerStruct, record, results);
  }

  private static <P, R> R visitUnion(
      P partner,
      Schema union,
      AvroWithPartnerVisitor<P, R> visitor,
      PartnerAccessors<P> accessors) {
    Preconditions.checkArgument(
        AvroSchemaUtil.isOptionSchema(union), "Cannot visit non-option union: %s", union);

    List<Schema> types = union.getTypes();
    List<R> options = Lists.newArrayListWithExpectedSize(types.size());
    for (Schema branch : types) {
      options.add(visit(partner, branch, visitor, accessors));
    }

    return visitor.union(partner, union, options);
  }

  private static <P, R> R visitArray(
      P partnerArray,
      Schema array,
      AvroWithPartnerVisitor<P, R> visitor,
      PartnerAccessors<P> accessors) {
    if (array.getLogicalType() instanceof LogicalMap) {
      Preconditions.checkState(
          AvroSchemaUtil.isKeyValueSchema(array.getElementType()),
          "Cannot visit invalid logical map type: %s",
          array);

      List<Schema.Field> keyValueFields = array.getElementType().getFields();
      return visitor.arrayMap(
          partnerArray,
          array,
          visit(
              partnerArray != null ? accessors.mapKeyPartner(partnerArray) : null,
              keyValueFields.get(0).schema(),
              visitor,
              accessors),
          visit(
              partnerArray != null ? accessors.mapValuePartner(partnerArray) : null,
              keyValueFields.get(1).schema(),
              visitor,
              accessors));

    } else {
      return visitor.array(
          partnerArray,
          array,
          visit(
              partnerArray != null ? accessors.listElementPartner(partnerArray) : null,
              array.getElementType(),
              visitor,
              accessors));
    }
  }
}
