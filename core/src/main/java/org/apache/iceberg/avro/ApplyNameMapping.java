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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * An Avro Schema visitor to apply a name mapping to add Iceberg field IDs.
 *
 * <p>Methods return null when a schema has no ID and cannot be projected.
 */
public class ApplyNameMapping extends AvroSchemaVisitor<Schema> {
  private final NameMapping nameMapping;

  public ApplyNameMapping(NameMapping nameMapping) {
    this.nameMapping = nameMapping;
  }

  @Override
  public Schema record(Schema record, List<String> names, List<Schema> fields) {
    List<Schema.Field> originalFields = record.getFields();

    List<Schema.Field> newFields = Lists.newArrayList();
    for (int i = 0; i < originalFields.size(); i += 1) {
      Schema newSchema = fields.get(i);
      if (newSchema != null) {
        Schema.Field field = originalFields.get(i);
        Integer fieldId = AvroSchemaUtil.getFieldId(field, nameMapping, fieldNames());
        if (fieldId != null) {
          // always copy because fields can't be reused
          newFields.add(copyField(field, newSchema, fieldId));
        }
      }
    }

    return copyRecord(record, newFields);
  }

  @Override
  public Schema union(Schema union, List<Schema> options) {
    if (options.equals(union.getTypes())) {
      return union;
    }

    List<Schema> validOptions =
        options.stream().filter(Objects::nonNull).collect(Collectors.toList());

    return copyProps(union, Schema.createUnion(validOptions));
  }

  @Override
  public Schema array(Schema array, Schema element) {
    if (array.getLogicalType() instanceof LogicalMap
        || (isKeyValueMapping(fieldNames())
            && AvroSchemaUtil.isKeyValueSchema(array.getElementType()))) {
      return copyProps(array, Schema.createArray(element));
    }

    Integer elementId = AvroSchemaUtil.elementId(array);
    if (elementId != null) {
      if (array.getElementType().equals(element)) {
        return array;
      }

      return copyProps(array, createArray(element, elementId));
    }

    MappedField mapping = nameMapping.find(fieldNames(), "element");
    if (mapping != null) {
      return copyProps(array, createArray(element, mapping.id()));
    }

    return null;
  }

  private boolean isKeyValueMapping(Iterable<String> names) {
    return nameMapping.find(names, "key") != null && nameMapping.find(names, "value") != null;
  }

  @Override
  public Schema map(Schema map, Schema value) {
    Integer keyId = AvroSchemaUtil.keyId(map);
    Integer valueId = AvroSchemaUtil.valueId(map);
    if (keyId != null && valueId != null) {
      if (map.getValueType().equals(value)) {
        return map;
      }

      return copyProps(map, createMap(value, keyId, valueId));
    }

    MappedField keyMapping = nameMapping.find(fieldNames(), "key");
    MappedField valueMapping = nameMapping.find(fieldNames(), "value");
    if (keyMapping != null && valueMapping != null) {
      return copyProps(map, createMap(value, keyMapping.id(), valueMapping.id()));
    }

    return null;
  }

  @Override
  public Schema primitive(Schema primitive) {
    return primitive;
  }

  private Schema copyRecord(Schema record, List<Schema.Field> newFields) {
    Schema copy =
        Schema.createRecord(
            record.getName(), record.getDoc(), record.getNamespace(), record.isError(), newFields);

    copyProps(record, copy);

    return copy;
  }

  static Schema.Field copyField(Schema.Field field, Schema newSchema, int fieldId) {
    Schema.Field copy =
        new Schema.Field(field.name(), newSchema, field.doc(), field.defaultVal(), field.order());

    for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }

    copy.addProp(AvroSchemaUtil.FIELD_ID_PROP, fieldId);

    for (String alias : field.aliases()) {
      copy.addAlias(alias);
    }

    return copy;
  }

  private Schema createMap(Schema value, int keyId, int valueId) {
    Schema result = Schema.createMap(value);
    result.addProp(AvroSchemaUtil.KEY_ID_PROP, keyId);
    result.addProp(AvroSchemaUtil.VALUE_ID_PROP, valueId);
    return result;
  }

  private Schema createArray(Schema element, int elementId) {
    Schema result = Schema.createArray(element);
    result.addProp(AvroSchemaUtil.ELEMENT_ID_PROP, elementId);
    return result;
  }

  private Schema copyProps(Schema from, Schema copy) {
    for (Map.Entry<String, Object> prop : from.getObjectProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }

    LogicalType logicalType = from.getLogicalType();
    if (logicalType != null) {
      logicalType.addToSchema(copy);
    }

    return copy;
  }
}
