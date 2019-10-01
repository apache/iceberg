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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PruneColumns extends AvroSchemaVisitor<Schema> {
  private static final Logger LOG = LoggerFactory.getLogger(PruneColumns.class);

  private final Set<Integer> selectedIds;
  private final NameMapping nameMapping;

  PruneColumns(Set<Integer> selectedIds, NameMapping nameMapping) {
    this.selectedIds = selectedIds;
    this.nameMapping = nameMapping;
  }

  Schema rootSchema(Schema record) {
    Schema result = visit(record, this);
    if (result != null) {
      return result;
    }

    return copyRecord(record, ImmutableList.of());
  }

  @Override
  public Schema record(Schema record, List<String> names, List<Schema> fields) {
    // Then this should access the record's fields by name
    List<Schema.Field> filteredFields = Lists.newArrayListWithExpectedSize(fields.size());
    boolean hasChange = false;
    for (Schema.Field field : record.getFields()) {
      Integer fieldId = fieldId(field);
      if (fieldId == null) {
        // both the schema and the nameMapping does not have field id. We prune this field.
        continue;
      }

      if (!AvroSchemaUtil.hasFieldId(field)) {
        hasChange = true;
      }

      Schema fieldSchema = fields.get(field.pos());
      // All primitives are selected by selecting the field, but map and list
      // types can be selected by projecting the keys, values, or elements.
      // This creates two conditions where the field should be selected: if the
      // id is selected or if the result of the field is non-null. The only
      // case where the converted field is non-null is when a map or list is
      // selected by lower IDs.
      if (selectedIds.contains(fieldId)) {
        filteredFields.add(copyField(field, field.schema(), fieldId));
      } else if (fieldSchema != null) {
        filteredFields.add(copyField(field, fieldSchema, fieldId));
      }
    }

    if (hasChange) {
      return copyRecord(record, filteredFields);
    } else if (filteredFields.size() == record.getFields().size()) {
      return record;
    } else if (!filteredFields.isEmpty()) {
      return copyRecord(record, filteredFields);
    }

    return null;
  }

  @Override
  public Schema union(Schema union, List<Schema> options) {
    Preconditions.checkState(AvroSchemaUtil.isOptionSchema(union),
        "Invalid schema: non-option unions are not supported: %s", union);

    // only unions with null are allowed, and a null schema results in null
    Schema pruned = null;
    if (options.get(0) != null) {
      pruned = options.get(0);
    } else if (options.get(1) != null) {
      pruned = options.get(1);
    }

    if (pruned != null) {
      if (pruned != AvroSchemaUtil.fromOption(union)) {
        return AvroSchemaUtil.toOption(pruned);
      }
      return union;
    }

    return null;
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public Schema array(Schema array, Schema element) {
    if (array.getLogicalType() instanceof LogicalMap || AvroSchemaUtil.isKeyValueSchema(array.getElementType())) {
      Schema keyValue = array.getElementType();
      Integer keyId = fieldId(keyValue.getField("key"));
      Integer valueId = fieldId(keyValue.getField("value"));
      if (keyId == null) {
        LOG.warn("Map schema {} has value id but not key id", array);
        return null;
      }

      // if either key or value is selected, the whole map must be projected
      if (selectedIds.contains(keyId) || selectedIds.contains(valueId)) {
        return complexMapWithIds(array, keyId, valueId);
      } else if (element != null) {
        if (keyValue.getField("key").schema() != element.getField("key").schema() ||
            keyValue.getField("value").schema() != element.getField("value").schema()) {
          // the value must be a projection
          return AvroSchemaUtil.createMap(
              keyId, element.getField("key").schema(),
              valueId, element.getField("value").schema());
        } else {
          return complexMapWithIds(array, keyId, valueId);
        }
      }

    } else {
      Integer elementId = id(array, AvroSchemaUtil.ELEMENT_ID_PROP, "element");
      if (elementId == null) {
        return null;
      }

      if (selectedIds.contains(elementId)) {
        return arrayWithId(array, elementId);
      } else if (element != null) {
        if (element != array.getElementType()) {
          // the element must be a projection
          return arrayWithId(Schema.createArray(element), elementId);
        }
        return arrayWithId(array, elementId);
      }
    }

    return null;
  }

  @Override
  public Schema map(Schema map, Schema value) {
    Integer keyId = id(map, AvroSchemaUtil.KEY_ID_PROP, "key");
    Integer valueId = id(map, AvroSchemaUtil.VALUE_ID_PROP, "value");
    if (keyId == null) {
      LOG.warn("Map schema {} has value-id but not key-id", map);
      return null;
    }
    // if either key or value is selected, the whole map must be projected
    if (selectedIds.contains(keyId) || selectedIds.contains(valueId)) {
      // Assign ids. Ids may not always be present in the schema
      return mapWithIds(map, keyId, valueId);
    } else if (value != null) {
      if (value != map.getValueType()) {
        // the value must be a projection
        return mapWithIds(Schema.createMap(value), keyId, valueId);
      }
      return map;
    }

    return null;
  }

  private Schema arrayWithId(Schema array, Integer elementId) {
    if (!AvroSchemaUtil.hasProperty(array, AvroSchemaUtil.ELEMENT_ID_PROP)) {
      Schema result = Schema.createArray(array.getElementType());
      result.addProp(AvroSchemaUtil.ELEMENT_ID_PROP, elementId);
      return result;
    }
    return array;
  }

  private Schema complexMapWithIds(Schema map, Integer keyId, Integer valueId) {
    Schema keyValue = map.getElementType();
    if (!AvroSchemaUtil.hasFieldId(keyValue.getField("key"))) {
      return AvroSchemaUtil.createMap(
          keyId, keyValue.getField("key").schema(),
          valueId, keyValue.getField("value").schema());
    }
    return map;
  }

  private Schema mapWithIds(Schema map, Integer keyId, Integer valueId) {
    if (!AvroSchemaUtil.hasProperty(map, AvroSchemaUtil.KEY_ID_PROP)) {
      Schema result = Schema.createMap(map.getValueType());
      result.addProp(AvroSchemaUtil.KEY_ID_PROP, keyId);
      result.addProp(AvroSchemaUtil.VALUE_ID_PROP, valueId);
      return result;
    }
    return map;
  }

  @Override
  public Schema primitive(Schema primitive) {
    // primitives are not selected directly
    return null;
  }

  private Integer id(Schema schema, String propertyName, String mappedName) {
    if (AvroSchemaUtil.hasProperty(schema, propertyName)) {
      return AvroSchemaUtil.getId(schema, propertyName);
    } else {
      MappedField mappedField = mappedField(mappedName);
      if (mappedField != null) {
        return mappedField.id();
      } else {
        return null;
      }
    }
  }

  private Integer fieldId(Schema.Field field) {
    if (AvroSchemaUtil.hasFieldId(field)) {
      return AvroSchemaUtil.getFieldId(field);
    } else {
      MappedField mappedField = mappedField(field.name());
      if (mappedField != null) {
        return mappedField.id();
      } else {
        return null;
      }
    }
  }

  private MappedField mappedField(String fieldName) {
    Preconditions.checkState(nameMapping != null,
        "Cannot find mapped field for field name %s. NameMapping is null", fieldName);
    List<String> fieldNames = Lists.newArrayList(fieldNames());
    fieldNames.add(fieldName);
    return nameMapping.find(fieldNames);
  }

  private static Schema copyRecord(Schema record, List<Schema.Field> newFields) {
    Schema copy = Schema.createRecord(record.getName(),
        record.getDoc(), record.getNamespace(), record.isError(), newFields);

    for (Map.Entry<String, Object> prop : record.getObjectProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }

    return copy;
  }

  private static Schema.Field copyField(Schema.Field field, Schema newSchema, Integer fieldId) {
    Schema.Field copy = new Schema.Field(field.name(),
        newSchema, field.doc(), field.defaultVal(), field.order());

    for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }

    if (!AvroSchemaUtil.hasFieldId(field)) {
      // field may not have a fieldId if the fieldId was fetched from nameMapping
      copy.addProp(AvroSchemaUtil.FIELD_ID_PROP, fieldId);
    }

    return copy;
  }
}
