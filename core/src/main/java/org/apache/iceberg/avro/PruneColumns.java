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

class PruneColumns extends AvroSchemaVisitor<Schema> {
  private final Set<Integer> selectedIds;

  PruneColumns(Set<Integer> selectedIds) {
    this.selectedIds = selectedIds;
  }

  public Schema rootSchema(Schema record) {
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
      int fieldId = AvroSchemaUtil.getFieldId(field);
      Schema fieldSchema = fields.get(field.pos());
      // All primitives are selected by selecting the field, but map and list
      // types can be selected by projecting the keys, values, or elements.
      // This creates two conditions where the field should be selected: if the
      // id is selected or if the result of the field is non-null. The only
      // case where the converted field is non-null is when a map or list is
      // selected by lower IDs.
      if (selectedIds.contains(fieldId)) {
        filteredFields.add(copyField(field, field.schema()));
      } else if (fieldSchema != null) {
        hasChange = true;
        filteredFields.add(copyField(field, fieldSchema));
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
  public Schema array(Schema array, Schema element) {
    if (array.getLogicalType() instanceof LogicalMap) {
      Schema keyValue = array.getElementType();
      int keyId = AvroSchemaUtil.getFieldId(keyValue.getField("key"));
      int valueId = AvroSchemaUtil.getFieldId(keyValue.getField("value"));

      // if either key or value is selected, the whole map must be projected
      if (selectedIds.contains(keyId) || selectedIds.contains(valueId)) {
        return array;
      } else if (element != null) {
        if (keyValue.getField("value").schema() != element.getField("value").schema()) {
          // the value must be a projection
          return AvroSchemaUtil.createMap(
              keyId, keyValue.getField("key").schema(),
              valueId, element.getField("value").schema());
        } else {
          return array;
        }
      }

    } else {
      int elementId = AvroSchemaUtil.getElementId(array);
      if (selectedIds.contains(elementId)) {
        return array;
      } else if (element != null) {
        if (element != array.getElementType()) {
          // the element must be a projection
          return Schema.createArray(element);
        }
        return array;
      }
    }

    return null;
  }

  @Override
  public Schema map(Schema map, Schema value) {
    int keyId = AvroSchemaUtil.getKeyId(map);
    int valueId = AvroSchemaUtil.getValueId(map);
    // if either key or value is selected, the whole map must be projected
    if (selectedIds.contains(keyId) || selectedIds.contains(valueId)) {
      return map;
    } else if (value != null) {
      if (value != map.getValueType()) {
        // the value must be a projection
        return Schema.createMap(value);
      }
      return map;
    }

    return null;
  }

  @Override
  public Schema primitive(Schema primitive) {
    // primitives are not selected directly
    return null;
  }

  private static Schema copyRecord(Schema record, List<Schema.Field> newFields) {
    Schema copy = Schema.createRecord(record.getName(),
        record.getDoc(), record.getNamespace(), record.isError(), newFields);

    for (Map.Entry<String, Object> prop : record.getObjectProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }

    return copy;
  }

  private static Schema.Field copyField(Schema.Field field, Schema newSchema) {
    Schema.Field copy = new Schema.Field(field.name(),
        newSchema, field.doc(), field.defaultVal(), field.order());

    for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
      copy.addProp(prop.getKey(), prop.getValue());
    }

    return copy;
  }
}
