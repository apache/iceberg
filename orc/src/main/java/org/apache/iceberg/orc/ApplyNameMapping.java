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
package org.apache.iceberg.orc;

import java.util.List;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.orc.TypeDescription;

class ApplyNameMapping extends OrcSchemaVisitor<TypeDescription> {
  private final NameMapping nameMapping;

  ApplyNameMapping(NameMapping nameMapping) {
    this.nameMapping = nameMapping;
  }

  @Override
  public String elementName() {
    return "element";
  }

  @Override
  public String keyName() {
    return "key";
  }

  @Override
  public String valueName() {
    return "value";
  }

  TypeDescription setId(TypeDescription type, MappedField mappedField) {
    if (mappedField != null) {
      type.setAttribute(ORCSchemaUtil.ICEBERG_ID_ATTRIBUTE, mappedField.id().toString());
    }
    return type;
  }

  @Override
  public TypeDescription record(
      TypeDescription record, List<String> names, List<TypeDescription> fields) {
    Preconditions.checkArgument(names.size() == fields.size(), "All fields must have names");
    MappedField field = nameMapping.find(currentPath());
    TypeDescription structType = TypeDescription.createStruct();

    for (int i = 0; i < fields.size(); i++) {
      String fieldName = names.get(i);
      TypeDescription fieldType = fields.get(i);
      if (fieldType != null) {
        structType.addField(fieldName, fieldType);
      }
    }
    return setId(structType, field);
  }

  @Override
  public TypeDescription list(TypeDescription array, TypeDescription element) {
    Preconditions.checkArgument(element != null, "List type must have element type");

    MappedField field = nameMapping.find(currentPath());
    TypeDescription listType = TypeDescription.createList(element);
    return setId(listType, field);
  }

  @Override
  public TypeDescription map(TypeDescription map, TypeDescription key, TypeDescription value) {
    Preconditions.checkArgument(
        key != null && value != null, "Map type must have both key and value types");

    MappedField field = nameMapping.find(currentPath());
    TypeDescription mapType = TypeDescription.createMap(key, value);
    return setId(mapType, field);
  }

  @Override
  public TypeDescription primitive(TypeDescription primitive) {
    MappedField field = nameMapping.find(currentPath());
    return setId(primitive.clone(), field);
  }
}
