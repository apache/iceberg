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
package org.apache.iceberg.parquet;

import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

class ApplyNameMapping extends ParquetTypeVisitor<Type> {
  private static final String LIST_ELEMENT_NAME = "element";
  private static final String MAP_KEY_NAME = "key";
  private static final String MAP_VALUE_NAME = "value";
  private final NameMapping nameMapping;
  private final Deque<String> fieldNames = Lists.newLinkedList();

  ApplyNameMapping(NameMapping nameMapping) {
    this.nameMapping = nameMapping;
  }

  @Override
  public Type message(MessageType message, List<Type> fields) {
    Types.MessageTypeBuilder builder = Types.buildMessage();
    fields.stream().filter(Objects::nonNull).forEach(builder::addField);

    return builder.named(message.getName());
  }

  @Override
  public Type struct(GroupType struct, List<Type> types) {
    MappedField field = nameMapping.find(currentPath());
    List<Type> actualTypes = types.stream().filter(Objects::nonNull).collect(Collectors.toList());
    Type structType = struct.withNewFields(actualTypes);

    return field == null ? structType : structType.withId(field.id());
  }

  @Override
  public Type list(GroupType list, Type elementType) {
    Preconditions.checkArgument(elementType != null, "List type must have element field");

    Type listElement = ParquetSchemaUtil.determineListElementType(list);
    MappedField field = nameMapping.find(currentPath());

    Types.GroupBuilder<GroupType> listBuilder =
        Types.buildGroup(list.getRepetition()).as(LogicalTypeAnnotation.listType());
    if (listElement.isRepetition(Type.Repetition.REPEATED)) {
      listBuilder.addFields(elementType);
    } else {
      listBuilder.repeatedGroup().addFields(elementType).named(list.getFieldName(0));
    }
    Type listType = listBuilder.named(list.getName());

    return field == null ? listType : listType.withId(field.id());
  }

  @Override
  public Type map(GroupType map, Type keyType, Type valueType) {
    Preconditions.checkArgument(
        keyType != null && valueType != null, "Map type must have both key field and value field");

    MappedField field = nameMapping.find(currentPath());
    Type mapType =
        Types.buildGroup(map.getRepetition())
            .as(LogicalTypeAnnotation.mapType())
            .repeatedGroup()
            .addFields(keyType, valueType)
            .named(map.getFieldName(0))
            .named(map.getName());

    return field == null ? mapType : mapType.withId(field.id());
  }

  @Override
  public Type primitive(PrimitiveType primitive) {
    MappedField field = nameMapping.find(currentPath());
    return field == null ? primitive : primitive.withId(field.id());
  }

  @Override
  public void beforeField(Type type) {
    fieldNames.push(type.getName());
  }

  @Override
  public void afterField(Type type) {
    fieldNames.pop();
  }

  @Override
  public void beforeElementField(Type element) {
    // normalize the name to "element" so that the mapping will match structures with alternative
    // names
    fieldNames.push(LIST_ELEMENT_NAME);
  }

  @Override
  public void beforeKeyField(Type key) {
    // normalize the name to "key" so that the mapping will match structures with alternative names
    fieldNames.push(MAP_KEY_NAME);
  }

  @Override
  public void beforeValueField(Type key) {
    // normalize the name to "value" so that the mapping will match structures with alternative
    // names
    fieldNames.push(MAP_VALUE_NAME);
  }

  @Override
  public void beforeRepeatedElement(Type element) {
    // do not add the repeated element's name
  }

  @Override
  public void afterRepeatedElement(Type element) {
    // do not remove the repeated element's name
  }

  @Override
  public void beforeRepeatedKeyValue(Type keyValue) {
    // do not add the repeated element's name
  }

  @Override
  public void afterRepeatedKeyValue(Type keyValue) {
    // do not remove the repeated element's name
  }

  @Override
  protected String[] currentPath() {
    return Lists.newArrayList(fieldNames.descendingIterator()).toArray(new String[0]);
  }

  @Override
  protected String[] path(String name) {
    List<String> list = Lists.newArrayList(fieldNames.descendingIterator());
    list.add(name);
    return list.toArray(new String[0]);
  }
}
