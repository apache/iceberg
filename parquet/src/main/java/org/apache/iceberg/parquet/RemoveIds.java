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

import java.util.List;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class RemoveIds extends ParquetTypeVisitor<Type> {

  @Override
  public Type message(MessageType message, List<Type> fields) {
    Types.MessageTypeBuilder builder = Types.buildMessage();
    for (Type field : struct(message.asGroupType(), fields).asGroupType().getFields()) {
      builder.addField(field);
    }
    return builder.named(message.getName());
  }

  @Override
  public Type struct(GroupType struct, List<Type> fields) {
    Types.GroupBuilder<GroupType> builder = Types.buildGroup(struct.getRepetition());
    for (Type field : fields) {
      builder.addField(field);
    }
    return builder.named(struct.getName());
  }

  @Override
  public Type list(GroupType array, Type item) {
    Types.GroupBuilder<GroupType> listBuilder =
        Types.buildGroup(array.getRepetition()).as(LogicalTypeAnnotation.listType());
    final Type listElement = ParquetSchemaUtil.determineListElementType(array);
    if (listElement.isRepetition(Type.Repetition.REPEATED)) {
      listBuilder.addFields(item);
    } else {
      listBuilder.repeatedGroup().addFields(item).named(array.getFieldName(0));
    }
    return listBuilder.named(array.getName());
  }

  @Override
  public Type map(GroupType map, Type key, Type value) {
    return Types.buildGroup(map.getRepetition())
        .as(LogicalTypeAnnotation.mapType())
        .repeatedGroup()
        .addFields(key, value)
        .named(map.getFieldName(0))
        .named(map.getName());
  }

  @Override
  public Type primitive(PrimitiveType primitive) {
    return Types.primitive(primitive.getPrimitiveTypeName(), primitive.getRepetition())
        .length(primitive.getTypeLength())
        .as(primitive.getLogicalTypeAnnotation())
        .named(primitive.getName());
  }

  public static MessageType removeIds(MessageType type) {
    return (MessageType) ParquetTypeVisitor.visit(type, new RemoveIds());
  }
}
