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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Deque;
import java.util.List;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class ParquetTypeVisitor<T> {
  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected Deque<String> fieldNames = Lists.newLinkedList();

  public static <T> T visit(Type type, ParquetTypeVisitor<T> visitor) {
    if (type instanceof MessageType) {
      return visitor.message((MessageType) type,
          visitFields(type.asGroupType(), visitor));

    } else if (type.isPrimitive()) {
      return visitor.primitive(type.asPrimitiveType());

    } else {
      // if not a primitive, the typeId must be a group
      GroupType group = type.asGroupType();
      OriginalType annotation = group.getOriginalType();
      if (annotation != null) {
        switch (annotation) {
          case LIST:
            Preconditions.checkArgument(!group.isRepetition(Type.Repetition.REPEATED),
                "Invalid list: top-level group is repeated: %s", group);
            Preconditions.checkArgument(group.getFieldCount() == 1,
                "Invalid list: does not contain single repeated field: %s", group);

            GroupType repeatedElement = group.getFields().get(0).asGroupType();
            Preconditions.checkArgument(repeatedElement.isRepetition(Type.Repetition.REPEATED),
                "Invalid list: inner group is not repeated");
            Preconditions.checkArgument(repeatedElement.getFieldCount() <= 1,
                "Invalid list: repeated group is not a single field: %s", group);

            visitor.fieldNames.push(repeatedElement.getName());
            try {
              T elementResult = null;
              if (repeatedElement.getFieldCount() > 0) {
                elementResult = visitField(repeatedElement.getType(0), visitor);
              }

              return visitor.list(group, elementResult);

            } finally {
              visitor.fieldNames.pop();
            }

          case MAP:
            Preconditions.checkArgument(!group.isRepetition(Type.Repetition.REPEATED),
                "Invalid map: top-level group is repeated: %s", group);
            Preconditions.checkArgument(group.getFieldCount() == 1,
                "Invalid map: does not contain single repeated field: %s", group);

            GroupType repeatedKeyValue = group.getType(0).asGroupType();
            Preconditions.checkArgument(repeatedKeyValue.isRepetition(Type.Repetition.REPEATED),
                "Invalid map: inner group is not repeated");
            Preconditions.checkArgument(repeatedKeyValue.getFieldCount() <= 2,
                "Invalid map: repeated group does not have 2 fields");

            visitor.fieldNames.push(repeatedKeyValue.getName());
            try {
              T keyResult = null;
              T valueResult = null;
              switch (repeatedKeyValue.getFieldCount()) {
                case 2:
                  // if there are 2 fields, both key and value are projected
                  keyResult = visitField(repeatedKeyValue.getType(0), visitor);
                  valueResult = visitField(repeatedKeyValue.getType(1), visitor);
                  break;
                case 1:
                  // if there is just one, use the name to determine what it is
                  Type keyOrValue = repeatedKeyValue.getType(0);
                  if (keyOrValue.getName().equalsIgnoreCase("key")) {
                    keyResult = visitField(keyOrValue, visitor);
                    // value result remains null
                  } else {
                    valueResult = visitField(keyOrValue, visitor);
                    // key result remains null
                  }
                  break;
                default:
                  // both results will remain null
              }

              return visitor.map(group, keyResult, valueResult);

            } finally {
              visitor.fieldNames.pop();
            }

          default:
        }
      }

      return visitor.struct(group, visitFields(group, visitor));
    }
  }

  private static <T> T visitField(Type field, ParquetTypeVisitor<T> visitor) {
    visitor.fieldNames.push(field.getName());
    try {
      return visit(field, visitor);
    } finally {
      visitor.fieldNames.pop();
    }
  }

  private static <T> List<T> visitFields(GroupType group, ParquetTypeVisitor<T> visitor) {
    List<T> results = Lists.newArrayListWithExpectedSize(group.getFieldCount());
    for (Type field : group.getFields()) {
      results.add(visitField(field, visitor));
    }

    return results;
  }

  public T message(MessageType message, List<T> fields) {
    return null;
  }

  public T struct(GroupType struct, List<T> fields) {
    return null;
  }

  public T list(GroupType array, T element) {
    return null;
  }

  public T map(GroupType map, T key, T value) {
    return null;
  }

  public T primitive(PrimitiveType primitive) {
    return null;
  }
}
