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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.orc.TypeDescription;

/**
 * Generic visitor of an ORC Schema.
 */
public abstract class OrcSchemaVisitor<T> {

  public static <T> List<T> visitSchema(TypeDescription schema, OrcSchemaVisitor<T> visitor) {
    Preconditions.checkArgument(schema.getId() == 0, "TypeDescription must be root schema.");

    List<TypeDescription> fields = schema.getChildren();
    List<String> names = schema.getFieldNames();

    return visitFields(fields, names, visitor);
  }

  public static <T> T visit(TypeDescription schema, OrcSchemaVisitor<T> visitor) {
    switch (schema.getCategory()) {
      case STRUCT:
        return visitRecord(schema, visitor);

      case UNION:
        throw new UnsupportedOperationException("Cannot handle " + schema);

      case LIST:
        return visitor.list(schema, visit(schema.getChildren().get(0), visitor));

      case MAP:
        return visitor.map(schema, visit(schema.getChildren().get(0), visitor),
            visit(schema.getChildren().get(1), visitor));

      default:
        return visitor.primitive(schema);
    }
  }

  private static <T> List<T> visitFields(List<TypeDescription> fields, List<String> names,
                                         OrcSchemaVisitor<T> visitor) {
    Preconditions.checkArgument(fields.size() == names.size(), "Not all fields have names in ORC struct");

    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      TypeDescription field = fields.get(i);
      String name = names.get(i);
      visitor.beforeField(name, field);
      try {
        results.add(visit(field, visitor));
      } finally {
        visitor.afterField(name, field);
      }
    }
    return results;
  }

  private static <T> T visitRecord(TypeDescription record, OrcSchemaVisitor<T> visitor) {
    List<TypeDescription> fields = record.getChildren();
    List<String> names = record.getFieldNames();

    return visitor.record(record, names, visitFields(fields, names, visitor));
  }

  public void beforeField(String name, TypeDescription type) {}

  public void afterField(String name, TypeDescription type) {}

  public T record(TypeDescription record, List<String> names, List<T> fields) {
    return null;
  }

  public T list(TypeDescription array, T element) {
    return null;
  }

  public T map(TypeDescription map, T key, T value) {
    return null;
  }

  public T primitive(TypeDescription primitive) {
    return null;
  }
}
