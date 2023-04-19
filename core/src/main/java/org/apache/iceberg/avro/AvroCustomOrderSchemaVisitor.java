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
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

abstract class AvroCustomOrderSchemaVisitor<T, F> {
  public static <T, F> T visit(Schema schema, AvroCustomOrderSchemaVisitor<T, F> visitor) {
    switch (schema.getType()) {
      case RECORD:
        // check to make sure this hasn't been visited before
        String name = schema.getFullName();
        Preconditions.checkState(
            !visitor.recordLevels.contains(name), "Cannot process recursive Avro record %s", name);

        visitor.recordLevels.push(name);

        List<Schema.Field> fields = schema.getFields();
        List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
        List<Supplier<F>> results = Lists.newArrayListWithExpectedSize(fields.size());
        for (Schema.Field field : schema.getFields()) {
          names.add(field.name());
          results.add(new VisitFieldFuture<>(field, visitor));
        }

        visitor.recordLevels.pop();

        return visitor.record(schema, names, Iterables.transform(results, Supplier::get));

      case UNION:
        List<Schema> types = schema.getTypes();
        List<Supplier<T>> options = Lists.newArrayListWithExpectedSize(types.size());
        for (Schema type : types) {
          options.add(new VisitFuture<>(type, visitor));
        }
        return visitor.union(schema, Iterables.transform(options, Supplier::get));

      case ARRAY:
        return visitor.array(schema, new VisitFuture<>(schema.getElementType(), visitor));

      case MAP:
        return visitor.map(schema, new VisitFuture<>(schema.getValueType(), visitor));

      default:
        return visitor.primitive(schema);
    }
  }

  private Deque<String> recordLevels = Lists.newLinkedList();

  public T record(Schema record, List<String> names, Iterable<F> fields) {
    return null;
  }

  public F field(Schema.Field field, Supplier<T> fieldResult) {
    return null;
  }

  public T union(Schema union, Iterable<T> options) {
    return null;
  }

  public T array(Schema array, Supplier<T> element) {
    return null;
  }

  public T map(Schema map, Supplier<T> value) {
    return null;
  }

  public T primitive(Schema primitive) {
    return null;
  }

  private static class VisitFuture<T, F> implements Supplier<T> {
    private final Schema schema;
    private final AvroCustomOrderSchemaVisitor<T, F> visitor;

    private VisitFuture(Schema schema, AvroCustomOrderSchemaVisitor<T, F> visitor) {
      this.schema = schema;
      this.visitor = visitor;
    }

    @Override
    public T get() {
      return visit(schema, visitor);
    }
  }

  private static class VisitFieldFuture<T, F> implements Supplier<F> {
    private final Schema.Field field;
    private final AvroCustomOrderSchemaVisitor<T, F> visitor;

    private VisitFieldFuture(Schema.Field field, AvroCustomOrderSchemaVisitor<T, F> visitor) {
      this.field = field;
      this.visitor = visitor;
    }

    @Override
    public F get() {
      return visitor.field(field, new VisitFuture<>(field.schema(), visitor));
    }
  }
}
