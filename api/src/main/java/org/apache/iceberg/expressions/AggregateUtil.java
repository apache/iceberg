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
package org.apache.iceberg.expressions;

import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Aggregate utility methods. */
public class AggregateUtil {
  private AggregateUtil() {}

  /**
   * Create a NestedField for this Aggregate Expression. This NestedField is used to build the
   * pushed down aggregate schema.
   *
   * <p>e.g. SELECT COUNT(*), MAX(col1), MIN(col1), MAX(col2), MIN(col3) FROM table;
   *
   * <p>Suppose the table schema is Schema(Types.NestedField.required(1, "col1",
   * Types.IntegerType.get()), Types.NestedField.required(2, "col2", Types.StringType.get()),
   * Types.NestedField.required(3, "col3", Types.StringType.get()));
   *
   * <p>The returned NestedField for the aggregates are Types.NestedField.required(1, COUNT(*),
   * Types.LongType.get()) Types.NestedField.required(2, MAX(col1), Types.IntegerType.get())
   * Types.NestedField.required(3, MIN(col1), Types.IntegerType.get()) Types.NestedField.required(4,
   * MAX(col2), Types.StringType.get()) Types.NestedField.required(5, MIN(col3),
   * Types.StringType.get())
   */
  public static Types.NestedField buildAggregateNestedField(BoundAggregate aggregate, int index) {
    return aggregate.nestedField(index);
  }

  /**
   * Returns the column name this aggregate function is on. e.g. SELECT Max(col3) FROM table; This
   * method returns col3
   */
  public static String getAggregateColumnName(BoundAggregate aggregate) {
    return aggregate.columnName();
  }

  /**
   * Returns the data type of this Aggregate Expression. The data type for COUNT is always Long. The
   * data type for MAX and MIX are the same as the data type of the column this aggregate is applied
   * on.
   */
  public static Type getAggregateType(BoundAggregate aggregate) {
    return aggregate.type();
  }

  /**
   * Returns the index of this Aggregate column in table schema. e.g. SELECT Max(col3) FROM table;
   * Suppose the table has columns (col1, col2, col3), this method returns 2.
   */
  public static int columnIndexInTableSchema(
      BoundAggregate aggregate, Table table, boolean caseSensitive) {
    List<Types.NestedField> columns = table.schema().columns();
    for (int i = 0; i < columns.size(); i++) {
      if (aggregate.columnName().equals("*")) {
        return -1;
      }
      if (caseSensitive) {
        if (aggregate.columnName().equals(columns.get(i).name())) {
          return i;
        }
      } else {
        if (aggregate.columnName().equalsIgnoreCase(columns.get(i).name())) {
          return i;
        }
      }
    }
    throw new ValidationException(
        "Aggregate is on an invalid table column %s", aggregate.columnName());
  }
}
