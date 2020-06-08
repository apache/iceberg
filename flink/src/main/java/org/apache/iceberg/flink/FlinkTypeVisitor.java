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

package org.apache.iceberg.flink;

import java.util.List;
import java.util.Map;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;

public class FlinkTypeVisitor<T> {

  static <T> T visit(DataType dataType, FlinkTypeVisitor<T> visitor) {
    if (dataType instanceof FieldsDataType) {
      FieldsDataType fieldsType = (FieldsDataType) dataType;
      Map<String, DataType> fields = fieldsType.getFieldDataTypes();
      Map<String, Pair<String, T>> fieldResults = Maps.newLinkedHashMap();

      Preconditions.checkArgument(dataType.getLogicalType() instanceof RowType, "The logical type must be RowType");

      // Make sure that we're traversing the fields in the same order as constructing the schema's fields, so that we
      // could get the field's comment correctly. NOTICE: the flink type don't attach the comment inside it so we
      // iceberg need to read the comment firstly and then maintain the pair (comment, type) for each field.
      List<RowType.RowField> rowFields = ((RowType) dataType.getLogicalType()).getFields();
      for (RowType.RowField rowField : rowFields) {
        String name = rowField.getName();
        String comment = rowField.getDescription().orElse(null);
        fieldResults.put(name, Pair.of(comment, visit(fields.get(name), visitor)));
      }

      return visitor.fields(fieldsType, fieldResults);
    } else if (dataType instanceof CollectionDataType) {
      CollectionDataType collectionType = (CollectionDataType) dataType;
      return visitor.collection(collectionType,
          visit(collectionType.getElementDataType(), visitor));
    } else if (dataType instanceof KeyValueDataType) {
      KeyValueDataType mapType = (KeyValueDataType) dataType;
      return visitor.map(mapType,
          visit(mapType.getKeyDataType(), visitor),
          visit(mapType.getValueDataType(), visitor));
    } else if (dataType instanceof AtomicDataType) {
      AtomicDataType atomic = (AtomicDataType) dataType;
      return visitor.atomic(atomic);
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  /**
   * The Flink Type did not include the 'comment' inside it, so here we need to maintain a map to mapping the field name
   * to the (comment, type) pair.
   */
  public T fields(FieldsDataType dataType, Map<String, Pair<String, T>> fieldResults) {
    return null;
  }

  public T collection(CollectionDataType type, T elementResult) {
    return null;
  }

  public T map(KeyValueDataType type, T keyResult, T valueResult) {
    return null;
  }

  public T atomic(AtomicDataType type) {
    return null;
  }
}
