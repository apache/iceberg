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


package org.apache.iceberg.spark.data;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.BaseDataReader;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;

public class SparkDefaultValueAwareOrcSchemaWithTypeVisitor<T> extends OrcSchemaWithTypeVisitor<T> {

  private final Map<Integer, Object> idToConstant;

  protected Map<Integer, Object> getIdToConstant() {
    return idToConstant;
  }

  protected SparkDefaultValueAwareOrcSchemaWithTypeVisitor(Map<Integer, ?> idToConstant) {
    this.idToConstant = Maps.newHashMap();
    this.idToConstant.putAll(idToConstant);
  }

  /**
   * This visitor differs from the parent visitor on how it handles Struct type visiting,
   * particularly, it assumes the iceberg schema represented by {@code struct} might have
   * additional fields than the ORCSchemaUtil.buildOrcProjection projected orc file reader
   * schema {@code record}, at this point, we know for sure those additional fields have
   * initial default values, and we build an idToConstant map here to supply to ORC value
   * readers.
   */
  @Override
  public T visitRecord(
      Types.StructType struct, TypeDescription record, OrcSchemaWithTypeVisitor<T> visitor) {

    List<Types.NestedField> iFields = struct.fields();
    List<TypeDescription> fields = record.getChildren();
    List<String> names = record.getFieldNames();
    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());

    for (int i = 0, j = 0; i < iFields.size(); i++) {
      Types.NestedField iField = iFields.get(i);
      TypeDescription field = j < fields.size() ? fields.get(j) : null;
      if (field == null || (iField.fieldId() != ORCSchemaUtil.fieldId(field))) {
        if (!MetadataColumns.isMetadataColumn(iField.name()) && !idToConstant.containsKey(iField.fieldId())) {
          idToConstant.put(
              iField.fieldId(),
              BaseDataReader.convertConstant(iField.type(), iField.initialDefaultValue()));
        }
      } else {
        results.add(visit(iField.type(), field, visitor));
        j++;
      }
    }
    return visitor.record(struct, record, names, results);
  }
}
