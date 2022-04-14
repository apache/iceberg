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
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.AvroSchemaWithTypeVisitor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.BaseDataReader;
import org.apache.iceberg.types.Types;

public abstract class SparkDefaultValueAwareAvroSchemaWithTypeVisitor<T> extends AvroSchemaWithTypeVisitor<T> {

  private final Map<Integer, Object> idToConstant;

  protected Map<Integer, Object> getIdToConstant() {
    return idToConstant;
  }

  protected SparkDefaultValueAwareAvroSchemaWithTypeVisitor(Map<Integer, ?> idToConstant) {
    this.idToConstant = Maps.newHashMap();
    this.idToConstant.putAll(idToConstant);
  }

  @Override
  public T visitRecord(Types.StructType struct, Schema record) {
    // check to make sure this hasn't been visited before
    String name = record.getFullName();
    Preconditions.checkState(!recordLevels.contains(name),
        "Cannot process recursive Avro record %s", name);

    recordLevels.push(name);

    List<Schema.Field> fields = record.getFields();
    List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
    for (Schema.Field field : fields) {
      int fieldId = AvroSchemaUtil.getFieldId(field);
      Types.NestedField iField = struct != null ? struct.field(fieldId) : null;
      Object shouldUseDefaultFlag = field.getObjectProp(AvroSchemaUtil.SHOULD_USE_INIT_DEFAULT);
      if (iField != null && shouldUseDefaultFlag != null && (Boolean) shouldUseDefaultFlag) {
        idToConstant.put(
            fieldId,
            BaseDataReader.convertConstant(iField.type(), iField.initialDefaultValue()));
      }
      names.add(field.name());
      results.add(visit(iField != null ? iField.type() : null, field.schema(), this));
    }

    recordLevels.pop();

    return record(struct, record, names, results);
  }
}
