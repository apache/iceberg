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

package org.apache.iceberg.flink.connector.sink;

import java.util.List;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;

public class AvroUtils {

  private AvroUtils() {}

  /**
   * we only support union type for optional field.
   * we don't support union for arbitrary types, e.g. union of int and string.
   */
  public static Schema getActualSchema(Schema fieldSchema) {
    Schema actualSchema = null;
    if (fieldSchema.getType().equals(Schema.Type.UNION)) {
      List<Schema> schemaList = fieldSchema.getTypes();
      // this should only contain two entries, the actual type and NULL
      for (Schema schema : schemaList) {
        if (!schema.getType().equals(Schema.Type.NULL)) {
          if (null == actualSchema) {
            actualSchema = schema;
          } else {
            throw new AvroTypeException("Only support union for optional/nullable field: " + fieldSchema.getName());
          }
        }
      }
    } else {
      actualSchema = fieldSchema;
    }
    if (actualSchema == null) {
      throw new AvroTypeException("No actual schema for field: " + fieldSchema.getName());
    }
    return actualSchema;
  }

  public static boolean isOptional(Schema fieldSchema) {
    boolean isOptional = false;
    if (fieldSchema.getType().equals(Schema.Type.UNION)) {
      for (Schema schema : fieldSchema.getTypes()) {
        if (schema.getType().equals(Schema.Type.NULL)) {
          isOptional = true;
        }
      }
    }
    return isOptional;
  }
}
