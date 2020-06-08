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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;

public class FlinkSchemaUtil {

  private FlinkSchemaUtil() {
  }

  /**
   * Convert the flink table schema to apache iceberg schema.
   */
  public static Schema convert(TableSchema schema) {
    Preconditions.checkArgument(schema.toRowDataType() instanceof FieldsDataType, "Should be FieldsDataType");

    FieldsDataType root = (FieldsDataType) schema.toRowDataType();
    Type converted = FlinkTypeVisitor.visit(root, new FlinkTypeToType(root));

    return new Schema(converted.asNestedType().asStructType().fields());
  }
}
