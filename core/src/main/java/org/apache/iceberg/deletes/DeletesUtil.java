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

package org.apache.iceberg.deletes;

import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

public class DeletesUtil {

  private DeletesUtil() {
  }

  public static Schema pathPosSchema() {
    return new Schema(
        MetadataColumns.DELETE_FILE_PATH,
        MetadataColumns.DELETE_FILE_POS);
  }

  public static Schema pathPosSchema(Schema rowSchema) {
    Preconditions.checkNotNull(rowSchema, "Row schema should not be null when constructing the pos-delete schema.");

    // the appender uses the row schema wrapped with position fields
    return new Schema(
        MetadataColumns.DELETE_FILE_PATH,
        MetadataColumns.DELETE_FILE_POS,
        Types.NestedField.required(
            MetadataColumns.DELETE_FILE_ROW_FIELD_ID, "row", rowSchema.asStruct(),
            MetadataColumns.DELETE_FILE_ROW_DOC));
  }

  public static Schema posDeleteSchema(Schema rowSchema) {
    return rowSchema == null ? pathPosSchema() : pathPosSchema(rowSchema);
  }
}
