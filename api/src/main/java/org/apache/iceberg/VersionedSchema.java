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

package org.apache.iceberg;

import java.io.Serializable;
import java.util.Objects;

/**
 * A wrapper around Schema to include its schema ID
 */
public class VersionedSchema implements Serializable {
  private final int schemaId;
  private final Schema schema;

  public VersionedSchema(int schemaId, Schema schema) {
    this.schemaId = schemaId;
    this.schema = schema;
  }

  public int schemaId() {
    return schemaId;
  }

  public Schema schema() {
    return schema;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof VersionedSchema)) {
      return false;
    }

    VersionedSchema that = (VersionedSchema) other;
    return schemaId == that.schemaId &&
        schema != null && that.schema != null &&
        Objects.equals(schema.asStruct(), that.schema.asStruct());
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaId, schema);
  }

  @Override
  public String toString() {
    return "VersionedSchema(schemaId=" + schemaId + ", schema={" + schema + "})";
  }

}
