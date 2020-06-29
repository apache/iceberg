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

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.required;

class PositionBasedDeleteRecord implements IndexedRecord, SpecificData.SchemaConstructable {

  private transient org.apache.avro.Schema avroSchema = null;
  private String filePath = null;
  private Long position = null;

  static org.apache.iceberg.Schema schema() {
    return new org.apache.iceberg.Schema(
        required(1, "file_path", Types.StringType.get()),
        required(2, "position", Types.LongType.get()));
  }

  /**
   * Used by Avro reflection to instantiate this class when reading manifest files.
   */
  PositionBasedDeleteRecord(org.apache.avro.Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  PositionBasedDeleteRecord(String path, Long pos) {
    this.filePath = path;
    this.position = pos;
  }

  @Override
  public void put(int pos, Object v) {
    switch (pos) {
      case 0:
        // always coerce to String for Serializable
        this.filePath = v.toString();
        return;
      case 1:
        this.position = (Long) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int pos) {
    switch (pos) {
      case 0:
        return filePath;
      case 1:
        return position;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("file_path", filePath)
        .add("position", position)
        .toString();
  }
}
