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

package com.netflix.iceberg.encryption;

import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.avro.AvroSchemaUtil;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.util.ByteBuffers;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

public class GenericFileEncryptionMetadata
  implements
    FileEncryptionMetadata,
    StructLike,
    IndexedRecord,
    SpecificData.SchemaConstructable,
    Serializable {

  private static final Schema AVRO_SCHEMA = AvroSchemaUtil.convert(
      FileEncryptionMetadata.schema(), "encryption_metadata");

  // Everything is not final for serialization.
  private transient Schema avroSchema;
  private int[] fromProjectionPos;

  // data fields
  private EncryptionKeyMetadata keyMetadata;
  private String cipherAlgorithm;

  /**
   * Used by Avro reflection to instantiate this class when reading manifest files.
   */
  public GenericFileEncryptionMetadata(org.apache.avro.Schema avroSchema) {
    this.avroSchema = avroSchema;

    List<Types.NestedField> fields = AvroSchemaUtil.convert(avroSchema)
        .asNestedType()
        .asStructType()
        .fields();
    List<Types.NestedField> allFields = FileEncryptionMetadata.schema().asStruct().fields();

    this.fromProjectionPos = new int[fields.size()];
    for (int i = 0; i < fromProjectionPos.length; i += 1) {
      boolean found = false;
      for (int j = 0; j < allFields.size(); j += 1) {
        if (fields.get(i).fieldId() == allFields.get(j).fieldId()) {
          found = true;
          fromProjectionPos[i] = j;
        }
      }

      if (!found) {
        throw new IllegalArgumentException("Cannot find projected field: " + fields.get(i));
      }
    }
  }

  GenericFileEncryptionMetadata(EncryptionKeyMetadata keyMetadata, String cipherAlgorithm) {
    this.keyMetadata = keyMetadata;
    this.cipherAlgorithm = cipherAlgorithm;
    this.avroSchema = AVRO_SCHEMA;
  }

  GenericFileEncryptionMetadata(FileEncryptionMetadata toCopy) {
    this.keyMetadata = toCopy.keyMetadata().copy();
    this.cipherAlgorithm = toCopy.cipherAlgorithm();
    this.avroSchema = AVRO_SCHEMA;
  }

  @Override
  public int size() {
    return FileEncryptionMetadata.schema().columns().size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public <T> void set(int pos, T value) {
    put(pos, value);
  }

  @Override
  public EncryptionKeyMetadata keyMetadata() {
    return keyMetadata;
  }

  @Override
  public String cipherAlgorithm() {
    return cipherAlgorithm;
  }

  @Override
  public FileEncryptionMetadata copy() {
    return new GenericFileEncryptionMetadata(this);
  }

  @Override
  public void put(int i, Object v) {
    int pos = i;
    // if the schema was projected, map the incoming ordinal to the expected one
    if (fromProjectionPos != null) {
      pos = fromProjectionPos[i];
    }
    switch (pos) {
      case 0:
        // always coerce to String for Serializable
        this.keyMetadata = GenericEncryptionKeyMetadata.of(
            ByteBuffers.toByteArray((ByteBuffer) v));
        return;
      case 1:
        this.cipherAlgorithm = v.toString();
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    int pos = i;
    // if the schema was projected, map the incoming ordinal to the expected one
    if (fromProjectionPos != null) {
      pos = fromProjectionPos[i];
    }
    switch (pos) {
      case 0:
        return keyMetadata == null ? null : keyMetadata().keyMetadata();
      case 1:
        return cipherAlgorithm;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }
}
