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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

/**
 * Serializes the equality values of generic {@link Record}s into Flink keyed-state keys ({@link
 * SerializedEqualityValues}). The key holds the equality values alone, so all rows with the same
 * key co-locate on one shard regardless of partition spec. Spec scoping of a delete is applied at
 * resolve time, not in the key.
 *
 * <p>Generic records hold {@code java.time} values, so they are converted to Iceberg's internal
 * representation with an {@link InternalRecordWrapper} created once per serializer.
 */
class RecordSerializer {

  private final List<Types.NestedField> keyFields;
  private final InternalRecordWrapper keyWrapper;
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final DataOutputStream dos = new DataOutputStream(baos);

  RecordSerializer(Types.StructType keyType) {
    this.keyFields = keyType.fields();
    this.keyWrapper = new InternalRecordWrapper(keyType);
  }

  /**
   * Serializes the equality values of {@code key}. The key fields must occupy the leading positions
   * of the record, in key type order; trailing fields are ignored.
   */
  public SerializedEqualityValues serializeKey(Record key) {
    InternalRecordWrapper wrapped = keyWrapper.wrap(key);
    baos.reset();
    try {
      dos.writeInt(keyFields.size());
      for (Types.NestedField field : keyFields) {
        dos.writeInt(field.fieldId());
      }

      for (int i = 0; i < keyFields.size(); i++) {
        StructLikeSerializer.writeField(dos, wrapped, i, keyFields.get(i).type());
      }

      dos.flush();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize PK index key", e);
    }

    return new SerializedEqualityValues(baos.toByteArray());
  }
}
