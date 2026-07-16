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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Serializer for {@link StructLike} tuples. Two entry points:
 *
 * <ul>
 *   <li>{@link #serializeKey} builds a Flink keyed-state key ({@link SerializedEqualityValues})
 *       from the equality values alone, so all rows with the same key co-locate on one shard
 *       regardless of partition spec. Spec scoping of a delete is applied at resolve time, not in
 *       the key.
 *   <li>{@link #encodePartition} / {@link #decodePartition} serialize partition tuples into bytes.
 * </ul>
 */
class StructLikeSerializer {

  static final byte[] EMPTY_PARTITION = new byte[0];

  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final DataOutputStream dos = new DataOutputStream(baos);

  public SerializedEqualityValues serializeKey(StructLike key, Types.StructType keyType) {
    baos.reset();
    try {
      List<Types.NestedField> fields = keyType.fields();
      dos.writeInt(fields.size());
      for (Types.NestedField field : fields) {
        dos.writeInt(field.fieldId());
      }

      for (int i = 0; i < fields.size(); i++) {
        writeField(key, i, fields.get(i).type());
      }

      dos.flush();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize PK index key", e);
    }

    return new SerializedEqualityValues(baos.toByteArray());
  }

  public byte[] encodePartition(StructLike partition, Types.StructType partitionType) {
    List<Types.NestedField> fields = partitionType.fields();
    if (fields.isEmpty()) {
      return EMPTY_PARTITION;
    }

    baos.reset();
    try {
      for (int i = 0; i < fields.size(); i++) {
        writeField(partition, i, fields.get(i).type());
      }

      dos.flush();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode partition", e);
    }

    return baos.toByteArray();
  }

  public static StructLike decodePartition(byte[] encoded, Types.StructType partitionType) {
    PartitionData partition = new PartitionData(partitionType);
    List<Types.NestedField> fields = partitionType.fields();
    if (fields.isEmpty()) {
      return partition;
    }

    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(encoded))) {
      for (int i = 0; i < fields.size(); i++) {
        boolean isNull = dis.readBoolean();
        if (isNull) {
          partition.set(i, null);
        } else {
          int length = dis.readInt();
          byte[] bytes = new byte[length];
          dis.readFully(bytes);
          Object value = Conversions.fromByteBuffer(fields.get(i).type(), ByteBuffer.wrap(bytes));
          // Conversions returns CharBuffer for STRING; PartitionData (and the manifest writer)
          // expects String.
          if (value instanceof CharSequence cs && !(value instanceof String)) {
            value = cs.toString();
          }

          partition.set(i, value);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode partition", e);
    }

    return partition;
  }

  private void writeField(StructLike struct, int pos, Type fieldType) throws IOException {
    Object value = struct.get(pos, Object.class);
    if (value == null) {
      dos.writeBoolean(true);
      return;
    }

    dos.writeBoolean(false);
    ByteBuffer buf = Conversions.toByteBuffer(fieldType, value);
    dos.writeInt(buf.remaining());
    dos.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
  }
}
