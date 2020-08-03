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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * The {@link DataFile} have few unmodified fields which could not be serialized by the default kryo serializer, so we
 * customize its own flink serializer for {@link DataFile}.
 */
class DataFileTypeInfo extends GenericTypeInfo<DataFile> {
  static final DataFileTypeInfo TYPE_INFO = new DataFileTypeInfo();

  private DataFileTypeInfo() {
    super(DataFile.class);
  }

  @Override
  public TypeSerializer<DataFile> createSerializer(ExecutionConfig config) {
    return DataFileSerializer.INSTANCE;
  }

  private static class DataFileSerializer extends TypeSerializer<DataFile> {
    private static final DataFileSerializer INSTANCE = new DataFileSerializer();

    @Override
    public boolean isImmutableType() {
      return false;
    }

    @Override
    public TypeSerializer<DataFile> duplicate() {
      // This serializer is not stateful so we could just return itself.
      return this;
    }

    @Override
    public DataFile createInstance() {
      return InstantiationUtil.instantiate(DataFile.class);
    }

    @Override
    public DataFile copy(DataFile from) {
      return from.copy();
    }

    @Override
    public DataFile copy(DataFile from, DataFile reuse) {
      return copy(from);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
      int length = source.readInt();
      target.writeInt(length);
      target.write(source, length);
    }

    @Override
    public int getLength() {
      return -1;
    }

    @Override
    public void serialize(DataFile record, DataOutputView target) throws IOException {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
           ObjectOutputStream out = new ObjectOutputStream(bos)) {
        out.writeObject(record);
        out.flush();
        byte[] bytes = bos.toByteArray();
        target.writeInt(bytes.length);
        target.write(bytes);
      }
    }

    @Override
    public DataFile deserialize(DataInputView source) throws IOException {
      int length = source.readInt();
      byte[] data = new byte[length];
      int readLength = source.read(data);
      Preconditions.checkArgument(readLength >= 0, "Invalid read bytes length: " + readLength);

      try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
           ObjectInputStream in = new ObjectInputStream(bais)) {
        try {
          return (DataFile) in.readObject();
        } catch (ClassNotFoundException e) {
          throw new IOException(e);
        }
      }
    }

    @Override
    public DataFile deserialize(DataFile reuse, DataInputView source) throws IOException {
      return deserialize(source);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      return obj != null && obj.getClass() == DataFileSerializer.class;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(DataFile.class);
    }

    @Override
    public TypeSerializerSnapshot<DataFile> snapshotConfiguration() {
      return DataFileSerializerSnapshot.INSTANCE;
    }
  }

  private static class DataFileSerializerSnapshot implements TypeSerializerSnapshot<DataFile> {
    private static final DataFileSerializerSnapshot INSTANCE = new DataFileSerializerSnapshot();

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getCurrentVersion() {
      return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) {
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {
    }

    @Override
    public TypeSerializer<DataFile> restoreSerializer() {
      return new DataFileSerializer();
    }

    @Override
    public TypeSerializerSchemaCompatibility<DataFile> resolveSchemaCompatibility(
        TypeSerializer<DataFile> newSerializer) {
      if (!(newSerializer instanceof DataFileSerializer)) {
        return TypeSerializerSchemaCompatibility.incompatible();
      }

      return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }
  }
}
