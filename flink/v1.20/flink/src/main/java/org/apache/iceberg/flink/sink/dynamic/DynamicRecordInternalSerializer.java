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
package org.apache.iceberg.flink.sink.dynamic;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

@Internal
class DynamicRecordInternalSerializer extends TypeSerializer<DynamicRecordInternal> {
  private static final long serialVersionUID = 1L;

  private final RowDataSerializerCache serializerCache;
  private final boolean writeSchemaAndSpec;

  DynamicRecordInternalSerializer(
      RowDataSerializerCache serializerCache, boolean writeSchemaAndSpec) {
    this.serializerCache = serializerCache;
    this.writeSchemaAndSpec = writeSchemaAndSpec;
  }

  @Override
  public TypeSerializer<DynamicRecordInternal> duplicate() {
    return new DynamicRecordInternalSerializer(
        new RowDataSerializerCache(serializerCache.catalogLoader(), serializerCache.maximumSize()),
        writeSchemaAndSpec);
  }

  @Override
  public DynamicRecordInternal createInstance() {
    return new DynamicRecordInternal();
  }

  @Override
  public void serialize(DynamicRecordInternal toSerialize, DataOutputView dataOutputView)
      throws IOException {
    dataOutputView.writeUTF(toSerialize.tableName());
    dataOutputView.writeUTF(toSerialize.branch());
    if (writeSchemaAndSpec) {
      dataOutputView.writeUTF(SchemaParser.toJson(toSerialize.schema()));
      dataOutputView.writeUTF(PartitionSpecParser.toJson(toSerialize.spec()));
    } else {
      dataOutputView.writeInt(toSerialize.schema().schemaId());
      dataOutputView.writeInt(toSerialize.spec().specId());
    }
    dataOutputView.writeInt(toSerialize.writerKey());
    final Tuple3<RowDataSerializer, Schema, PartitionSpec> rowDataSerializer;
    if (writeSchemaAndSpec) {
      rowDataSerializer =
          serializerCache.serializer(
              toSerialize.tableName(), toSerialize.schema(), toSerialize.spec(), null, null);
    } else {
      // Check that the schema id can be resolved. Not strictly necessary for serialization.
      rowDataSerializer =
          serializerCache.serializer(
              toSerialize.tableName(),
              null,
              null,
              toSerialize.schema().schemaId(),
              toSerialize.spec().specId());
    }
    rowDataSerializer.f0.serialize(toSerialize.rowData(), dataOutputView);
    dataOutputView.writeBoolean(toSerialize.upsertMode());
    dataOutputView.writeInt(toSerialize.equalityFields().size());
    for (Integer equalityField : toSerialize.equalityFields()) {
      dataOutputView.writeInt(equalityField);
    }
  }

  @Override
  public DynamicRecordInternal deserialize(DataInputView dataInputView) throws IOException {
    String tableName = dataInputView.readUTF();
    String branch = dataInputView.readUTF();
    Schema schema = null;
    PartitionSpec spec = null;
    Integer schemaId = null;
    Integer specId = null;
    if (writeSchemaAndSpec) {
      schema = SchemaParser.fromJson(dataInputView.readUTF());
      spec = PartitionSpecParser.fromJson(schema, dataInputView.readUTF());
    } else {
      schemaId = dataInputView.readInt();
      specId = dataInputView.readInt();
    }

    int writerKey = dataInputView.readInt();
    Tuple3<RowDataSerializer, Schema, PartitionSpec> rowDataSerializer =
        serializerCache.serializer(tableName, schema, spec, schemaId, specId);
    RowData rowData = rowDataSerializer.f0.deserialize(dataInputView);
    boolean upsertMode = dataInputView.readBoolean();
    int numEqualityFields = dataInputView.readInt();
    final List<Integer> equalityFieldIds;
    if (numEqualityFields > 0) {
      equalityFieldIds = Lists.newArrayList();
    } else {
      equalityFieldIds = Collections.emptyList();
    }
    for (int i = 0; i < numEqualityFields; i++) {
      equalityFieldIds.add(dataInputView.readInt());
    }
    return new DynamicRecordInternal(
        tableName,
        branch,
        rowDataSerializer.f1,
        rowDataSerializer.f2,
        writerKey,
        rowData,
        upsertMode,
        equalityFieldIds);
  }

  @Override
  public DynamicRecordInternal deserialize(DynamicRecordInternal reuse, DataInputView dataInputView)
      throws IOException {
    String tableName = dataInputView.readUTF();
    reuse.setTableName(tableName);
    String branch = dataInputView.readUTF();
    reuse.setBranch(branch);

    Schema schema = null;
    PartitionSpec spec = null;
    Integer schemaId = null;
    Integer specId = null;
    if (writeSchemaAndSpec) {
      schema = SchemaParser.fromJson(dataInputView.readUTF());
      spec = PartitionSpecParser.fromJson(schema, dataInputView.readUTF());
      reuse.setSchema(schema);
      reuse.setSpec(spec);
    } else {
      schemaId = dataInputView.readInt();
      specId = dataInputView.readInt();
    }

    int writerKey = dataInputView.readInt();
    reuse.setWriterKey(writerKey);
    Tuple3<RowDataSerializer, Schema, PartitionSpec> rowDataSerializer =
        serializerCache.serializer(tableName, schema, spec, schemaId, specId);
    RowData rowData = rowDataSerializer.f0.deserialize(dataInputView);
    boolean upsertMode = dataInputView.readBoolean();
    int numEqualityFields = dataInputView.readInt();
    final List<Integer> equalityFieldIds;
    if (numEqualityFields > 0) {
      equalityFieldIds = Lists.newArrayList();
    } else {
      equalityFieldIds = Collections.emptyList();
    }
    for (int i = 0; i < numEqualityFields; i++) {
      equalityFieldIds.add(dataInputView.readInt());
    }
    return new DynamicRecordInternal(
        tableName,
        branch,
        rowDataSerializer.f1,
        rowDataSerializer.f2,
        writerKey,
        rowData,
        upsertMode,
        equalityFieldIds);
  }

  @Override
  public DynamicRecordInternal copy(DynamicRecordInternal from) {
    return new DynamicRecordInternal(
        from.tableName(),
        from.branch(),
        from.schema(),
        from.spec(),
        from.writerKey(),
        from.rowData(),
        from.upsertMode(),
        from.equalityFields());
  }

  @Override
  public DynamicRecordInternal copy(DynamicRecordInternal from, DynamicRecordInternal reuse) {
    reuse.setTableName(from.tableName());
    reuse.setBranch(from.branch());
    reuse.setSchema(from.schema());
    reuse.setSpec(from.spec());
    reuse.setWriterKey(from.writerKey());
    reuse.setRowData(from.rowData());
    reuse.setUpsertMode(from.upsertMode());
    reuse.setEqualityFieldIds(from.equalityFields());
    return reuse;
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    serialize(deserialize(source), target);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof DynamicRecordInternalSerializer) {
      DynamicRecordInternalSerializer other = (DynamicRecordInternalSerializer) obj;
      return writeSchemaAndSpec == other.writeSchemaAndSpec;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(writeSchemaAndSpec);
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public TypeSerializerSnapshot<DynamicRecordInternal> snapshotConfiguration() {
    return new DynamicRecordInternalTypeSerializerSnapshot(writeSchemaAndSpec);
  }

  public static class DynamicRecordInternalTypeSerializerSnapshot
      implements TypeSerializerSnapshot<DynamicRecordInternal> {

    private boolean writeSchemaAndSpec;

    // Zero args constructor is required to instantiate this class on restore
    @SuppressWarnings({"unused", "checkstyle:RedundantModifier"})
    public DynamicRecordInternalTypeSerializerSnapshot() {}

    DynamicRecordInternalTypeSerializerSnapshot(boolean writeSchemaAndSpec) {
      this.writeSchemaAndSpec = writeSchemaAndSpec;
    }

    @Override
    public int getCurrentVersion() {
      return 0;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
      out.writeBoolean(writeSchemaAndSpec);
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
        throws IOException {
      this.writeSchemaAndSpec = in.readBoolean();
    }

    @Override
    public TypeSerializerSchemaCompatibility<DynamicRecordInternal> resolveSchemaCompatibility(
        TypeSerializerSnapshot<DynamicRecordInternal> oldSerializerSnapshot) {
      return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }

    @Override
    public TypeSerializer<DynamicRecordInternal> restoreSerializer() {
      // Note: We pass in a null serializer cache which would create issues if we tried to use this
      // restored serializer, but since we are using {@code
      // TypeSerializerSchemaCompatibility.compatibleAsIs()} above, this serializer will never be
      // used. A new one will be created via {@code DynamicRecordInternalType}.
      return new DynamicRecordInternalSerializer(null, writeSchemaAndSpec);
    }
  }
}
