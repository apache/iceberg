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
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.hadoop.util.Sets;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;

@Internal
class DynamicRecordInternalSerializer extends TypeSerializer<DynamicRecordInternal> {

  private static final long serialVersionUID = 1L;

  private final TableSerializerCache serializerCache;
  private final boolean writeSchemaAndSpec;

  DynamicRecordInternalSerializer(
      TableSerializerCache serializerCache, boolean writeSchemaAndSpec) {
    this.serializerCache = serializerCache;
    this.writeSchemaAndSpec = writeSchemaAndSpec;
  }

  @Override
  public TypeSerializer<DynamicRecordInternal> duplicate() {
    return new DynamicRecordInternalSerializer(
        new TableSerializerCache(serializerCache.catalogLoader(), serializerCache.maximumSize()),
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
    final RowDataSerializer rowDataSerializer;
    if (writeSchemaAndSpec) {
      rowDataSerializer =
          serializerCache.serializer(
              toSerialize.tableName(), toSerialize.schema(), toSerialize.spec());
    } else {
      // Check that the schema id can be resolved. Not strictly necessary for serialization.
      Tuple3<RowDataSerializer, Schema, PartitionSpec> serializer =
          serializerCache.serializerWithSchemaAndSpec(
              toSerialize.tableName(),
              toSerialize.schema().schemaId(),
              toSerialize.spec().specId());
      rowDataSerializer = serializer.f0;
    }

    rowDataSerializer.serialize(toSerialize.rowData(), dataOutputView);
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

    final Schema schema;
    final PartitionSpec spec;
    final RowDataSerializer rowDataSerializer;
    if (writeSchemaAndSpec) {
      schema = SchemaParser.fromJson(dataInputView.readUTF());
      spec = PartitionSpecParser.fromJson(schema, dataInputView.readUTF());
      rowDataSerializer = serializerCache.serializer(tableName, schema, spec);
    } else {
      Integer schemaId = dataInputView.readInt();
      Integer specId = dataInputView.readInt();
      Tuple3<RowDataSerializer, Schema, PartitionSpec> serializerWithSchemaAndSpec =
          serializerCache.serializerWithSchemaAndSpec(tableName, schemaId, specId);
      schema = serializerWithSchemaAndSpec.f1;
      spec = serializerWithSchemaAndSpec.f2;
      rowDataSerializer = serializerWithSchemaAndSpec.f0;
    }

    int writerKey = dataInputView.readInt();
    RowData rowData = rowDataSerializer.deserialize(dataInputView);
    boolean upsertMode = dataInputView.readBoolean();
    int numEqualityFields = dataInputView.readInt();
    final Set<Integer> equalityFieldIds;
    if (numEqualityFields > 0) {
      equalityFieldIds = Sets.newHashSetWithExpectedSize(numEqualityFields);
    } else {
      equalityFieldIds = Collections.emptySet();
    }

    for (int i = 0; i < numEqualityFields; i++) {
      equalityFieldIds.add(dataInputView.readInt());
    }

    return new DynamicRecordInternal(
        tableName, branch, schema, rowData, spec, writerKey, upsertMode, equalityFieldIds);
  }

  @Override
  public DynamicRecordInternal deserialize(DynamicRecordInternal reuse, DataInputView dataInputView)
      throws IOException {
    String tableName = dataInputView.readUTF();
    reuse.setTableName(tableName);
    String branch = dataInputView.readUTF();
    reuse.setBranch(branch);

    final Schema schema;
    final PartitionSpec spec;
    final RowDataSerializer rowDataSerializer;
    if (writeSchemaAndSpec) {
      schema = SchemaParser.fromJson(dataInputView.readUTF());
      spec = PartitionSpecParser.fromJson(schema, dataInputView.readUTF());
      reuse.setSchema(schema);
      reuse.setSpec(spec);
      rowDataSerializer = serializerCache.serializer(tableName, schema, spec);
    } else {
      Integer schemaId = dataInputView.readInt();
      Integer specId = dataInputView.readInt();
      Tuple3<RowDataSerializer, Schema, PartitionSpec> serializerWithSchemaAndSpec =
          serializerCache.serializerWithSchemaAndSpec(tableName, schemaId, specId);
      schema = serializerWithSchemaAndSpec.f1;
      spec = serializerWithSchemaAndSpec.f2;
      rowDataSerializer = serializerWithSchemaAndSpec.f0;
    }

    int writerKey = dataInputView.readInt();
    reuse.setWriterKey(writerKey);
    RowData rowData = rowDataSerializer.deserialize(dataInputView);
    boolean upsertMode = dataInputView.readBoolean();
    int numEqualityFields = dataInputView.readInt();
    final Set<Integer> equalityFieldIds;
    if (numEqualityFields > 0) {
      equalityFieldIds = Sets.newHashSetWithExpectedSize(numEqualityFields);
    } else {
      equalityFieldIds = Collections.emptySet();
    }
    for (int i = 0; i < numEqualityFields; i++) {
      equalityFieldIds.add(dataInputView.readInt());
    }
    return new DynamicRecordInternal(
        tableName, branch, schema, rowData, spec, writerKey, upsertMode, equalityFieldIds);
  }

  @Override
  public DynamicRecordInternal copy(DynamicRecordInternal from) {
    return new DynamicRecordInternal(
        from.tableName(),
        from.branch(),
        from.schema(),
        from.rowData(),
        from.spec(),
        from.writerKey(),
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
    return Boolean.hashCode(writeSchemaAndSpec);
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
