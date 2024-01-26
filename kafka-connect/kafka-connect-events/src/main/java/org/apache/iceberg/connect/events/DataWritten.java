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
package org.apache.iceberg.connect.events;

import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.UUIDType;

/**
 * A control event payload for events sent by a worker that contains the table data that has been
 * written and is ready to commit.
 */
public class DataWritten implements Payload {

  private StructType partitionType;

  private UUID commitId;
  private TableReference tableReference;
  private List<DataFile> dataFiles;
  private List<DeleteFile> deleteFiles;
  private StructType icebergSchema;
  private final Schema avroSchema;

  static final int COMMIT_ID = 10_300;
  static final int TABLE_REFERENCE = 10_301;
  static final int DATA_FILES = 10_302;
  static final int DATA_FILES_ELEMENT = 10_303;
  static final int DELETE_FILES = 10_304;
  static final int DELETE_FILES_ELEMENT = 10_304;

  // Used by Avro reflection to instantiate this class when reading events, note that this does not
  // set the partition type so the instance cannot be re-serialized
  public DataWritten(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public DataWritten(
      StructType partitionType,
      UUID commitId,
      TableReference tableReference,
      List<DataFile> dataFiles,
      List<DeleteFile> deleteFiles) {
    this.partitionType = partitionType;
    this.commitId = commitId;
    this.tableReference = tableReference;
    this.dataFiles = dataFiles;
    this.deleteFiles = deleteFiles;
    this.avroSchema = AvroUtil.convert(writeSchema(), getClass());
  }

  @Override
  public PayloadType type() {
    return PayloadType.DATA_WRITTEN;
  }

  public UUID commitId() {
    return commitId;
  }

  public TableReference tableReference() {
    return tableReference;
  }

  public List<DataFile> dataFiles() {
    return dataFiles;
  }

  public List<DeleteFile> deleteFiles() {
    return deleteFiles;
  }

  @Override
  public StructType writeSchema() {
    if (icebergSchema == null) {
      StructType dataFileStruct = DataFile.getType(partitionType);

      this.icebergSchema =
          StructType.of(
              NestedField.required(COMMIT_ID, "commit_id", UUIDType.get()),
              NestedField.required(
                  TABLE_REFERENCE, "table_reference", TableReference.ICEBERG_SCHEMA),
              NestedField.optional(
                  DATA_FILES,
                  "data_files",
                  ListType.ofRequired(DATA_FILES_ELEMENT, dataFileStruct)),
              NestedField.optional(
                  DELETE_FILES,
                  "delete_files",
                  ListType.ofRequired(DELETE_FILES_ELEMENT, dataFileStruct)));
    }

    return icebergSchema;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case COMMIT_ID:
        this.commitId = (UUID) v;
        return;
      case TABLE_REFERENCE:
        this.tableReference = (TableReference) v;
        return;
      case DATA_FILES:
        this.dataFiles = (List<DataFile>) v;
        return;
      case DELETE_FILES:
        this.deleteFiles = (List<DeleteFile>) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (AvroUtil.positionToId(i, avroSchema)) {
      case COMMIT_ID:
        return commitId;
      case TABLE_REFERENCE:
        return tableReference;
      case DATA_FILES:
        return dataFiles;
      case DELETE_FILES:
        return deleteFiles;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }
}
