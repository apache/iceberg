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

import java.util.List;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.avro.AvroSchemaUtil;

class V1Metadata {
  private V1Metadata() {
  }

  static Schema MANIFEST_LIST_SCHEMA = new Schema(
      ManifestFile.PATH, ManifestFile.LENGTH, ManifestFile.SPEC_ID, ManifestFile.SNAPSHOT_ID,
      ManifestFile.ADDED_FILES_COUNT, ManifestFile.EXISTING_FILES_COUNT, ManifestFile.DELETED_FILES_COUNT,
      ManifestFile.PARTITION_SUMMARIES,
      ManifestFile.ADDED_ROWS_COUNT, ManifestFile.EXISTING_ROWS_COUNT, ManifestFile.DELETED_ROWS_COUNT);

  /**
   * A wrapper class to write any ManifestFile implementation to Avro using the v1 schema.
   *
   * This is used to maintain compatibility with v1 by writing manifest list files with the old schema, instead of
   * writing a sequence number into metadata files in v1 tables.
   */
  static class IndexedManifestFile implements ManifestFile, IndexedRecord {
    private static final org.apache.avro.Schema AVRO_SCHEMA =
        AvroSchemaUtil.convert(MANIFEST_LIST_SCHEMA, "manifest_file");

    private ManifestFile wrapped = null;

    public ManifestFile wrap(ManifestFile file) {
      this.wrapped = file;
      return this;
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return AVRO_SCHEMA;
    }

    @Override
    public void put(int i, Object v) {
      throw new UnsupportedOperationException("Cannot read using IndexedManifestFile");
    }

    @Override
    public Object get(int pos) {
      switch (pos) {
        case 0:
          return path();
        case 1:
          return length();
        case 2:
          return partitionSpecId();
        case 3:
          return snapshotId();
        case 4:
          return addedFilesCount();
        case 5:
          return existingFilesCount();
        case 6:
          return deletedFilesCount();
        case 7:
          return partitions();
        case 8:
          return addedRowsCount();
        case 9:
          return existingRowsCount();
        case 10:
          return deletedRowsCount();
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public String path() {
      return wrapped.path();
    }

    @Override
    public long length() {
      return wrapped.length();
    }

    @Override
    public int partitionSpecId() {
      return wrapped.partitionSpecId();
    }

    @Override
    public long sequenceNumber() {
      return wrapped.sequenceNumber();
    }

    @Override
    public long minSequenceNumber() {
      return wrapped.minSequenceNumber();
    }

    @Override
    public Long snapshotId() {
      return wrapped.snapshotId();
    }

    @Override
    public boolean hasAddedFiles() {
      return wrapped.hasAddedFiles();
    }

    @Override
    public Integer addedFilesCount() {
      return wrapped.addedFilesCount();
    }

    @Override
    public Long addedRowsCount() {
      return wrapped.addedRowsCount();
    }

    @Override
    public boolean hasExistingFiles() {
      return wrapped.hasExistingFiles();
    }

    @Override
    public Integer existingFilesCount() {
      return wrapped.existingFilesCount();
    }

    @Override
    public Long existingRowsCount() {
      return wrapped.existingRowsCount();
    }

    @Override
    public boolean hasDeletedFiles() {
      return wrapped.hasDeletedFiles();
    }

    @Override
    public Integer deletedFilesCount() {
      return wrapped.deletedFilesCount();
    }

    @Override
    public Long deletedRowsCount() {
      return wrapped.deletedRowsCount();
    }

    @Override
    public List<PartitionFieldSummary> partitions() {
      return wrapped.partitions();
    }

    @Override
    public ManifestFile copy() {
      return wrapped.copy();
    }
  }
}
