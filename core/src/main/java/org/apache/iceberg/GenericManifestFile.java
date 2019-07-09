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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;

public class GenericManifestFile
    implements ManifestFile, StructLike, IndexedRecord, SchemaConstructable, Serializable {
  private static final Schema AVRO_SCHEMA = AvroSchemaUtil.convert(
      ManifestFile.schema(), "manifest_file");

  private transient Schema avroSchema; // not final for Java serialization
  private int[] fromProjectionPos;

  // data fields
  private InputFile file = null;
  private String manifestPath = null;
  private Long length = null;
  private int specId = -1;
  private Long snapshotId = null;
  private Integer addedFilesCount = null;
  private Integer existingFilesCount = null;
  private Integer deletedFilesCount = null;
  private List<PartitionFieldSummary> partitions = null;

  /**
   * Used by Avro reflection to instantiate this class when reading manifest files.
   */
  public GenericManifestFile(org.apache.avro.Schema avroSchema) {
    this.avroSchema = avroSchema;

    List<Types.NestedField> fields = AvroSchemaUtil.convert(avroSchema)
        .asNestedType()
        .asStructType()
        .fields();
    List<Types.NestedField> allFields = ManifestFile.schema().asStruct().fields();

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

  GenericManifestFile(InputFile file, int specId) {
    this.avroSchema = AVRO_SCHEMA;
    this.file = file;
    this.manifestPath = file.location();
    this.length = null; // lazily loaded from file
    this.specId = specId;
    this.snapshotId = null;
    this.addedFilesCount = null;
    this.existingFilesCount = null;
    this.deletedFilesCount = null;
    this.partitions = null;
    this.fromProjectionPos = null;
  }

  public GenericManifestFile(String path, long length, int specId, long snapshotId,
                             int addedFilesCount, int existingFilesCount, int deletedFilesCount,
                             List<PartitionFieldSummary> partitions) {
    this.avroSchema = AVRO_SCHEMA;
    this.manifestPath = path;
    this.length = length;
    this.specId = specId;
    this.snapshotId = snapshotId;
    this.addedFilesCount = addedFilesCount;
    this.existingFilesCount = existingFilesCount;
    this.deletedFilesCount = deletedFilesCount;
    this.partitions = partitions;
    this.fromProjectionPos = null;
  }

  /**
   * Copy constructor.
   *
   * @param toCopy a generic manifest file to copy.
   */
  private GenericManifestFile(GenericManifestFile toCopy) {
    this.avroSchema = toCopy.avroSchema;
    this.manifestPath = toCopy.manifestPath;
    this.length = toCopy.length;
    this.specId = toCopy.specId;
    this.snapshotId = toCopy.snapshotId;
    this.addedFilesCount = toCopy.addedFilesCount;
    this.existingFilesCount = toCopy.existingFilesCount;
    this.deletedFilesCount = toCopy.deletedFilesCount;
    this.partitions = ImmutableList.copyOf(Iterables.transform(toCopy.partitions, PartitionFieldSummary::copy));
    this.fromProjectionPos = toCopy.fromProjectionPos;
  }

  /**
   * Constructor for Java serialization.
   */
  GenericManifestFile() {
  }

  @Override
  public String path() {
    return manifestPath;
  }

  public Long lazyLength() {
    if (length == null) {
      if (file != null) {
        // this was created from an input file and length is lazily loaded
        this.length = file.getLength();
      } else {
        // this was loaded from a file without projecting length, throw an exception
        return null;
      }
    }
    return length;
  }

  @Override
  public long length() {
    return lazyLength();
  }

  @Override
  public int partitionSpecId() {
    return specId;
  }

  @Override
  public Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Integer addedFilesCount() {
    return addedFilesCount;
  }

  @Override
  public Integer existingFilesCount() {
    return existingFilesCount;
  }

  @Override
  public Integer deletedFilesCount() {
    return deletedFilesCount;
  }

  @Override
  public List<PartitionFieldSummary> partitions() {
    return partitions;
  }

  @Override
  public int size() {
    return ManifestFile.schema().columns().size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
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
        return manifestPath;
      case 1:
        return lazyLength();
      case 2:
        return specId;
      case 3:
        return snapshotId;
      case 4:
        return addedFilesCount;
      case 5:
        return existingFilesCount;
      case 6:
        return deletedFilesCount;
      case 7:
        return partitions;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> void set(int i, T value) {
    int pos = i;
    // if the schema was projected, map the incoming ordinal to the expected one
    if (fromProjectionPos != null) {
      pos = fromProjectionPos[i];
    }
    switch (pos) {
      case 0:
        // always coerce to String for Serializable
        this.manifestPath = value.toString();
        return;
      case 1:
        this.length = (Long) value;
        return;
      case 2:
        this.specId = (Integer) value;
        return;
      case 3:
        this.snapshotId = (Long) value;
        return;
      case 4:
        this.addedFilesCount = (Integer) value;
        return;
      case 5:
        this.existingFilesCount = (Integer) value;
        return;
      case 6:
        this.deletedFilesCount = (Integer) value;
        return;
      case 7:
        this.partitions = (List<PartitionFieldSummary>) value;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public void put(int i, Object v) {
    set(i, v);
  }

  @Override
  public ManifestFile copy() {
    return new GenericManifestFile(this);
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    GenericManifestFile that = (GenericManifestFile) other;
    return Objects.equal(manifestPath, that.manifestPath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(manifestPath);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("path", manifestPath)
        .add("length", length)
        .add("partition_spec_id", specId)
        .add("added_snapshot_id", snapshotId)
        .add("added_data_files_count", addedFilesCount)
        .add("existing_data_files_count", existingFilesCount)
        .add("deleted_data_files_count", deletedFilesCount)
        .add("partitions", partitions)
        .toString();
  }
}
