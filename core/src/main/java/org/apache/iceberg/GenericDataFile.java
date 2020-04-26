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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

class GenericDataFile
    implements DataFile, IndexedRecord, StructLike, SpecificData.SchemaConstructable, Serializable {
  private static final Types.StructType EMPTY_STRUCT_TYPE = Types.StructType.of();
  private static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_STRUCT_TYPE) {
    @Override
    public PartitionData copy() {
      return this; // this does not change
    }
  };

  private boolean skipEntryFields;
  private int[] fromProjectionPos;
  private Types.StructType partitionType;

  // ManifestEntry fields
  private ManifestEntry asEntry = null;
  private FileStatus status = FileStatus.ADDED;
  private Long snapshotId = null;
  private Long sequenceNumber = null;

  private String filePath = null;
  private FileFormat format = null;
  private PartitionData partitionData = null;
  private Long recordCount = null;
  private long fileSizeInBytes = -1L;

  // optional fields
  private Map<Integer, Long> columnSizes = null;
  private Map<Integer, Long> valueCounts = null;
  private Map<Integer, Long> nullValueCounts = null;
  private Map<Integer, ByteBuffer> lowerBounds = null;
  private Map<Integer, ByteBuffer> upperBounds = null;
  private List<Long> splitOffsets = null;
  private byte[] keyMetadata = null;

  // cached schema
  private transient org.apache.avro.Schema avroSchema = null;

  /**
   * Used by Avro to instantiate this class when reading manifest files.
   */
  private GenericDataFile(org.apache.avro.Schema avroSchema) {
    this(avroSchema, null);
  }

  /**
   * Used by AsManifestEntry to instantiate this class when reading manifest files.
   */
  private GenericDataFile(org.apache.avro.Schema avroSchema, AsManifestEntry asEntry) {
    this.avroSchema = avroSchema;
    this.skipEntryFields = asEntry != null;

    Types.StructType schema = AvroSchemaUtil.convert(avroSchema).asNestedType().asStructType();

    // partition type may be null if the field was not projected
    Type partType = schema.fieldType("partition");
    if (partType != null) {
      this.partitionType = partType.asNestedType().asStructType();
    } else {
      this.partitionType = EMPTY_STRUCT_TYPE;
    }

    if (asEntry != null) {
      this.asEntry = asEntry;
    } else {
      this.asEntry = new AsManifestEntry(this, partitionType);
    }

    List<Types.NestedField> fields = schema.fields();
    List<Types.NestedField> allFields = DataFile.getType(partitionType).fields();
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

    this.partitionData = new PartitionData(partitionType);
  }

  GenericDataFile(String filePath, FileFormat format, long recordCount,
                  long fileSizeInBytes) {
    this.filePath = filePath;
    this.format = format;
    this.partitionData = EMPTY_PARTITION_DATA;
    this.partitionType = EMPTY_PARTITION_DATA.getPartitionType();
    this.asEntry = new AsManifestEntry(this, partitionType);
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSizeInBytes;
  }

  GenericDataFile(String filePath, FileFormat format, PartitionData partition,
                  long recordCount, long fileSizeInBytes) {
    this.filePath = filePath;
    this.format = format;
    this.partitionData = partition;
    this.partitionType = partition.getPartitionType();
    this.asEntry = new AsManifestEntry(this, partitionType);
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSizeInBytes;
  }

  GenericDataFile(String filePath, FileFormat format, PartitionData partition,
                  long fileSizeInBytes, Metrics metrics, List<Long> splitOffsets) {
    this.filePath = filePath;
    this.format = format;

    // this constructor is used by DataFiles.Builder, which passes null for unpartitioned data
    if (partition == null) {
      this.partitionData = EMPTY_PARTITION_DATA;
      this.partitionType = EMPTY_PARTITION_DATA.getPartitionType();
    } else {
      this.partitionData = partition;
      this.partitionType = partition.getPartitionType();
    }
    this.asEntry = new AsManifestEntry(this, partitionType);

    // this will throw NPE if metrics.recordCount is null
    this.recordCount = metrics.recordCount();
    this.fileSizeInBytes = fileSizeInBytes;
    this.columnSizes = metrics.columnSizes();
    this.valueCounts = metrics.valueCounts();
    this.nullValueCounts = metrics.nullValueCounts();
    this.lowerBounds = SerializableByteBufferMap.wrap(metrics.lowerBounds());
    this.upperBounds = SerializableByteBufferMap.wrap(metrics.upperBounds());
    this.splitOffsets = copy(splitOffsets);
  }

  GenericDataFile(String filePath, FileFormat format, PartitionData partition,
                  long fileSizeInBytes, Metrics metrics,
                  ByteBuffer keyMetadata, List<Long> splitOffsets) {
    this(filePath, format, partition, fileSizeInBytes, metrics, splitOffsets);
    this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
  }

  /**
   * Copy constructor.
   *
   * @param toCopy a generic data file to copy.
   * @param fullCopy whether to copy all fields or to drop column-level stats
   */
  private GenericDataFile(GenericDataFile toCopy, boolean fullCopy) {
    this.status = toCopy.status;
    this.snapshotId = toCopy.snapshotId;
    this.sequenceNumber = toCopy.sequenceNumber;
    this.filePath = toCopy.filePath;
    this.format = toCopy.format;
    this.partitionData = toCopy.partitionData.copy();
    this.partitionType = toCopy.partitionType;
    this.asEntry = new AsManifestEntry(this, partitionType);
    this.recordCount = toCopy.recordCount;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
    if (fullCopy) {
      // TODO: support lazy conversion to/from map
      this.columnSizes = copy(toCopy.columnSizes);
      this.valueCounts = copy(toCopy.valueCounts);
      this.nullValueCounts = copy(toCopy.nullValueCounts);
      this.lowerBounds = SerializableByteBufferMap.wrap(copy(toCopy.lowerBounds));
      this.upperBounds = SerializableByteBufferMap.wrap(copy(toCopy.upperBounds));
    } else {
      this.columnSizes = null;
      this.valueCounts = null;
      this.nullValueCounts = null;
      this.lowerBounds = null;
      this.upperBounds = null;
    }
    this.fromProjectionPos = toCopy.fromProjectionPos;
    this.keyMetadata = toCopy.keyMetadata == null ? null : Arrays.copyOf(toCopy.keyMetadata, toCopy.keyMetadata.length);
    this.splitOffsets = copy(toCopy.splitOffsets);
  }

  /**
   * Constructor for Java serialization.
   */
  GenericDataFile() {
  }

  @Override
  public FileStatus status() {
    return status;
  }

  @Override
  public Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long sequenceNumber() {
    return sequenceNumber;
  }

  public ManifestEntry asEntry() {
    return asEntry;
  }

  @Override
  public CharSequence path() {
    return filePath;
  }

  @Override
  public FileFormat format() {
    return format;
  }

  @Override
  public StructLike partition() {
    return partitionData;
  }

  @Override
  public long recordCount() {
    return recordCount;
  }

  @Override
  public long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  @Override
  public Map<Integer, Long> columnSizes() {
    return columnSizes;
  }

  @Override
  public Map<Integer, Long> valueCounts() {
    return valueCounts;
  }

  @Override
  public Map<Integer, Long> nullValueCounts() {
    return nullValueCounts;
  }

  @Override
  public Map<Integer, ByteBuffer> lowerBounds() {
    return lowerBounds;
  }

  @Override
  public Map<Integer, ByteBuffer> upperBounds() {
    return upperBounds;
  }

  @Override
  public ByteBuffer keyMetadata() {
    return keyMetadata != null ? ByteBuffer.wrap(keyMetadata) : null;
  }

  @Override
  public List<Long> splitOffsets() {
    return splitOffsets;
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    if (avroSchema == null) {
      this.avroSchema = getAvroSchema(partitionType);
    }
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    int pos = i;
    // if the schema was projected, map the incoming ordinal to the expected one
    if (fromProjectionPos != null) {
      pos = fromProjectionPos[i];
    }

    if (!(skipEntryFields && pos < 3)) {
      setInternal(pos, v);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> void setInternal(int pos, T value) {
    switch (pos) {
      case 0:
        this.status = FileStatus.values()[(Integer) value];
        return;
      case 1:
        this.snapshotId = (Long) value;
        return;
      case 2:
        this.sequenceNumber = (Long) value;
        return;
      case 3:
        // always coerce to String for Serializable
        this.filePath = value.toString();
        return;
      case 4:
        this.format = FileFormat.valueOf(value.toString());
        return;
      case 5:
        this.partitionData = (PartitionData) value;
        return;
      case 6:
        this.recordCount = (Long) value;
        return;
      case 7:
        this.fileSizeInBytes = (Long) value;
        return;
      case 8:
        this.columnSizes = (Map<Integer, Long>) value;
        return;
      case 9:
        this.valueCounts = (Map<Integer, Long>) value;
        return;
      case 10:
        this.nullValueCounts = (Map<Integer, Long>) value;
        return;
      case 11:
        this.lowerBounds = SerializableByteBufferMap.wrap((Map<Integer, ByteBuffer>) value);
        return;
      case 12:
        this.upperBounds = SerializableByteBufferMap.wrap((Map<Integer, ByteBuffer>) value);
        return;
      case 13:
        this.keyMetadata = ByteBuffers.toByteArray((ByteBuffer) value);
        return;
      case 14:
        this.splitOffsets = (List<Long>) value;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    put(pos, value);
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
        return status.id();
      case 1:
        return snapshotId;
      case 2:
        return sequenceNumber;
      case 3:
        return filePath;
      case 4:
        return format != null ? format.toString() : null;
      case 5:
        return partitionData;
      case 6:
        return recordCount;
      case 7:
        return fileSizeInBytes;
      case 8:
        return columnSizes;
      case 9:
        return valueCounts;
      case 10:
        return nullValueCounts;
      case 11:
        return lowerBounds;
      case 12:
        return upperBounds;
      case 13:
        return keyMetadata();
      case 14:
        return splitOffsets;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  private static org.apache.avro.Schema getAvroSchema(Types.StructType partitionType) {
    Types.StructType type = DataFile.getType(partitionType);
    return AvroSchemaUtil.convert(type, ImmutableMap.of(
        type, GenericDataFile.class.getName(),
        partitionType, PartitionData.class.getName()));
  }

  @Override
  public int size() {
    return 15;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("status", status)
        .add("snapshot_id", snapshotId)
        .add("sequence_number", sequenceNumber)
        .add("file_path", filePath)
        .add("file_format", format)
        .add("partition", partitionData)
        .add("record_count", recordCount)
        .add("file_size_in_bytes", fileSizeInBytes)
        .add("column_sizes", columnSizes)
        .add("value_counts", valueCounts)
        .add("null_value_counts", nullValueCounts)
        .add("lower_bounds", lowerBounds)
        .add("upper_bounds", upperBounds)
        .add("key_metadata", keyMetadata == null ? "null" : "(redacted)")
        .add("split_offsets", splitOffsets == null ? "null" : splitOffsets)
        .toString();
  }

  @Override
  public GenericDataFile copyWithoutStats() {
    return new GenericDataFile(this, false /* drop stats */);
  }

  @Override
  public GenericDataFile copy() {
    return new GenericDataFile(this, true /* full copy */);
  }

  private static <K, V> Map<K, V> copy(Map<K, V> map) {
    if (map != null) {
      Map<K, V> copy = Maps.newHashMapWithExpectedSize(map.size());
      copy.putAll(map);
      return Collections.unmodifiableMap(copy);
    }
    return null;
  }

  private static <E> List<E> copy(List<E> list) {
    if (list != null) {
      List<E> copy = Lists.newArrayListWithExpectedSize(list.size());
      copy.addAll(list);
      return Collections.unmodifiableList(copy);
    }
    return null;
  }

  /**
   * An adapter that makes a DataFile appear like a ManifestEntry for v1 metadata files.
   */
  static class AsManifestEntry
      implements ManifestEntry, IndexedRecord, SpecificData.SchemaConstructable, StructLike, Serializable {
    private transient Schema avroSchema = null;
    private GenericDataFile file = null;

    /**
     * Used by Avro reflection to instantiate this class when reading manifest files.
     *
     * @param avroSchema an Avro read schema
     */
    protected AsManifestEntry(Schema avroSchema) {
      this.avroSchema = avroSchema;
      this.file = new GenericDataFile(avroSchema.getField("data_file").schema(), this);
    }

    /**
     * Used by DataFile to create a ManifestEntry adapter.
     *
     * @param file a GenericDataFile that contains manifest entry data
     */
    protected AsManifestEntry(GenericDataFile file, Types.StructType partitionType) {
      this.avroSchema = AvroSchemaUtil.convert(ManifestEntry.getSchema(partitionType), "manifest_entry");
      this.file = file;
    }

    /**
     * Constructor for Java serialization.
     */
    AsManifestEntry() {
    }

    @Override
    public Schema getSchema() {
      return avroSchema;
    }

    @Override
    public Status status() {
      return ManifestEntry.Status.values()[file.status.id()];
    }

    @Override
    public Long snapshotId() {
      return file.snapshotId;
    }

    @Override
    public void setSnapshotId(long snapshotId) {
      file.snapshotId = snapshotId;
    }

    @Override
    public Long sequenceNumber() {
      return file.sequenceNumber;
    }

    @Override
    public void setSequenceNumber(long sequenceNumber) {
      file.sequenceNumber = sequenceNumber;
    }

    @Override
    public DataFile file() {
      return file;
    }

    @Override
    public ManifestEntry copy() {
      return file.copy().asEntry;
    }

    @Override
    public ManifestEntry copyWithoutStats() {
      return file.copyWithoutStats().asEntry;
    }

    @Override
    public void put(int pos, Object value) {
      switch (pos) {
        case 0:
          file.status = FileStatus.values()[(Integer) value];
          return;
        case 1:
          file.snapshotId = (Long) value;
          return;
        case 2:
          file.sequenceNumber = (Long) value;
          return;
        case 3:
          if (file != value) {
            throw new IllegalArgumentException("Cannot replace data file");
          }
          return;
        default:
          // ignore the object, it must be from a newer version of the format
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      put(pos, value);
    }

    @Override
    public Object get(int i) {
      switch (i) {
        case 0:
          return file.status.id();
        case 1:
          return file.snapshotId;
        case 2:
          return file.sequenceNumber;
        case 3:
          return file;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + i);
      }
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    @Override
    public int size() {
      return avroSchema.getFields().size();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("status", file.status)
          .add("snapshot_id", file.snapshotId)
          .add("sequence_number", file.sequenceNumber)
          .add("file", file)
          .toString();
    }
  }
}
