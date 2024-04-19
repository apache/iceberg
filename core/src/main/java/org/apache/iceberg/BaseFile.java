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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.SerializableMap;

/** Base class for both {@link DataFile} and {@link DeleteFile}. */
abstract class BaseFile<F>
    implements ContentFile<F>,
        IndexedRecord,
        StructLike,
        SpecificData.SchemaConstructable,
        Serializable {
  private static final FileContent[] FILE_CONTENT_VALUES = FileContent.values();
  static final Types.StructType EMPTY_STRUCT_TYPE = Types.StructType.of();
  static final PartitionData EMPTY_PARTITION_DATA =
      new PartitionData(EMPTY_STRUCT_TYPE) {
        @Override
        public PartitionData copy() {
          return this; // this does not change
        }
      };

  private int[] fromProjectionPos;
  private Types.StructType partitionType;

  private Long fileOrdinal = null;
  private int partitionSpecId = -1;
  private FileContent content = FileContent.DATA;
  private String filePath = null;
  private FileFormat format = null;
  private PartitionData partitionData = null;
  private Long recordCount = null;
  private long fileSizeInBytes = -1L;
  private Long dataSequenceNumber = null;
  private Long fileSequenceNumber = null;

  // optional fields
  private Map<Integer, Long> columnSizes = null;
  private Map<Integer, Long> valueCounts = null;
  private Map<Integer, Long> nullValueCounts = null;
  private Map<Integer, Long> nanValueCounts = null;
  private Map<Integer, ByteBuffer> lowerBounds = null;
  private Map<Integer, ByteBuffer> upperBounds = null;
  private long[] splitOffsets = null;
  private int[] equalityIds = null;
  private byte[] keyMetadata = null;
  private Integer sortOrderId;

  // cached schema
  private transient Schema avroSchema = null;

  /** Used by Avro reflection to instantiate this class when reading manifest files. */
  BaseFile(Schema avroSchema) {
    this.avroSchema = avroSchema;

    Types.StructType schema = AvroSchemaUtil.convert(avroSchema).asNestedType().asStructType();

    // partition type may be null if the field was not projected
    Type partType = schema.fieldType("partition");
    if (partType != null) {
      this.partitionType = partType.asNestedType().asStructType();
    } else {
      this.partitionType = EMPTY_STRUCT_TYPE;
    }

    List<Types.NestedField> fields = schema.fields();
    List<Types.NestedField> allFields = Lists.newArrayList();
    allFields.addAll(DataFile.getType(partitionType).fields());
    allFields.add(MetadataColumns.ROW_POSITION);

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

  BaseFile(
      int specId,
      FileContent content,
      String filePath,
      FileFormat format,
      PartitionData partition,
      long fileSizeInBytes,
      long recordCount,
      Map<Integer, Long> columnSizes,
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullValueCounts,
      Map<Integer, Long> nanValueCounts,
      Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds,
      List<Long> splitOffsets,
      int[] equalityFieldIds,
      Integer sortOrderId,
      ByteBuffer keyMetadata) {
    this.partitionSpecId = specId;
    this.content = content;
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

    // this will throw NPE if metrics.recordCount is null
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSizeInBytes;
    this.columnSizes = columnSizes;
    this.valueCounts = valueCounts;
    this.nullValueCounts = nullValueCounts;
    this.nanValueCounts = nanValueCounts;
    this.lowerBounds = SerializableByteBufferMap.wrap(lowerBounds);
    this.upperBounds = SerializableByteBufferMap.wrap(upperBounds);
    this.splitOffsets = ArrayUtil.toLongArray(splitOffsets);
    this.equalityIds = equalityFieldIds;
    this.sortOrderId = sortOrderId;
    this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
  }

  /**
   * Copy constructor.
   *
   * @param toCopy a generic data file to copy.
   * @param copyStats whether to copy all fields or to drop column-level stats
   * @param requestedColumnIds column ids for which to keep stats. If <code>null</code> then every
   *     column stat is kept.
   */
  BaseFile(BaseFile<F> toCopy, boolean copyStats, Set<Integer> requestedColumnIds) {
    this.fileOrdinal = toCopy.fileOrdinal;
    this.partitionSpecId = toCopy.partitionSpecId;
    this.content = toCopy.content;
    this.filePath = toCopy.filePath;
    this.format = toCopy.format;
    this.partitionData = toCopy.partitionData.copy();
    this.partitionType = toCopy.partitionType;
    this.recordCount = toCopy.recordCount;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
    if (copyStats) {
      this.columnSizes = copyMap(toCopy.columnSizes, requestedColumnIds);
      this.valueCounts = copyMap(toCopy.valueCounts, requestedColumnIds);
      this.nullValueCounts = copyMap(toCopy.nullValueCounts, requestedColumnIds);
      this.nanValueCounts = copyMap(toCopy.nanValueCounts, requestedColumnIds);
      this.lowerBounds = copyByteBufferMap(toCopy.lowerBounds, requestedColumnIds);
      this.upperBounds = copyByteBufferMap(toCopy.upperBounds, requestedColumnIds);
    } else {
      this.columnSizes = null;
      this.valueCounts = null;
      this.nullValueCounts = null;
      this.nanValueCounts = null;
      this.lowerBounds = null;
      this.upperBounds = null;
    }
    this.fromProjectionPos = toCopy.fromProjectionPos;
    this.keyMetadata =
        toCopy.keyMetadata == null
            ? null
            : Arrays.copyOf(toCopy.keyMetadata, toCopy.keyMetadata.length);
    this.splitOffsets =
        toCopy.splitOffsets == null
            ? null
            : Arrays.copyOf(toCopy.splitOffsets, toCopy.splitOffsets.length);
    this.equalityIds =
        toCopy.equalityIds != null
            ? Arrays.copyOf(toCopy.equalityIds, toCopy.equalityIds.length)
            : null;
    this.sortOrderId = toCopy.sortOrderId;
    this.dataSequenceNumber = toCopy.dataSequenceNumber;
    this.fileSequenceNumber = toCopy.fileSequenceNumber;
  }

  /** Constructor for Java serialization. */
  BaseFile() {}

  @Override
  public int specId() {
    return partitionSpecId;
  }

  void setSpecId(int specId) {
    this.partitionSpecId = specId;
  }

  @Override
  public Long dataSequenceNumber() {
    return dataSequenceNumber;
  }

  public void setDataSequenceNumber(Long dataSequenceNumber) {
    this.dataSequenceNumber = dataSequenceNumber;
  }

  @Override
  public Long fileSequenceNumber() {
    return fileSequenceNumber;
  }

  public void setFileSequenceNumber(Long fileSequenceNumber) {
    this.fileSequenceNumber = fileSequenceNumber;
  }

  protected abstract Schema getAvroSchema(Types.StructType partitionStruct);

  @Override
  public Schema getSchema() {
    if (avroSchema == null) {
      this.avroSchema = getAvroSchema(partitionType);
    }
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object value) {
    int pos = i;
    // if the schema was projected, map the incoming ordinal to the expected one
    if (fromProjectionPos != null) {
      pos = fromProjectionPos[i];
    }
    switch (pos) {
      case 0:
        this.content = value != null ? FILE_CONTENT_VALUES[(Integer) value] : FileContent.DATA;
        return;
      case 1:
        // always coerce to String for Serializable
        this.filePath = value.toString();
        return;
      case 2:
        this.format = FileFormat.fromString(value.toString());
        return;
      case 3:
        this.partitionSpecId = (value != null) ? (Integer) value : -1;
        return;
      case 4:
        this.partitionData = (PartitionData) value;
        return;
      case 5:
        this.recordCount = (Long) value;
        return;
      case 6:
        this.fileSizeInBytes = (Long) value;
        return;
      case 7:
        this.columnSizes = (Map<Integer, Long>) value;
        return;
      case 8:
        this.valueCounts = (Map<Integer, Long>) value;
        return;
      case 9:
        this.nullValueCounts = (Map<Integer, Long>) value;
        return;
      case 10:
        this.nanValueCounts = (Map<Integer, Long>) value;
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
        this.splitOffsets = ArrayUtil.toLongArray((List<Long>) value);
        return;
      case 15:
        this.equalityIds = ArrayUtil.toIntArray((List<Integer>) value);
        return;
      case 16:
        this.sortOrderId = (Integer) value;
        return;
      case 17:
        this.fileOrdinal = (long) value;
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
        return content.id();
      case 1:
        return filePath;
      case 2:
        return format != null ? format.toString() : null;
      case 3:
        return partitionSpecId;
      case 4:
        return partitionData;
      case 5:
        return recordCount;
      case 6:
        return fileSizeInBytes;
      case 7:
        return columnSizes;
      case 8:
        return valueCounts;
      case 9:
        return nullValueCounts;
      case 10:
        return nanValueCounts;
      case 11:
        return lowerBounds;
      case 12:
        return upperBounds;
      case 13:
        return keyMetadata();
      case 14:
        return splitOffsets();
      case 15:
        return equalityFieldIds();
      case 16:
        return sortOrderId;
      case 17:
        return fileOrdinal;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public int size() {
    return DataFile.getType(EMPTY_STRUCT_TYPE).fields().size();
  }

  @Override
  public Long pos() {
    return fileOrdinal;
  }

  @Override
  public FileContent content() {
    return content;
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
    return toReadableMap(columnSizes);
  }

  @Override
  public Map<Integer, Long> valueCounts() {
    return toReadableMap(valueCounts);
  }

  @Override
  public Map<Integer, Long> nullValueCounts() {
    return toReadableMap(nullValueCounts);
  }

  @Override
  public Map<Integer, Long> nanValueCounts() {
    return toReadableMap(nanValueCounts);
  }

  @Override
  public Map<Integer, ByteBuffer> lowerBounds() {
    return toReadableByteBufferMap(lowerBounds);
  }

  @Override
  public Map<Integer, ByteBuffer> upperBounds() {
    return toReadableByteBufferMap(upperBounds);
  }

  @Override
  public ByteBuffer keyMetadata() {
    return keyMetadata != null ? ByteBuffer.wrap(keyMetadata) : null;
  }

  @Override
  public List<Long> splitOffsets() {
    if (hasWellDefinedOffsets()) {
      return ArrayUtil.toUnmodifiableLongList(splitOffsets);
    }

    return null;
  }

  long[] splitOffsetArray() {
    if (hasWellDefinedOffsets()) {
      return splitOffsets;
    }

    return null;
  }

  private boolean hasWellDefinedOffsets() {
    // If the last split offset is past the file size this means the split offsets are corrupted and
    // should not be used
    return splitOffsets != null
        && splitOffsets.length != 0
        && splitOffsets[splitOffsets.length - 1] < fileSizeInBytes;
  }

  @Override
  public List<Integer> equalityFieldIds() {
    return ArrayUtil.toIntList(equalityIds);
  }

  @Override
  public Integer sortOrderId() {
    return sortOrderId;
  }

  private static <K, V> Map<K, V> copyMap(Map<K, V> map, Set<K> keys) {
    return keys == null ? SerializableMap.copyOf(map) : SerializableMap.filteredCopyOf(map, keys);
  }

  private static Map<Integer, ByteBuffer> copyByteBufferMap(
      Map<Integer, ByteBuffer> map, Set<Integer> keys) {
    return SerializableByteBufferMap.wrap(copyMap(map, keys));
  }

  private static <K, V> Map<K, V> toReadableMap(Map<K, V> map) {
    if (map == null) {
      return null;
    } else if (map instanceof SerializableMap) {
      return ((SerializableMap<K, V>) map).immutableMap();
    } else {
      return Collections.unmodifiableMap(map);
    }
  }

  private static Map<Integer, ByteBuffer> toReadableByteBufferMap(Map<Integer, ByteBuffer> map) {
    if (map == null) {
      return null;
    } else if (map instanceof SerializableByteBufferMap) {
      return ((SerializableByteBufferMap) map).immutableMap();
    } else {
      return Collections.unmodifiableMap(map);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("content", content.toString().toLowerCase(Locale.ROOT))
        .add("file_path", filePath)
        .add("file_format", format)
        .add("spec_id", specId())
        .add("partition", partitionData)
        .add("record_count", recordCount)
        .add("file_size_in_bytes", fileSizeInBytes)
        .add("column_sizes", columnSizes)
        .add("value_counts", valueCounts)
        .add("null_value_counts", nullValueCounts)
        .add("nan_value_counts", nanValueCounts)
        .add("lower_bounds", lowerBounds)
        .add("upper_bounds", upperBounds)
        .add("key_metadata", keyMetadata == null ? "null" : "(redacted)")
        .add("split_offsets", splitOffsets == null ? "null" : splitOffsets())
        .add("equality_ids", equalityIds == null ? "null" : equalityFieldIds())
        .add("sort_order_id", sortOrderId)
        .add("data_sequence_number", dataSequenceNumber == null ? "null" : dataSequenceNumber)
        .add("file_sequence_number", fileSequenceNumber == null ? "null" : fileSequenceNumber)
        .toString();
  }
}
