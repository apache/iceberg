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
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

/**
 * Base class for both {@link DataFile} and {@link DeleteFile}.
 */
abstract class BaseFile<F>
    implements ContentFile<F>, IndexedRecord, StructLike, SpecificData.SchemaConstructable, Serializable {
  static final Types.StructType EMPTY_STRUCT_TYPE = Types.StructType.of();
  static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_STRUCT_TYPE) {
    @Override
    public PartitionData copy() {
      return this; // this does not change
    }
  };

  private int[] fromProjectionPos;
  private Types.StructType partitionType;

  private FileContent content = FileContent.DATA;
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
  private transient Schema avroSchema = null;

  /**
   * Used by Avro reflection to instantiate this class when reading manifest files.
   */
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

  BaseFile(FileContent content, String filePath, FileFormat format,
           PartitionData partition, long fileSizeInBytes, long recordCount,
           Map<Integer, Long> columnSizes, Map<Integer, Long> valueCounts, Map<Integer, Long> nullValueCounts,
           Map<Integer, ByteBuffer> lowerBounds, Map<Integer, ByteBuffer> upperBounds, List<Long> splitOffsets,
           ByteBuffer keyMetadata) {
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
    this.lowerBounds = SerializableByteBufferMap.wrap(lowerBounds);
    this.upperBounds = SerializableByteBufferMap.wrap(upperBounds);
    this.splitOffsets = copy(splitOffsets);
    this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
  }

  /**
   * Copy constructor.
   *
   * @param toCopy a generic data file to copy.
   * @param fullCopy whether to copy all fields or to drop column-level stats
   */
  BaseFile(BaseFile<F> toCopy, boolean fullCopy) {
    this.content = toCopy.content;
    this.filePath = toCopy.filePath;
    this.format = toCopy.format;
    this.partitionData = toCopy.partitionData.copy();
    this.partitionType = toCopy.partitionType;
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
  BaseFile() {
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
        this.content = value != null ? FileContent.values()[(Integer) value] : FileContent.DATA;
        return;
      case 1:
        // always coerce to String for Serializable
        this.filePath = value.toString();
        return;
      case 2:
        this.format = FileFormat.valueOf(value.toString());
        return;
      case 3:
        this.partitionData = (PartitionData) value;
        return;
      case 4:
        this.recordCount = (Long) value;
        return;
      case 5:
        this.fileSizeInBytes = (Long) value;
        return;
      case 6:
        this.columnSizes = (Map<Integer, Long>) value;
        return;
      case 7:
        this.valueCounts = (Map<Integer, Long>) value;
        return;
      case 8:
        this.nullValueCounts = (Map<Integer, Long>) value;
        return;
      case 9:
        this.lowerBounds = SerializableByteBufferMap.wrap((Map<Integer, ByteBuffer>) value);
        return;
      case 10:
        this.upperBounds = SerializableByteBufferMap.wrap((Map<Integer, ByteBuffer>) value);
        return;
      case 11:
        this.keyMetadata = ByteBuffers.toByteArray((ByteBuffer) value);
        return;
      case 12:
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
        return content.id();
      case 1:
        return filePath;
      case 2:
        return format != null ? format.toString() : null;
      case 3:
        return partitionData;
      case 4:
        return recordCount;
      case 5:
        return fileSizeInBytes;
      case 6:
        return columnSizes;
      case 7:
        return valueCounts;
      case 8:
        return nullValueCounts;
      case 9:
        return lowerBounds;
      case 10:
        return upperBounds;
      case 11:
        return keyMetadata != null ? ByteBuffer.wrap(keyMetadata) : null;
      case 12:
        return splitOffsets;
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

  public FileContent content() {
    return content;
  }

  public CharSequence path() {
    return filePath;
  }

  public FileFormat format() {
    return format;
  }

  public StructLike partition() {
    return partitionData;
  }

  public long recordCount() {
    return recordCount;
  }

  public long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  public Map<Integer, Long> columnSizes() {
    return columnSizes;
  }

  public Map<Integer, Long> valueCounts() {
    return valueCounts;
  }

  public Map<Integer, Long> nullValueCounts() {
    return nullValueCounts;
  }

  public Map<Integer, ByteBuffer> lowerBounds() {
    return lowerBounds;
  }

  public Map<Integer, ByteBuffer> upperBounds() {
    return upperBounds;
  }

  public ByteBuffer keyMetadata() {
    return keyMetadata != null ? ByteBuffer.wrap(keyMetadata) : null;
  }

  public List<Long> splitOffsets() {
    return splitOffsets;
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("content", content.toString().toLowerCase(Locale.ROOT))
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
}
