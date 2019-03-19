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

package com.netflix.iceberg;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.avro.AvroSchemaUtil;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.util.ByteBuffers;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

class GenericDataFile
    implements DataFile, IndexedRecord, StructLike, SpecificData.SchemaConstructable, Serializable {
  private static final Types.StructType EMPTY_STRUCT_TYPE = Types.StructType.of();
  private static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_STRUCT_TYPE) {
    @Override
    public PartitionData copy() {
      return this; // this does not change
    }
  };

  private int[] fromProjectionPos;
  private Types.StructType partitionType;

  private String filePath = null;
  private FileFormat format = null;
  private PartitionData partitionData = null;
  private Long recordCount = null;
  private long fileSizeInBytes = -1L;
  private long blockSizeInBytes = -1L;

  // optional fields
  private Integer fileOrdinal = null; // boxed for nullability
  private List<Integer> sortColumns = null;
  private Map<Integer, Long> columnSizes = null;
  private Map<Integer, Long> valueCounts = null;
  private Map<Integer, Long> nullValueCounts = null;
  private Map<Integer, ByteBuffer> lowerBounds = null;
  private Map<Integer, ByteBuffer> upperBounds = null;
  private ByteBuffer keyMetadata = null;

  // cached schema
  private transient org.apache.avro.Schema avroSchema = null;

  /**
   * Used by Avro reflection to instantiate this class when reading manifest files.
   */
  public GenericDataFile(org.apache.avro.Schema avroSchema) {
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

  GenericDataFile(String filePath, FileFormat format, long recordCount,
                  long fileSizeInBytes, long blockSizeInBytes) {
    this.filePath = filePath;
    this.format = format;
    this.partitionData = EMPTY_PARTITION_DATA;
    this.partitionType = EMPTY_PARTITION_DATA.getPartitionType();
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSizeInBytes;
    this.blockSizeInBytes = blockSizeInBytes;
    this.fileOrdinal = null;
    this.sortColumns = null;
    this.columnSizes = null;
    this.valueCounts = null;
    this.nullValueCounts = null;
    this.lowerBounds = null;
    this.upperBounds = null;
    this.fromProjectionPos = null;
  }

  GenericDataFile(String filePath, FileFormat format, PartitionData partition,
                  long recordCount, long fileSizeInBytes, long blockSizeInBytes) {
    this.filePath = filePath;
    this.format = format;
    this.partitionData = partition;
    this.partitionType = partition.getPartitionType();
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSizeInBytes;
    this.blockSizeInBytes = blockSizeInBytes;
    this.fileOrdinal = null;
    this.sortColumns = null;
    this.columnSizes = null;
    this.valueCounts = null;
    this.nullValueCounts = null;
    this.lowerBounds = null;
    this.upperBounds = null;
    this.fromProjectionPos = null;
  }

  GenericDataFile(String filePath, FileFormat format, PartitionData partition,
                  long fileSizeInBytes, long blockSizeInBytes, Metrics metrics) {
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
    this.recordCount = metrics.recordCount();
    this.fileSizeInBytes = fileSizeInBytes;
    this.blockSizeInBytes = blockSizeInBytes;
    this.fileOrdinal = null;
    this.sortColumns = null;
    this.columnSizes = metrics.columnSizes();
    this.valueCounts = metrics.valueCounts();
    this.nullValueCounts = metrics.nullValueCounts();
    this.lowerBounds = SerializableByteBufferMap.wrap(metrics.lowerBounds());
    this.upperBounds = SerializableByteBufferMap.wrap(metrics.upperBounds());
    this.fromProjectionPos = null;
  }

  GenericDataFile(String filePath, FileFormat format, PartitionData partition,
                  long fileSizeInBytes, long blockSizeInBytes, Metrics metrics,
                  ByteBuffer keyMetadata) {
    this(filePath, format, partition, fileSizeInBytes, blockSizeInBytes, metrics);
    this.keyMetadata = keyMetadata;
  }

  /**
   * Copy constructor.
   *
   * @param toCopy a generic data file to copy.
   */
  private GenericDataFile(GenericDataFile toCopy) {
    this.filePath = toCopy.filePath;
    this.format = toCopy.format;
    this.partitionData = toCopy.partitionData.copy();
    this.partitionType = toCopy.partitionType;
    this.recordCount = toCopy.recordCount;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
    this.blockSizeInBytes = toCopy.blockSizeInBytes;
    this.fileOrdinal = toCopy.fileOrdinal;
    this.sortColumns = copy(toCopy.sortColumns);
    // TODO: support lazy conversion to/from map
    this.columnSizes = copy(toCopy.columnSizes);
    this.valueCounts = copy(toCopy.valueCounts);
    this.nullValueCounts = copy(toCopy.nullValueCounts);
    this.lowerBounds = SerializableByteBufferMap.wrap(copy(toCopy.lowerBounds));
    this.upperBounds = SerializableByteBufferMap.wrap(copy(toCopy.upperBounds));
    this.fromProjectionPos = toCopy.fromProjectionPos;
    this.keyMetadata = toCopy.keyMetadata == null ? null : ByteBuffers.copy(toCopy.keyMetadata);
  }

  /**
   * Constructor for Java serialization.
   */
  GenericDataFile() {
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
  public long blockSizeInBytes() {
    return blockSizeInBytes;
  }

  @Override
  public Integer fileOrdinal() {
    return fileOrdinal;
  }

  @Override
  public List<Integer> sortColumns() {
    return sortColumns;
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
    return keyMetadata;
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    if (avroSchema == null) {
      this.avroSchema = getAvroSchema(partitionType);
    }
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    int pos = i;
    // if the schema was projected, map the incoming ordinal to the expected one
    if (fromProjectionPos != null) {
      pos = fromProjectionPos[i];
    }
    switch (pos) {
      case 0:
        // always coerce to String for Serializable
        this.filePath = v.toString();
        return;
      case 1:
        this.format = FileFormat.valueOf(v.toString());
        return;
      case 2:
        this.partitionData = (PartitionData) v;
        return;
      case 3:
        this.recordCount = (Long) v;
        return;
      case 4:
        this.fileSizeInBytes = (Long) v;
        return;
      case 5:
        this.blockSizeInBytes = (Long) v;
        return;
      case 6:
        this.fileOrdinal = (Integer) v;
        return;
      case 7:
        this.sortColumns = (List<Integer>) v;
        return;
      case 8:
        this.columnSizes = (Map<Integer, Long>) v;
        return;
      case 9:
        this.valueCounts = (Map<Integer, Long>) v;
        return;
      case 10:
        this.nullValueCounts = (Map<Integer, Long>) v;
        return;
      case 11:
        this.lowerBounds = SerializableByteBufferMap.wrap((Map<Integer, ByteBuffer>) v);
        return;
      case 12:
        this.upperBounds = SerializableByteBufferMap.wrap((Map<Integer, ByteBuffer>) v);
        return;
      case 13:
        this.keyMetadata = (ByteBuffer) v;
      default:
        // ignore the object, it must be from a newer version of the format
    }
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
        return filePath;
      case 1:
        return format != null ? format.toString() : null;
      case 2:
        return partitionData;
      case 3:
        return recordCount;
      case 4:
        return fileSizeInBytes;
      case 5:
        return blockSizeInBytes;
      case 6:
        return fileOrdinal;
      case 7:
        return sortColumns;
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
        return keyMetadata;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  private static org.apache.avro.Schema getAvroSchema(Types.StructType partitionType) {
    Types.StructType type = DataFile.getType(partitionType);
    return AvroSchemaUtil.convert(type, ImmutableMap.of(
        type, GenericDataFile.class.getName(),
        partitionType, PartitionData.class.getName()));
  }

  @Override
  public int size() {
    return 14;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public <T> void set(int pos, T value) {
    put(pos, value);
  }

  @Override
  public DataFile copy() {
    return new GenericDataFile(this);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("file_path", filePath)
        .add("file_format", format)
        .add("partition", partitionData)
        .add("record_count", recordCount)
        .add("file_size_in_bytes", fileSizeInBytes)
        .add("block_size_in_bytes", blockSizeInBytes)
        .add("column_sizes", columnSizes)
        .add("value_counts", valueCounts)
        .add("null_value_counts", nullValueCounts)
        .add("lower_bounds", lowerBounds)
        .add("upper_bounds", upperBounds)
        .add("key_metadata", keyMetadata == null ? "null" : "(redacted)")
        .toString();
  }

  private static <K, V> Map<K, V> copy(Map<K, V> map) {
    if (map != null) {
      return ImmutableMap.copyOf(map);
    }
    return null;
  }

  private static <E> List<E> copy(List<E> list) {
    if (list != null) {
      return ImmutableList.copyOf(list);
    }
    return null;
  }
}
