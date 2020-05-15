package org.apache.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GenericDeltaFile implements DeltaFile , IndexedRecord {
  private static final Types.StructType EMPTY_STRUCT_TYPE = Types.StructType.of();
  private static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_STRUCT_TYPE) {
    @Override
    public PartitionData copy() {
      return this; // this does not change
    }
  };

  private Types.StructType partitionType;
  private Types.StructType primaryKeyType;

  private String filePath = null;
  private FileFormat format = null;
  private Long rowCount = null;
  private Long deleteCount = null;
  private long fileSizeInBytes = -1L;

  // optional fields
  private byte[] keyMetadata = null;
  private List<Long> splitOffsets = null;

  // cached schema
  private transient org.apache.avro.Schema avroSchema = null;

  public GenericDeltaFile(String filePath, FileFormat format, PartitionData partition,
                          Types.StructType pkType, Long rowCount, Long deleteCount,
                          long fileSizeInBytes, List<Long> splitOffsets) {
    this.filePath = filePath;
    this.format = format;
    this.partitionType = partition.getPartitionType();
    this.primaryKeyType = pkType;
    this.rowCount = rowCount;
    this.deleteCount = deleteCount;
    this.fileSizeInBytes = fileSizeInBytes;
    this.splitOffsets = splitOffsets;
  }

  public GenericDeltaFile(String filePath, FileFormat format, Long rowCount, Long deleteCount, long fileSizeInBytes,
                          ByteBuffer keyMetadata, List<Long> splitOffsets) {
    this(filePath, format, EMPTY_PARTITION_DATA, EMPTY_STRUCT_TYPE, rowCount, deleteCount, fileSizeInBytes, splitOffsets);
    this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
  }

  private GenericDeltaFile(GenericDeltaFile toCopy) {
    this.filePath = toCopy.filePath;
    this.format = toCopy.format;
    this.rowCount = toCopy.rowCount;
    this.deleteCount = toCopy.deleteCount;
    this.fileSizeInBytes = toCopy.fileSizeInBytes;
    this.keyMetadata = toCopy.keyMetadata == null ? null : Arrays.copyOf(toCopy.keyMetadata, toCopy.keyMetadata.length);
    this.splitOffsets = copy(toCopy.splitOffsets);
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
    return null;
  }

  @Override
  public StructLike primaryKey() {
    return null;
  }

  @Override
  public long rowCount() {
    return rowCount;
  }

  @Override
  public long deleteCount() {
    return deleteCount;
  }

  @Override
  public long fileSizeInBytes() {
    return fileSizeInBytes;
  }

  @Override
  public ByteBuffer keyMetadata() {
    return keyMetadata != null ? ByteBuffer.wrap(keyMetadata) : null;
  }

  @Override
  public DeltaFile copy() {
    return new GenericDeltaFile(this);
  }

  @Override
  public DeltaFile copyWithoutStats() {
    return null;
  }

  @Override
  public List<Long> splitOffsets() {
    return splitOffsets;
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
  public void put(int i, Object v) {

  }

  @Override
  public Object get(int i) {
    int pos = i;
    switch (pos) {
      case 0:
        return filePath;
      case 1:
        return format != null ? format.toString() : null;
      case 2:
      case 3:
        return null;
      case 4:
        return fileSizeInBytes;
      case 5:
        return rowCount;
      case 6:
        return deleteCount;
      case 7:
        return keyMetadata();
      case 8:
        return splitOffsets;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  public Schema getSchema() {
    if (avroSchema == null) {
      Types.StructType type = DeltaFile.getType(partitionType, primaryKeyType);
      this.avroSchema = AvroSchemaUtil.convert(type, ImmutableMap.of(
              type, GenericDataFile.class.getName(),
              partitionType, PartitionData.class.getName()));
    }
    return avroSchema;
  }
}
