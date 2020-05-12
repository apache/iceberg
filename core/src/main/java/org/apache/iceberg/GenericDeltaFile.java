package org.apache.iceberg;

import com.google.common.collect.Lists;
import org.apache.iceberg.util.ByteBuffers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GenericDeltaFile implements DeltaFile {

  private String filePath = null;
  private FileFormat format = null;
  private Long rowCount = null;
  private long fileSizeInBytes = -1L;

  // optional fields
  private byte[] keyMetadata = null;
  private List<Long> splitOffsets = null;

  public GenericDeltaFile(String filePath, FileFormat format, Long rowCount, long fileSizeInBytes,
                          List<Long> splitOffsets) {
    this.filePath = filePath;
    this.format = format;
    this.rowCount = rowCount;
    this.fileSizeInBytes = fileSizeInBytes;
    this.splitOffsets = splitOffsets;
  }

  public GenericDeltaFile(String filePath, FileFormat format, Long rowCount, long fileSizeInBytes,
                          ByteBuffer keyMetadata, List<Long> splitOffsets) {
    this(filePath, format, rowCount, fileSizeInBytes, splitOffsets);
    this.keyMetadata = ByteBuffers.toByteArray(keyMetadata);
  }

  private GenericDeltaFile(GenericDeltaFile toCopy) {
    this.filePath = toCopy.filePath;
    this.format = toCopy.format;
    this.rowCount = toCopy.rowCount;
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
  public StructLike primaryKey() {
    return null;
  }

  @Override
  public long rowCount() {
    return rowCount;
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
}
