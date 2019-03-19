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

import com.google.common.base.Preconditions;
import com.netflix.iceberg.encryption.EncryptedInputFile;
import com.netflix.iceberg.encryption.EncryptedOutputFile;
import com.netflix.iceberg.encryption.EncryptionKeyMetadata;
import com.netflix.iceberg.hadoop.HadoopInputFile;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.types.Conversions;
import com.netflix.iceberg.util.ByteBuffers;
import org.apache.hadoop.fs.FileStatus;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class DataFiles {

  private static final long DEFAULT_BLOCK_SIZE = 64*1024*1024;

  private static PartitionData newPartitionData(PartitionSpec spec) {
    return new PartitionData(spec.partitionType());
  }

  private static PartitionData copyPartitionData(PartitionSpec spec, StructLike partitionData, PartitionData reuse) {
    PartitionData data = reuse;
    if (data == null) {
      data = newPartitionData(spec);
    }

    Class<?>[] javaClasses = spec.javaClasses();
    List<PartitionField> fields = spec.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      data.set(i, partitionData.get(i, javaClasses[i]));
    }

    return data;
  }

  private static PartitionData fillFromPath(PartitionSpec spec, String partitionPath, PartitionData reuse) {
    PartitionData data = reuse;
    if (data == null) {
      data = newPartitionData(spec);
    }

    String[] partitions = partitionPath.split("/", -1);
    Preconditions.checkArgument(partitions.length <= spec.fields().size(),
        "Invalid partition data, too many fields (expecting %s): %s",
        spec.fields().size(), partitionPath);
    Preconditions.checkArgument(partitions.length >= spec.fields().size(),
        "Invalid partition data, not enough fields (expecting %s): %s",
        spec.fields().size(), partitionPath);

    for (int i = 0; i < partitions.length; i += 1) {
      PartitionField field = spec.fields().get(i);
      String[] parts = partitions[i].split("=", 2);
      Preconditions.checkArgument(
          parts.length == 2 &&
              parts[0] != null &&
              field.name().equals(parts[0]),
          "Invalid partition: " + partitions[i]);

      data.set(i, Conversions.fromPartitionString(data.getType(i), parts[1]));
    }

    return data;
  }

  public static PartitionData data(PartitionSpec spec, String partitionPath) {
    return fillFromPath(spec, partitionPath, null);
  }

  public static PartitionData copy(PartitionSpec spec, StructLike partition) {
    return copyPartitionData(spec, partition, null);
  }

  public static DataFile fromInputFile(InputFile file, long rowCount) {
    if (file instanceof HadoopInputFile) {
      return fromStat(((HadoopInputFile) file).getStat(), rowCount);
    }

    String location = file.location();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(location, format, rowCount, file.getLength(), DEFAULT_BLOCK_SIZE);
  }

  public static DataFile fromStat(FileStatus stat, long rowCount) {
    String location = stat.getPath().toString();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(location, format, rowCount, stat.getLen(), stat.getBlockSize());
  }

  public static DataFile fromInputFile(InputFile file, PartitionData partition, long rowCount) {
    if (file instanceof HadoopInputFile) {
      return fromStat(((HadoopInputFile) file).getStat(), partition, rowCount);
    }

    String location = file.location();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(
        location, format, partition, rowCount, file.getLength(), DEFAULT_BLOCK_SIZE);
  }

  public static DataFile fromStat(FileStatus stat, PartitionData partition, long rowCount) {
    String location = stat.getPath().toString();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(
        location, format, partition, rowCount, stat.getLen(), stat.getBlockSize());
  }

  public static DataFile fromInputFile(InputFile file, PartitionData partition, Metrics metrics) {
    if (file instanceof HadoopInputFile) {
      return fromStat(((HadoopInputFile) file).getStat(), partition, metrics);
    }

    String location = file.location();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(
        location, format, partition, file.getLength(), DEFAULT_BLOCK_SIZE, metrics);
  }

  public static DataFile fromEncryptedOutputFile(EncryptedOutputFile encryptedFile, PartitionData partition,
                                                Metrics metrics) {
    EncryptionKeyMetadata keyMetadata = encryptedFile.keyMetadata();
    InputFile file = encryptedFile.encryptingOutputFile().toInputFile();
    if (encryptedFile instanceof HadoopInputFile) {
      return fromStat(((HadoopInputFile) file).getStat(), partition, metrics, keyMetadata);
    }

    String location = file.location();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(
        location, format, partition, file.getLength(), DEFAULT_BLOCK_SIZE, metrics, keyMetadata.buffer());
  }

  public static DataFile fromStat(FileStatus stat, PartitionData partition, Metrics metrics) {
    String location = stat.getPath().toString();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(
        location, format, partition, stat.getLen(), stat.getBlockSize(), metrics);
  }

  public static DataFile fromStat(FileStatus stat, PartitionData partition, Metrics metrics,
                                  EncryptionKeyMetadata keyMetadata) {
    String location = stat.getPath().toString();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(
        location, format, partition, stat.getLen(), stat.getBlockSize(), metrics, keyMetadata.buffer());
  }

  public static DataFile fromParquetInputFile(InputFile file,
                                              PartitionData partition,
                                              Metrics metrics) {
    if (file instanceof HadoopInputFile) {
      return fromParquetStat(((HadoopInputFile) file).getStat(), partition, metrics);
    }

    String location = file.location();
    FileFormat format = FileFormat.PARQUET;
    return new GenericDataFile(
        location, format, partition, file.getLength(), DEFAULT_BLOCK_SIZE, metrics);
  }

  public static DataFile fromParquetStat(FileStatus stat, PartitionData partition, Metrics metrics) {
    String location = stat.getPath().toString();
    FileFormat format = FileFormat.PARQUET;
    return new GenericDataFile(
        location, format, partition, stat.getLen(), stat.getBlockSize(), metrics);
  }

  public static Builder builder(PartitionSpec spec) {
    return new Builder(spec);
  }

  static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final PartitionSpec spec;
    private final boolean isPartitioned;
    private PartitionData partitionData;
    private String filePath = null;
    private FileFormat format = null;
    private long recordCount = -1L;
    private long fileSizeInBytes = -1L;
    private long blockSizeInBytes = -1L;

    // optional fields
    private Map<Integer, Long> columnSizes = null;
    private Map<Integer, Long> valueCounts = null;
    private Map<Integer, Long> nullValueCounts = null;
    private Map<Integer, ByteBuffer> lowerBounds = null;
    private Map<Integer, ByteBuffer> upperBounds = null;
    private ByteBuffer keyMetadata = null;

    public Builder() {
      this.spec = null;
      this.partitionData = null;
      this.isPartitioned = false;
    }

    public Builder(PartitionSpec spec) {
      this.spec = spec;
      this.isPartitioned = spec.fields().size() > 0;
      this.partitionData = isPartitioned ? newPartitionData(spec) : null;
    }

    public void clear() {
      if (isPartitioned) {
        partitionData.clear();
      }
      this.filePath = null;
      this.format = null;
      this.recordCount = -1L;
      this.fileSizeInBytes = -1L;
      this.blockSizeInBytes = -1L;
      this.columnSizes = null;
      this.valueCounts = null;
      this.nullValueCounts = null;
      this.lowerBounds = null;
      this.upperBounds = null;
    }

    public Builder copy(DataFile toCopy) {
      if (isPartitioned) {
        this.partitionData = copyPartitionData(spec, toCopy.partition(), partitionData);
      }
      this.filePath = toCopy.path().toString();
      this.format = toCopy.format();
      this.recordCount = toCopy.recordCount();
      this.fileSizeInBytes = toCopy.fileSizeInBytes();
      this.blockSizeInBytes = toCopy.blockSizeInBytes();
      this.columnSizes = toCopy.columnSizes();
      this.valueCounts = toCopy.valueCounts();
      this.nullValueCounts = toCopy.nullValueCounts();
      this.lowerBounds = toCopy.lowerBounds();
      this.upperBounds = toCopy.upperBounds();
      this.keyMetadata = toCopy.keyMetadata() == null ? null
          : ByteBuffers.copy(toCopy.keyMetadata());
      return this;
    }

    public Builder withStatus(FileStatus stat) {
      this.filePath = stat.getPath().toString();
      this.fileSizeInBytes = stat.getLen();
      this.blockSizeInBytes = stat.getBlockSize();
      return this;
    }

    public Builder withInputFile(InputFile file) {
      if (file instanceof HadoopInputFile) {
        return withStatus(((HadoopInputFile) file).getStat());
      }

      this.filePath = file.location();
      this.fileSizeInBytes = file.getLength();
      return this;
    }

    public Builder withEncryptedOutputFile(EncryptedOutputFile encryptedFile) {
      withInputFile(encryptedFile.encryptingOutputFile().toInputFile());
      withEncryptionKeyMetadata(encryptedFile.keyMetadata());
      return this;
    }

    public Builder withPath(String filePath) {
      this.filePath = filePath;
      return this;
    }

    public Builder withFormat(String format) {
      this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
      return this;
    }

    public Builder withFormat(FileFormat format) {
      this.format = format;
      return this;
    }

    public Builder withPartition(StructLike partition) {
      this.partitionData = copyPartitionData(spec, partition, partitionData);
      return this;
    }

    public Builder withRecordCount(long recordCount) {
      this.recordCount = recordCount;
      return this;
    }

    public Builder withFileSizeInBytes(long fileSizeInBytes) {
      this.fileSizeInBytes = fileSizeInBytes;
      return this;
    }

    public Builder withBlockSizeInBytes(long blockSizeInBytes) {
      this.blockSizeInBytes = blockSizeInBytes;
      return this;
    }

    public Builder withPartitionPath(String partitionPath) {
      Preconditions.checkArgument(isPartitioned || partitionPath.isEmpty(),
          "Cannot add partition data for an unpartitioned table");
      this.partitionData = fillFromPath(spec, partitionPath, partitionData);
      return this;
    }

    public Builder withMetrics(Metrics metrics) {
      // check for null to avoid NPE when unboxing
      this.recordCount = metrics.recordCount() == null ? -1 : metrics.recordCount();
      this.columnSizes = metrics.columnSizes();
      this.valueCounts = metrics.valueCounts();
      this.nullValueCounts = metrics.nullValueCounts();
      this.lowerBounds = metrics.lowerBounds();
      this.upperBounds = metrics.upperBounds();
      return this;
    }

    public Builder withEncryptionKeyMetadata(ByteBuffer keyMetadata) {
      this.keyMetadata = keyMetadata;
      return this;
    }

    public Builder withEncryptionKeyMetadata(EncryptionKeyMetadata keyMetadata) {
      return withEncryptionKeyMetadata(keyMetadata.buffer());
    }

    public Builder withEncryptionKeyMetadata(byte[] keyMetadata) {
      return withEncryptionKeyMetadata(ByteBuffer.wrap(keyMetadata));
    }

    public DataFile build() {
      Preconditions.checkArgument(filePath != null, "File path is required");
      if (format == null) {
        this.format = FileFormat.fromFileName(filePath);
      }
      Preconditions.checkArgument(format != null, "File format is required");
      Preconditions.checkArgument(fileSizeInBytes >= 0, "File size is required");
      Preconditions.checkArgument(recordCount >= 0, "Record count is required");

      if (blockSizeInBytes < 0) {
        this.blockSizeInBytes = DEFAULT_BLOCK_SIZE; // assume 64MB blocks
      }

      return new GenericDataFile(
          filePath, format, isPartitioned ? partitionData.copy() : null,
          fileSizeInBytes, blockSizeInBytes, new Metrics(
              recordCount, columnSizes, valueCounts, nullValueCounts, lowerBounds, upperBounds), keyMetadata);
    }
  }
}
