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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.util.ByteBuffers;

public class DataFiles {

  private DataFiles() {}

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
          "Invalid partition: %s",
          partitions[i]);

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

  public static DataFile fromStat(FileStatus stat, long rowCount) {
    String location = stat.getPath().toString();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(location, format, rowCount, stat.getLen());
  }

  public static DataFile fromStat(FileStatus stat, PartitionData partition, long rowCount) {
    String location = stat.getPath().toString();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(
        location, format, partition, rowCount, stat.getLen());
  }

  public static DataFile fromStat(FileStatus stat, PartitionData partition, Metrics metrics,
      EncryptionKeyMetadata keyMetadata, List<Long> splitOffsets) {
    String location = stat.getPath().toString();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(
        location, format, partition, stat.getLen(), metrics, keyMetadata.buffer(), splitOffsets);
  }

  public static DataFile fromInputFile(InputFile file, PartitionData partition, long rowCount) {
    if (file instanceof HadoopInputFile) {
      return fromStat(((HadoopInputFile) file).getStat(), partition, rowCount);
    }

    String location = file.location();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(
        location, format, partition, rowCount, file.getLength());
  }

  public static DataFile fromInputFile(InputFile file, long rowCount) {
    if (file instanceof HadoopInputFile) {
      return fromStat(((HadoopInputFile) file).getStat(), rowCount);
    }

    String location = file.location();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(location, format, rowCount, file.getLength());
  }

  public static DataFile fromEncryptedOutputFile(EncryptedOutputFile encryptedFile, PartitionData partition,
                                                Metrics metrics, List<Long> splitOffsets) {
    EncryptionKeyMetadata keyMetadata = encryptedFile.keyMetadata();
    InputFile file = encryptedFile.encryptingOutputFile().toInputFile();
    if (encryptedFile instanceof HadoopInputFile) {
      return fromStat(((HadoopInputFile) file).getStat(), partition, metrics, keyMetadata, splitOffsets);
    }

    String location = file.location();
    FileFormat format = FileFormat.fromFileName(location);
    return new GenericDataFile(
        location, format, partition, file.getLength(), metrics, keyMetadata.buffer(), splitOffsets);
  }

  public static DataFile fromManifest(ManifestFile manifest) {
    Preconditions.checkArgument(
        manifest.addedFilesCount() != null && manifest.existingFilesCount() != null,
        "Cannot create data file from manifest: data file counts are missing.");

    return new GenericDataFile(manifest.path(),
        FileFormat.AVRO,
        manifest.addedFilesCount() + manifest.existingFilesCount(),
        manifest.length());
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

    // optional fields
    private Map<Integer, Long> columnSizes = null;
    private Map<Integer, Long> valueCounts = null;
    private Map<Integer, Long> nullValueCounts = null;
    private Map<Integer, ByteBuffer> lowerBounds = null;
    private Map<Integer, ByteBuffer> upperBounds = null;
    private ByteBuffer keyMetadata = null;
    private List<Long> splitOffsets = null;

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
      this.columnSizes = null;
      this.valueCounts = null;
      this.nullValueCounts = null;
      this.lowerBounds = null;
      this.upperBounds = null;
      this.splitOffsets = null;
    }

    public Builder copy(DataFile toCopy) {
      if (isPartitioned) {
        this.partitionData = copyPartitionData(spec, toCopy.partition(), partitionData);
      }
      this.filePath = toCopy.path().toString();
      this.format = toCopy.format();
      this.recordCount = toCopy.recordCount();
      this.fileSizeInBytes = toCopy.fileSizeInBytes();
      this.columnSizes = toCopy.columnSizes();
      this.valueCounts = toCopy.valueCounts();
      this.nullValueCounts = toCopy.nullValueCounts();
      this.lowerBounds = toCopy.lowerBounds();
      this.upperBounds = toCopy.upperBounds();
      this.keyMetadata = toCopy.keyMetadata() == null ? null
          : ByteBuffers.copy(toCopy.keyMetadata());
      this.splitOffsets = toCopy.splitOffsets() == null ? null : copyList(toCopy.splitOffsets());
      return this;
    }

    public Builder withStatus(FileStatus stat) {
      this.filePath = stat.getPath().toString();
      this.fileSizeInBytes = stat.getLen();
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

    public Builder withEncryptedOutputFile(EncryptedOutputFile newEncryptedFile) {
      withInputFile(newEncryptedFile.encryptingOutputFile().toInputFile());
      withEncryptionKeyMetadata(newEncryptedFile.keyMetadata());
      return this;
    }

    public Builder withPath(String newFilePath) {
      this.filePath = newFilePath;
      return this;
    }

    public Builder withFormat(String newFormat) {
      this.format = FileFormat.valueOf(newFormat.toUpperCase(Locale.ENGLISH));
      return this;
    }

    public Builder withFormat(FileFormat newFormat) {
      this.format = newFormat;
      return this;
    }

    public Builder withPartition(StructLike newPartition) {
      this.partitionData = copyPartitionData(spec, newPartition, partitionData);
      return this;
    }

    public Builder withRecordCount(long newRecordCount) {
      this.recordCount = newRecordCount;
      return this;
    }

    public Builder withFileSizeInBytes(long newFileSizeInBytes) {
      this.fileSizeInBytes = newFileSizeInBytes;
      return this;
    }

    public Builder withPartitionPath(String newPartitionPath) {
      Preconditions.checkArgument(isPartitioned || newPartitionPath.isEmpty(),
          "Cannot add partition data for an unpartitioned table");
      if (!newPartitionPath.isEmpty()) {
        this.partitionData = fillFromPath(spec, newPartitionPath, partitionData);
      }
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

    public Builder withSplitOffsets(List<Long> offsets) {
      if (offsets != null) {
        this.splitOffsets = copyList(offsets);
      } else {
        this.splitOffsets = null;
      }
      return this;
    }

    public Builder withEncryptionKeyMetadata(ByteBuffer newKeyMetadata) {
      this.keyMetadata = newKeyMetadata;
      return this;
    }

    public Builder withEncryptionKeyMetadata(EncryptionKeyMetadata newKeyMetadata) {
      return withEncryptionKeyMetadata(newKeyMetadata.buffer());
    }

    public DataFile build() {
      Preconditions.checkArgument(filePath != null, "File path is required");
      if (format == null) {
        this.format = FileFormat.fromFileName(filePath);
      }
      Preconditions.checkArgument(format != null, "File format is required");
      Preconditions.checkArgument(fileSizeInBytes >= 0, "File size is required");
      Preconditions.checkArgument(recordCount >= 0, "Record count is required");

      return new GenericDataFile(
          filePath, format, isPartitioned ? partitionData.copy() : null,
          fileSizeInBytes, new Metrics(
              recordCount, columnSizes, valueCounts, nullValueCounts, lowerBounds, upperBounds),
          keyMetadata, splitOffsets);
    }
  }

  private static <E> List<E> copyList(List<E> toCopy) {
    List<E> copy = Lists.newArrayListWithExpectedSize(toCopy.size());
    copy.addAll(toCopy);
    return copy;
  }
}
