package org.apache.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.ByteBuffers;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;

public class DeltaFiles {

  public static DeltaFiles.Builder builder(PrimaryKeySpec pkSpec) {
    return new DeltaFiles.Builder(pkSpec);
  }

  public static class Builder {
    private final PrimaryKeySpec pkSpec;
    private String filePath = null;
    private FileFormat format = null;
    private long rowCount = -1L;
    private long deleteCount = -1L;
    private long fileSizeInBytes = -1L;

    // optional fields
    private ByteBuffer keyMetadata = null;
    private List<Long> splitOffsets = null;

    public Builder(PrimaryKeySpec pkSpec) {
      this.pkSpec = pkSpec;
    }

    public void clear() {
      this.filePath = null;
      this.format = null;
      this.rowCount = -1L;
      this.deleteCount = -1L;
      this.fileSizeInBytes = -1L;
      this.splitOffsets = null;
    }

    public DeltaFiles.Builder copy(DeltaFile toCopy) {
      this.filePath = toCopy.path().toString();
      this.format = toCopy.format();
      this.rowCount = toCopy.rowCount();
      this.deleteCount = toCopy.deleteCount();
      this.fileSizeInBytes = toCopy.fileSizeInBytes();
      this.keyMetadata = toCopy.keyMetadata() == null ? null
              : ByteBuffers.copy(toCopy.keyMetadata());
      this.splitOffsets = toCopy.splitOffsets() == null ? null : copyList(toCopy.splitOffsets());
      return this;
    }

    public DeltaFiles.Builder withStatus(FileStatus stat) {
      this.filePath = stat.getPath().toString();
      this.fileSizeInBytes = stat.getLen();
      return this;
    }

    public DeltaFiles.Builder withInputFile(InputFile file) {
      if (file instanceof HadoopInputFile) {
        return withStatus(((HadoopInputFile) file).getStat());
      }

      this.filePath = file.location();
      this.fileSizeInBytes = file.getLength();
      return this;
    }

    public DeltaFiles.Builder withPath(String newFilePath) {
      this.filePath = newFilePath;
      return this;
    }

    public DeltaFiles.Builder withFormat(String newFormat) {
      this.format = FileFormat.valueOf(newFormat.toUpperCase(Locale.ENGLISH));
      return this;
    }

    public DeltaFiles.Builder withFormat(FileFormat newFormat) {
      this.format = newFormat;
      return this;
    }

    public DeltaFiles.Builder withRowCount(long newRowCount) {
      this.rowCount = newRowCount;
      return this;
    }

    public DeltaFiles.Builder withDeleteCount(long newDeleteCount) {
      this.rowCount = newDeleteCount;
      return this;
    }

    public DeltaFiles.Builder withFileSizeInBytes(long newFileSizeInBytes) {
      this.fileSizeInBytes = newFileSizeInBytes;
      return this;
    }


    public DeltaFiles.Builder withSplitOffsets(List<Long> offsets) {
      if (offsets != null) {
        this.splitOffsets = copyList(offsets);
      } else {
        this.splitOffsets = null;
      }
      return this;
    }

    public DeltaFile build() {
      Preconditions.checkArgument(filePath != null, "File path is required");
      if (format == null) {
        this.format = FileFormat.fromFileName(filePath);
      }
      Preconditions.checkArgument(format != null, "File format is required");
      Preconditions.checkArgument(fileSizeInBytes >= 0, "File size is required");
      Preconditions.checkArgument(rowCount >= 0, "Record count is required");

      return new GenericDeltaFile(filePath, format, rowCount, deleteCount, fileSizeInBytes,
              keyMetadata, splitOffsets);
    }
  }

  private static <E> List<E> copyList(List<E> toCopy) {
    List<E> copy = Lists.newArrayListWithExpectedSize(toCopy.size());
    copy.addAll(toCopy);
    return copy;
  }
}
