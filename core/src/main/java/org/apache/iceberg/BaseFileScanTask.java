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

import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class BaseFileScanTask extends BaseContentScanTask<FileScanTask, DataFile>
    implements FileScanTask {
  private final DeleteFile[] deletes;
  private transient volatile List<DeleteFile> deleteList = null;
  private transient volatile long deletesSizeBytes = 0L;

  public BaseFileScanTask(
      DataFile file,
      DeleteFile[] deletes,
      String schemaString,
      String specString,
      ResidualEvaluator residuals) {
    super(file, schemaString, specString, residuals);
    this.deletes = deletes != null ? deletes : new DeleteFile[0];
  }

  @Override
  protected FileScanTask self() {
    return this;
  }

  @Override
  protected FileScanTask newSplitTask(FileScanTask parentTask, long offset, long length) {
    return new SplitScanTask(offset, length, parentTask, deletesSizeBytes());
  }

  @Override
  public List<DeleteFile> deletes() {
    if (deleteList == null) {
      this.deleteList = ImmutableList.copyOf(deletes);
    }

    return deleteList;
  }

  @Override
  public long sizeBytes() {
    return length() + deletesSizeBytes();
  }

  @Override
  public int filesCount() {
    return 1 + deletes.length;
  }

  @Override
  public Schema schema() {
    return super.schema();
  }

  // lazily cache the size of deletes to reuse in all split tasks
  private long deletesSizeBytes() {
    if (deletesSizeBytes == 0L && deletes.length > 0) {
      long size = 0L;
      for (DeleteFile deleteFile : deletes) {
        size += deleteFile.fileSizeInBytes();
      }
      this.deletesSizeBytes = size;
    }

    return deletesSizeBytes;
  }

  @VisibleForTesting
  static final class SplitScanTask implements FileScanTask, MergeableScanTask<SplitScanTask> {
    private final long len;
    private final long offset;
    private final FileScanTask fileScanTask;
    private transient volatile long deletesSizeBytes = 0L;

    SplitScanTask(long offset, long len, FileScanTask fileScanTask) {
      this.offset = offset;
      this.len = len;
      this.fileScanTask = fileScanTask;
    }

    SplitScanTask(long offset, long len, FileScanTask fileScanTask, long deletesSizeBytes) {
      this.offset = offset;
      this.len = len;
      this.fileScanTask = fileScanTask;
      this.deletesSizeBytes = deletesSizeBytes;
    }

    @Override
    public DataFile file() {
      return fileScanTask.file();
    }

    @Override
    public List<DeleteFile> deletes() {
      return fileScanTask.deletes();
    }

    @Override
    public Schema schema() {
      return fileScanTask.schema();
    }

    @Override
    public PartitionSpec spec() {
      return fileScanTask.spec();
    }

    @Override
    public long start() {
      return offset;
    }

    @Override
    public long length() {
      return len;
    }

    @Override
    public long estimatedRowsCount() {
      return BaseContentScanTask.estimateRowsCount(len, fileScanTask.file());
    }

    @Override
    public long sizeBytes() {
      return len + deletesSizeBytes();
    }

    @Override
    public int filesCount() {
      return fileScanTask.filesCount();
    }

    @Override
    public Expression residual() {
      return fileScanTask.residual();
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      throw new UnsupportedOperationException("Cannot split a task which is already split");
    }

    @Override
    public boolean canMerge(ScanTask other) {
      if (other instanceof SplitScanTask) {
        SplitScanTask that = (SplitScanTask) other;
        return file().equals(that.file()) && offset + len == that.start();
      } else {
        return false;
      }
    }

    @Override
    public SplitScanTask merge(ScanTask other) {
      SplitScanTask that = (SplitScanTask) other;
      return new SplitScanTask(offset, len + that.length(), fileScanTask, deletesSizeBytes);
    }

    private long deletesSizeBytes() {
      if (deletesSizeBytes == 0L && fileScanTask.filesCount() > 1) {
        long size = 0L;
        for (DeleteFile deleteFile : fileScanTask.deletes()) {
          size += deleteFile.fileSizeInBytes();
        }
        this.deletesSizeBytes = size;
      }

      return deletesSizeBytes;
    }
  }
}
