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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/** Event describing changes in an Iceberg table */
@Internal
public class TableChange {
  private int dataFileCount;
  private long dataFileSizeInBytes;
  private int posDeleteFileCount;
  private long posDeleteRecordCount;
  private int eqDeleteFileCount;
  private long eqDeleteRecordCount;
  private int commitCount;

  TableChange(
      int dataFileCount,
      long dataFileSizeInBytes,
      int posDeleteFileCount,
      long posDeleteRecordCount,
      int eqDeleteFileCount,
      long eqDeleteRecordCount,
      int commitCount) {
    this.dataFileCount = dataFileCount;
    this.dataFileSizeInBytes = dataFileSizeInBytes;
    this.posDeleteFileCount = posDeleteFileCount;
    this.posDeleteRecordCount = posDeleteRecordCount;
    this.eqDeleteFileCount = eqDeleteFileCount;
    this.eqDeleteRecordCount = eqDeleteRecordCount;
    this.commitCount = commitCount;
  }

  TableChange(Snapshot snapshot, FileIO io) {
    Iterable<DataFile> dataFiles = snapshot.addedDataFiles(io);
    Iterable<DeleteFile> deleteFiles = snapshot.addedDeleteFiles(io);

    dataFiles.forEach(
        dataFile -> {
          this.dataFileCount++;
          this.dataFileSizeInBytes += dataFile.fileSizeInBytes();
        });

    deleteFiles.forEach(
        deleteFile -> {
          switch (deleteFile.content()) {
            case POSITION_DELETES:
              this.posDeleteFileCount++;
              this.posDeleteRecordCount += deleteFile.recordCount();
              break;
            case EQUALITY_DELETES:
              this.eqDeleteFileCount++;
              this.eqDeleteRecordCount += deleteFile.recordCount();
              break;
            default:
              throw new IllegalArgumentException("Unexpected delete file content: " + deleteFile);
          }
        });

    this.commitCount = 1;
  }

  static TableChange empty() {
    return new TableChange(0, 0L, 0, 0L, 0, 0L, 0);
  }

  public static Builder builder() {
    return new Builder();
  }

  int dataFileCount() {
    return dataFileCount;
  }

  long dataFileSizeInBytes() {
    return dataFileSizeInBytes;
  }

  int posDeleteFileCount() {
    return posDeleteFileCount;
  }

  long posDeleteRecordCount() {
    return posDeleteRecordCount;
  }

  int eqDeleteFileCount() {
    return eqDeleteFileCount;
  }

  long eqDeleteRecordCount() {
    return eqDeleteRecordCount;
  }

  int commitCount() {
    return commitCount;
  }

  public void merge(TableChange other) {
    this.dataFileCount += other.dataFileCount;
    this.dataFileSizeInBytes += other.dataFileSizeInBytes;
    this.posDeleteFileCount += other.posDeleteFileCount;
    this.posDeleteRecordCount += other.posDeleteRecordCount;
    this.eqDeleteFileCount += other.eqDeleteFileCount;
    this.eqDeleteRecordCount += other.eqDeleteRecordCount;
    this.commitCount += other.commitCount;
  }

  TableChange copy() {
    return new TableChange(
        dataFileCount,
        dataFileSizeInBytes,
        posDeleteFileCount,
        posDeleteRecordCount,
        eqDeleteFileCount,
        eqDeleteRecordCount,
        commitCount);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("dataFileCount", dataFileCount)
        .add("dataFileSizeInBytes", dataFileSizeInBytes)
        .add("posDeleteFileCount", posDeleteFileCount)
        .add("posDeleteRecordCount", posDeleteRecordCount)
        .add("eqDeleteFileCount", eqDeleteFileCount)
        .add("eqDeleteRecordCount", eqDeleteRecordCount)
        .add("commitCount", commitCount)
        .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    TableChange that = (TableChange) other;
    return dataFileCount == that.dataFileCount
        && dataFileSizeInBytes == that.dataFileSizeInBytes
        && posDeleteFileCount == that.posDeleteFileCount
        && posDeleteRecordCount == that.posDeleteRecordCount
        && eqDeleteFileCount == that.eqDeleteFileCount
        && eqDeleteRecordCount == that.eqDeleteRecordCount
        && commitCount == that.commitCount;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        dataFileCount,
        dataFileSizeInBytes,
        posDeleteFileCount,
        posDeleteRecordCount,
        eqDeleteFileCount,
        eqDeleteRecordCount,
        commitCount);
  }

  public static class Builder {
    private int dataFileCount = 0;
    private long dataFileSizeInBytes = 0L;
    private int posDeleteFileCount = 0;
    private long posDeleteRecordCount = 0L;
    private int eqDeleteFileCount = 0;
    private long eqDeleteRecordCount = 0L;
    private int commitCount = 0;

    private Builder() {}

    public Builder dataFileCount(int newDataFileCount) {
      this.dataFileCount = newDataFileCount;
      return this;
    }

    public Builder dataFileSizeInBytes(long newDataFileSizeInBytes) {
      this.dataFileSizeInBytes = newDataFileSizeInBytes;
      return this;
    }

    public Builder posDeleteFileCount(int newPosDeleteFileCount) {
      this.posDeleteFileCount = newPosDeleteFileCount;
      return this;
    }

    public Builder posDeleteRecordCount(long newPosDeleteRecordCount) {
      this.posDeleteRecordCount = newPosDeleteRecordCount;
      return this;
    }

    public Builder eqDeleteFileCount(int newEqDeleteFileCount) {
      this.eqDeleteFileCount = newEqDeleteFileCount;
      return this;
    }

    public Builder eqDeleteRecordCount(long newEqDeleteRecordCount) {
      this.eqDeleteRecordCount = newEqDeleteRecordCount;
      return this;
    }

    public Builder commitCount(int newCommitCount) {
      this.commitCount = newCommitCount;
      return this;
    }

    public TableChange build() {
      return new TableChange(
          dataFileCount,
          dataFileSizeInBytes,
          posDeleteFileCount,
          posDeleteRecordCount,
          eqDeleteFileCount,
          eqDeleteRecordCount,
          commitCount);
    }
  }
}
