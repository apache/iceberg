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
class TableChange {
  private int dataFileNum;
  private int deleteFileNum;
  private long dataFileSize;
  private long deleteFileSize;
  private int commitNum;

  TableChange(
      int dataFileNum, int deleteFileNum, long dataFileSize, long deleteFileSize, int commitNum) {
    this.dataFileNum = dataFileNum;
    this.deleteFileNum = deleteFileNum;
    this.dataFileSize = dataFileSize;
    this.deleteFileSize = deleteFileSize;
    this.commitNum = commitNum;
  }

  TableChange(Snapshot snapshot, FileIO io) {
    Iterable<DataFile> dataFiles = snapshot.addedDataFiles(io);
    Iterable<DeleteFile> deleteFiles = snapshot.addedDeleteFiles(io);

    dataFiles.forEach(
        dataFile -> {
          this.dataFileNum++;
          this.dataFileSize += dataFile.fileSizeInBytes();
        });

    deleteFiles.forEach(
        deleteFile -> {
          this.deleteFileNum++;
          this.deleteFileSize += deleteFile.fileSizeInBytes();
        });

    this.commitNum = 1;
  }

  static TableChange empty() {
    return new TableChange(0, 0, 0L, 0L, 0);
  }

  int dataFileNum() {
    return dataFileNum;
  }

  int deleteFileNum() {
    return deleteFileNum;
  }

  long dataFileSize() {
    return dataFileSize;
  }

  long deleteFileSize() {
    return deleteFileSize;
  }

  public int commitNum() {
    return commitNum;
  }

  public void merge(TableChange other) {
    this.dataFileNum += other.dataFileNum;
    this.deleteFileNum += other.deleteFileNum;
    this.dataFileSize += other.dataFileSize;
    this.deleteFileSize += other.deleteFileSize;
    this.commitNum += other.commitNum;
  }

  TableChange copy() {
    return new TableChange(dataFileNum, deleteFileNum, dataFileSize, deleteFileSize, commitNum);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("dataFileNum", dataFileNum)
        .add("deleteFileNum", deleteFileNum)
        .add("dataFileSize", dataFileSize)
        .add("deleteFileSize", deleteFileSize)
        .add("commitNum", commitNum)
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
    return dataFileNum == that.dataFileNum
        && deleteFileNum == that.deleteFileNum
        && dataFileSize == that.dataFileSize
        && deleteFileSize == that.deleteFileSize
        && commitNum == that.commitNum;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataFileNum, deleteFileNum, dataFileSize, deleteFileSize, commitNum);
  }
}
