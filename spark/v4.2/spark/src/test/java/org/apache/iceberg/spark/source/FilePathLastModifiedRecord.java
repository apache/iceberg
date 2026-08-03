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
package org.apache.iceberg.spark.source;

import java.sql.Timestamp;
import java.util.Objects;

public class FilePathLastModifiedRecord {
  private String filePath;
  private Timestamp lastModified;

  public FilePathLastModifiedRecord() {}

  public FilePathLastModifiedRecord(String filePath, Timestamp lastModified) {
    this.filePath = filePath;
    this.lastModified = lastModified;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public Timestamp getLastModified() {
    return lastModified;
  }

  public void setLastModified(Timestamp lastModified) {
    this.lastModified = lastModified;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FilePathLastModifiedRecord that = (FilePathLastModifiedRecord) o;
    return Objects.equals(filePath, that.filePath)
        && Objects.equals(lastModified, that.lastModified);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filePath, lastModified);
  }

  @Override
  public String toString() {
    return "FilePathLastModifiedRecord{"
        + "filePath='"
        + filePath
        + '\''
        + ", lastModified='"
        + lastModified
        + '\''
        + '}';
  }
}
