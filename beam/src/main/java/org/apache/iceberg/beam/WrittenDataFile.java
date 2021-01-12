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

package org.apache.iceberg.beam;


import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaBeanSchema.class)
public class WrittenDataFile implements Serializable {
  private String filename;
  private long records;
  private long filesize;

  public WrittenDataFile() {

  }

  public WrittenDataFile(String filename, long records, long filesize) {
    this.filename = filename;
    this.records = records;
    this.filesize = filesize;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public long getRecords() {
    return records;
  }

  public void setRecords(long records) {
    this.records = records;
  }

  public long getFilesize() {
    return filesize;
  }

  public void setFilesize(long filesize) {
    this.filesize = filesize;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WrittenDataFile that = (WrittenDataFile) o;
    return records == that.records && filesize == that.filesize && filename.equals(that.filename);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filename, records, filesize);
  }
}
