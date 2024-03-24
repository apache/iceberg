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
package org.apache.iceberg.spark.actions;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class FileInfo {
  public static final Encoder<FileInfo> ENCODER = Encoders.bean(FileInfo.class);

  private String path;
  private String type;

  private long sizeInBytes;

  public FileInfo(String path, String type) {
    this.path = path;
    this.type = type;
  }

  public FileInfo(String path, String type, long sizeInBytes) {
    this.path = path;
    this.type = type;
    this.sizeInBytes = sizeInBytes;
  }

  public FileInfo() {}

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public long getSizeInBytes() {
    return sizeInBytes;
  }

  public void setSizeInBytes(long sizeInBytes) {
    this.sizeInBytes = sizeInBytes;
  }
}
