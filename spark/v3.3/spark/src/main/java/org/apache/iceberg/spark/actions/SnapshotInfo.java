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

public class SnapshotInfo {
  public static final Encoder<SnapshotInfo> ENCODER = Encoders.bean(SnapshotInfo.class);

  private long id;
  private long timestamp_ms;
  private String manifestListLocation;
  private String operation;

  public SnapshotInfo(long id, long timestamp_ms, String manifestListLocation, String operation) {
    this.id = id;
    this.timestamp_ms = timestamp_ms;
    this.manifestListLocation = manifestListLocation;
    this.operation = operation;
  }

  public SnapshotInfo() {}

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getTimestamp_ms() {
    return timestamp_ms;
  }

  public void setTimestamp_ms(long timestamp_ms) {
    this.timestamp_ms = timestamp_ms;
  }

  public String getManifestListLocation() {
    return manifestListLocation;
  }

  public void setManifestListLocation(String manifestListLocation) {
    this.manifestListLocation = manifestListLocation;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }
}
