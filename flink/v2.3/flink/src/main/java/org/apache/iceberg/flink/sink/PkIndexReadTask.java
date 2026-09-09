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
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.InstantiationUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

@Internal
class PkIndexReadTask implements Serializable {

  private final DataFile file;
  private final DeleteFile[] deletes;

  PkIndexReadTask(DataFile file, List<DeleteFile> deletes) {
    Preconditions.checkNotNull(file, "Data file cannot be null");
    this.file = file;
    this.deletes = deletes == null ? new DeleteFile[0] : deletes.toArray(new DeleteFile[0]);
  }

  DataFile file() {
    return file;
  }

  DeleteFile[] deletes() {
    return deletes;
  }

  byte[] encode() {
    try {
      return InstantiationUtil.serializeObject(this);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize the read task for " + file.location(), e);
    }
  }

  static PkIndexReadTask decode(byte[] serialized) {
    try {
      return InstantiationUtil.deserializeObject(
          serialized, PkIndexReadTask.class.getClassLoader());
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalStateException("Failed to deserialize a primary key index read task", e);
    }
  }
}
