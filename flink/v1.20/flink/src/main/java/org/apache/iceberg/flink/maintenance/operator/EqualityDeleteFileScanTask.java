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

import org.apache.flink.annotation.Internal;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

/**
 * {@link ContentScanTask} for reading a single equality delete file standalone. The {@link
 * DeleteFile#equalityFieldIds()} on {@link #file()} carries the PK field IDs the worker resolves
 * against.
 *
 * <p>A plain class rather than a record so it round-trips through Flink's Kryo fallback on Flink
 * 1.20, whose bundled Kryo 2.x cannot build a serializer for record classes.
 */
@Internal
class EqualityDeleteFileScanTask implements ContentScanTask<DeleteFile> {

  private final DeleteFile file;
  private final PartitionSpec spec;

  EqualityDeleteFileScanTask(DeleteFile file, PartitionSpec spec) {
    this.file = file;
    this.spec = spec;
  }

  @Override
  public DeleteFile file() {
    return file;
  }

  @Override
  public PartitionSpec spec() {
    return spec;
  }

  @Override
  public long start() {
    return 0L;
  }

  @Override
  public long length() {
    return file.fileSizeInBytes();
  }

  @Override
  public Expression residual() {
    return Expressions.alwaysTrue();
  }
}
