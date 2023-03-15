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
package org.apache.iceberg.actions;

import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupRewriteResult;

/**
 * @deprecated will be removed in 1.4.0; use {@link
 *     ImmutableRewriteDataFiles.FileGroupRewriteResult#builder()} instead.
 */
@Deprecated
public class BaseFileGroupRewriteResult implements FileGroupRewriteResult {
  private final int addedDataFilesCount;
  private final int rewrittenDataFilesCount;
  private final long rewrittenBytesCount;
  private final FileGroupInfo info;

  public BaseFileGroupRewriteResult(
      FileGroupInfo info, int addedFilesCount, int rewrittenFilesCount) {
    this(info, addedFilesCount, rewrittenFilesCount, 0L);
  }

  public BaseFileGroupRewriteResult(
      FileGroupInfo info, int addedFilesCount, int rewrittenFilesCount, long rewrittenBytesCount) {
    this.info = info;
    this.addedDataFilesCount = addedFilesCount;
    this.rewrittenDataFilesCount = rewrittenFilesCount;
    this.rewrittenBytesCount = rewrittenBytesCount;
  }

  @Override
  public FileGroupInfo info() {
    return info;
  }

  @Override
  public int addedDataFilesCount() {
    return addedDataFilesCount;
  }

  @Override
  public int rewrittenDataFilesCount() {
    return rewrittenDataFilesCount;
  }

  @Override
  public long rewrittenBytesCount() {
    return rewrittenBytesCount;
  }
}
