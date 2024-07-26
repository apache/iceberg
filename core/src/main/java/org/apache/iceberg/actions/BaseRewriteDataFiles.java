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

import java.util.List;
import org.immutables.value.Value;

@Value.Enclosing
@SuppressWarnings("ImmutablesStyle")
@Value.Style(
    typeImmutableEnclosing = "ImmutableRewriteDataFiles",
    visibilityString = "PUBLIC",
    builderVisibilityString = "PUBLIC")
interface BaseRewriteDataFiles extends RewriteDataFiles {

  @Value.Immutable
  interface Result extends RewriteDataFiles.Result {
    @Override
    @Value.Default
    default List<RewriteDataFiles.FileGroupFailureResult> rewriteFailures() {
      return RewriteDataFiles.Result.super.rewriteFailures();
    }

    @Override
    @Value.Default
    default int addedDataFilesCount() {
      return RewriteDataFiles.Result.super.addedDataFilesCount();
    }

    @Override
    @Value.Default
    default int rewrittenDataFilesCount() {
      return RewriteDataFiles.Result.super.rewrittenDataFilesCount();
    }

    @Override
    @Value.Default
    default long rewrittenBytesCount() {
      return RewriteDataFiles.Result.super.rewrittenBytesCount();
    }

    @Override
    @Value.Default
    default int removedDeleteFilesCount() {
      return RewriteDataFiles.Result.super.removedDeleteFilesCount();
    }

    @Override
    @Value.Default
    default int failedDataFilesCount() {
      return RewriteDataFiles.Result.super.failedDataFilesCount();
    }
  }

  @Value.Immutable
  interface FileGroupRewriteResult extends RewriteDataFiles.FileGroupRewriteResult {
    @Override
    @Value.Default
    default long rewrittenBytesCount() {
      return RewriteDataFiles.FileGroupRewriteResult.super.rewrittenBytesCount();
    }
  }

  @Value.Immutable
  interface FileGroupFailureResult extends RewriteDataFiles.FileGroupFailureResult {}

  @Value.Immutable
  interface FileGroupInfo extends RewriteDataFiles.FileGroupInfo {}
}
