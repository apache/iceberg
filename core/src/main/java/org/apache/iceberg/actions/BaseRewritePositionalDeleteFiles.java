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

import org.immutables.value.Value;
import org.immutables.value.Value.Style.BuilderVisibility;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Value.Enclosing
@SuppressWarnings("ImmutablesStyle")
@Value.Style(
    typeImmutableEnclosing = "ImmutableRewritePositionDeleteFiles",
    visibility = ImplementationVisibility.PUBLIC,
    builderVisibility = BuilderVisibility.PUBLIC)
interface BaseRewritePositionalDeleteFiles extends RewritePositionDeleteFiles {

  @Value.Immutable
  interface Result extends RewritePositionDeleteFiles.Result {
    @Override
    @Value.Default
    default int rewrittenDeleteFilesCount() {
      return RewritePositionDeleteFiles.Result.super.rewrittenDeleteFilesCount();
    }

    @Override
    @Value.Default
    default int addedDeleteFilesCount() {
      return RewritePositionDeleteFiles.Result.super.addedDeleteFilesCount();
    }

    @Override
    @Value.Default
    default long rewrittenBytesCount() {
      return RewritePositionDeleteFiles.Result.super.rewrittenBytesCount();
    }

    @Override
    @Value.Default
    default long addedBytesCount() {
      return RewritePositionDeleteFiles.Result.super.addedBytesCount();
    }
  }

  @Value.Immutable
  interface FileGroupRewriteResult extends RewritePositionDeleteFiles.FileGroupRewriteResult {}

  @Value.Immutable
  interface FileGroupInfo extends RewritePositionDeleteFiles.FileGroupInfo {}
}
