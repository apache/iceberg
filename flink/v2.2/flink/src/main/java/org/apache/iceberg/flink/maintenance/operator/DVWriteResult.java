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

import java.io.Serializable;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.DeleteFile;

/**
 * Result from the {@code EqualityConvertDVWriter} containing new and rewritten DV files written by
 * a single resolver task. A result with {@link #hasError()} signals that the writer failed; the
 * committer must not commit data files in that case.
 */
@Internal
public record DVWriteResult(
    List<DeleteFile> dvFiles, List<DeleteFile> rewrittenDvFiles, boolean hasError)
    implements Serializable {

  public DVWriteResult(List<DeleteFile> dvFiles, List<DeleteFile> rewrittenDvFiles) {
    this(dvFiles, rewrittenDvFiles, false);
  }

  /** Sentinel result that tells the committer to abort the current cycle. */
  public static final DVWriteResult ABORT = new DVWriteResult(null, null, true);

  public boolean isAbort() {
    return hasError;
  }
}
