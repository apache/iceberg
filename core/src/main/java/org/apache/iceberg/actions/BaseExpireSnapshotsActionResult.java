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

public class BaseExpireSnapshotsActionResult implements ExpireSnapshots.Result {

  private final long deletedDataFilesCount;
  private final long deletedPosDeleteFilesCount;
  private final long deletedEqDeleteFilesCount;
  private final long deletedManifestsCount;
  private final long deletedManifestListsCount;

  public BaseExpireSnapshotsActionResult(
      long deletedDataFilesCount, long deletedManifestsCount, long deletedManifestListsCount) {
    this.deletedDataFilesCount = deletedDataFilesCount;
    this.deletedPosDeleteFilesCount = 0;
    this.deletedEqDeleteFilesCount = 0;
    this.deletedManifestsCount = deletedManifestsCount;
    this.deletedManifestListsCount = deletedManifestListsCount;
  }

  public BaseExpireSnapshotsActionResult(
      long deletedDataFilesCount,
      long deletedPosDeleteFilesCount,
      long deletedEqDeleteFilesCount,
      long deletedManifestsCount,
      long deletedManifestListsCount) {
    this.deletedDataFilesCount = deletedDataFilesCount;
    this.deletedPosDeleteFilesCount = deletedPosDeleteFilesCount;
    this.deletedEqDeleteFilesCount = deletedEqDeleteFilesCount;
    this.deletedManifestsCount = deletedManifestsCount;
    this.deletedManifestListsCount = deletedManifestListsCount;
  }

  @Override
  public long deletedDataFilesCount() {
    return deletedDataFilesCount;
  }

  @Override
  public long deletedPositionDeleteFilesCount() {
    return deletedPosDeleteFilesCount;
  }

  @Override
  public long deletedEqualityDeleteFilesCount() {
    return deletedEqDeleteFilesCount;
  }

  @Override
  public long deletedManifestsCount() {
    return deletedManifestsCount;
  }

  @Override
  public long deletedManifestListsCount() {
    return deletedManifestListsCount;
  }
}
