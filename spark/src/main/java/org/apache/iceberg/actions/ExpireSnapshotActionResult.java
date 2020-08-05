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

public class ExpireSnapshotActionResult {

  private final Long dataFilesDeleted;
  private final Long manifestFilesDeleted;
  private final Long manifestListsDeleted;
  private final Long otherDeleted;

  public ExpireSnapshotActionResult(Long dataFilesDeleted, Long manifestFilesDeleted, Long manifestListsDeleted,
      Long otherDeleted) {
    this.dataFilesDeleted = dataFilesDeleted;
    this.manifestFilesDeleted = manifestFilesDeleted;
    this.manifestListsDeleted = manifestListsDeleted;
    this.otherDeleted = otherDeleted;
  }

  public Long getDataFilesDeleted() {
    return dataFilesDeleted;
  }

  public Long getManifestFilesDeleted() {
    return manifestFilesDeleted;
  }

  public Long getManifestListsDeleted() {
    return manifestListsDeleted;
  }

  public Long getOtherDeleted() {
    return otherDeleted;
  }
}
