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
package org.apache.iceberg;

/** {@link RemoveMissingFiles} implementation. */
public class BaseRemoveFiles extends MergingSnapshotProducer<RemoveMissingFiles>
    implements RemoveMissingFiles {
  private boolean validateFilesToDeleteExist = false;

  protected BaseRemoveFiles(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected RemoveMissingFiles self() {
    return this;
  }

  @Override
  protected String operation() {
    if (!deletesDeleteFiles()) {
      return DataOperations.DELETE;
    }

    return DataOperations.OVERWRITE;
  }

  @Override
  public RemoveMissingFiles deleteFile(DataFile file) {
    delete(file);
    return this;
  }

  @Override
  public RemoveMissingFiles deleteFile(DeleteFile file) {
    delete(file);
    return this;
  }

  @Override
  public RemoveMissingFiles validateFilesExist() {
    this.validateFilesToDeleteExist = true;
    return this;
  }

  @Override
  public RemoveMissingFiles toBranch(String branch) {
    targetBranch(branch);
    return this;
  }

  @Override
  protected void validate(TableMetadata base, Snapshot parent) {
    if (validateFilesToDeleteExist) {
      failMissingDeletePaths();
    }
  }
}
