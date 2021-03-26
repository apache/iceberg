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

import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class BaseRewriteFiles extends MergingSnapshotProducer<RewriteFiles> implements RewriteFiles {
  BaseRewriteFiles(String tableName, TableOperations ops) {
    super(tableName, ops);

    // replace files must fail if any of the deleted paths is missing and cannot be deleted
    failMissingDeletePaths();
  }

  @Override
  protected RewriteFiles self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.REPLACE;
  }

  private void verifyInputAndOutputFiles(Set<DataFile> dataFilesToDelete, Set<DeleteFile> deleteFilesToDelete,
                                         Set<DataFile> dataFilesToAdd, Set<DeleteFile> deleteFilesToAdd) {
    Preconditions.checkNotNull(dataFilesToDelete, "Data files to delete can not be null");
    Preconditions.checkNotNull(deleteFilesToDelete, "Delete files to delete can not be null");
    Preconditions.checkNotNull(dataFilesToAdd, "Data files to add can not be null");
    Preconditions.checkNotNull(deleteFilesToAdd, "Delete files to add can not be null");

    int filesToDelete = 0;
    filesToDelete += dataFilesToDelete.size();
    filesToDelete += deleteFilesToDelete.size();

    Preconditions.checkArgument(filesToDelete > 0, "Files to delete cannot be null or empty");

    if (deleteFilesToDelete.isEmpty()) {
      // When there is no delete files in the rewrite action, data files to add cannot be null or empty.
      Preconditions.checkArgument(dataFilesToAdd.size() > 0,
          "Data files to add can not be empty because there's no delete file to be rewritten");
      Preconditions.checkArgument(deleteFilesToAdd.isEmpty(),
          "Delete files to add must be empty because there's no delete file to be rewritten");
    }
  }

  @Override
  public RewriteFiles rewriteFiles(Set<DataFile> dataFilesToDelete, Set<DeleteFile> deleteFilesToDelete,
                                   Set<DataFile> dataFilesToAdd, Set<DeleteFile> deleteFilesToAdd) {
    verifyInputAndOutputFiles(dataFilesToDelete, deleteFilesToDelete, dataFilesToAdd, deleteFilesToAdd);

    for (DataFile dataFile : dataFilesToDelete) {
      delete(dataFile);
    }

    for (DeleteFile deleteFile : deleteFilesToDelete) {
      delete(deleteFile);
    }

    for (DataFile dataFile : dataFilesToAdd) {
      add(dataFile);
    }

    for (DeleteFile deleteFile : deleteFilesToAdd) {
      add(deleteFile);
    }

    return this;
  }
}
