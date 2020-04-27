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

import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;

public class BaseReplacePartitions
    extends MergingSnapshotProducer<ReplacePartitions> implements ReplacePartitions {
  BaseReplacePartitions(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  protected ReplacePartitions self() {
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.OVERWRITE;
  }

  @Override
  public ReplacePartitions addFile(DataFile file) {
    dropPartition(file.partition());
    add(file);
    return this;
  }

  @Override
  public ReplacePartitions validateAppendOnly() {
    failAnyDelete();
    return this;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    if (writeSpec().fields().size() <= 0) {
      // replace all data in an unpartitioned table
      deleteByRowFilter(Expressions.alwaysTrue());
    }

    try {
      return super.apply(base);
    } catch (DeleteException e) {
      throw new ValidationException(
          "Cannot commit file that conflicts with existing partition: %s", e.partition());
    }
  }
}
