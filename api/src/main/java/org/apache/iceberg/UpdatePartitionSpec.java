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

import org.apache.iceberg.exceptions.CommitFailedException;

/**
 * API for partition spec evolution.
 * <p>
 * When committing, these changes will be applied to the current table metadata. Commit conflicts
 * will not be resolved and will result in a {@link CommitFailedException}.
 */
public interface UpdatePartitionSpec extends PendingUpdate<PartitionSpec> {

  /** Update the current partition spec to a new partition spec.
   * <p>
   * Partition field IDs of the new partitionSpec may be updated during the commit.
   *
   * @param partitionSpec new partition spec to update
   * @return this for method chaining
   */
  UpdatePartitionSpec update(PartitionSpec partitionSpec);

}
