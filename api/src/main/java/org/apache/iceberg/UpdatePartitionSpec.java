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

  /**
   * Create a new partition spec builder for a given schema
   * <p>
   * Partition field IDs is automatically assigned and will be updated during the commit.
   *
   * @param schema the schema for the new partition spec
   * @return this for method chaining
   */
  UpdatePartitionSpec newSpec(Schema schema);

  /**
   * Add a new partition field with identity transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param targetName the name of this partition field
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec identity(String sourceName, String targetName);

  /**
   * Add a new partition field with identity transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec identity(String sourceName);

  /**
   * Add a new partition field with year transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param targetName the name of this partition field
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec year(String sourceName, String targetName);

  /**
   * Add a new partition field with year transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec year(String sourceName);

  /**
   * Add a new partition field with month transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param targetName the name of this partition field
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec month(String sourceName, String targetName);

  /**
   * Add a new partition field with month transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec month(String sourceName);

  /**
   * Add a new partition field with day transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param targetName the name of this partition field
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec day(String sourceName, String targetName);

  /**
   * Add a new partition field with day transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec day(String sourceName);

  /**
   * Add a new partition field with hour transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param targetName the name of this partition field
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec hour(String sourceName, String targetName);

  /**
   * Add a new partition field with hour transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec hour(String sourceName);

  /**
   * Add a new partition field with bucket transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param numBuckets the number of buckets
   * @param targetName the name of this partition field
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec bucket(String sourceName, int numBuckets, String targetName);

  /**
   * Add a new partition field with bucket transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param numBuckets the number of buckets
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec bucket(String sourceName, int numBuckets);

  /**
   * Add a new partition field with truncate transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param width the width of truncation
   * @param targetName the name of this partition field
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec truncate(String sourceName, int width, String targetName);

  /**
   * Add a new partition field with truncate transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param width the width of truncation
   * @return this for method chaining
   * @throws NullPointerException If the table schema is not set by {@link #newSpec} before calling this method
   */
  UpdatePartitionSpec truncate(String sourceName, int width);

}
