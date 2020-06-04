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
import org.apache.iceberg.transforms.Transform;

/**
 * API for partition spec evolution.
 * <p>
 * When committing, these changes will be applied to the current table metadata. Commit conflicts
 * will not be resolved and will result in a {@link CommitFailedException}.
 */
public interface UpdatePartitionSpec extends PendingUpdate<PartitionSpec> {

  /**
   * Clear all partition fields in the current partition spec.
   * <p>
   * This will create a new partition spec without any partition field.
   * Partition field IDs is automatically assigned and will be updated during the commit.
   * Table schema should be obtained from the current table metadata
   *
   * @return this for method chaining
   */
  UpdatePartitionSpec clear();

  /**
   * Add a new partition field with identity transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param targetName the name of this partition field
   * @return this for method chaining
   */
  UpdatePartitionSpec addIdentityField(String sourceName, String targetName);

  /**
   * Add a new partition field with identity transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @return this for method chaining
   */
  UpdatePartitionSpec addIdentityField(String sourceName);

  /**
   * Add a new partition field with year transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param targetName the name of this partition field
   * @return this for method chaining
   */
  UpdatePartitionSpec addYearField(String sourceName, String targetName);

  /**
   * Add a new partition field with year transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @return this for method chaining
   */
  UpdatePartitionSpec addYearField(String sourceName);

  /**
   * Add a new partition field with month transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param targetName the name of this partition field
   * @return this for method chaining
   */
  UpdatePartitionSpec addMonthField(String sourceName, String targetName);

  /**
   * Add a new partition field with month transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @return this for method chaining
   */
  UpdatePartitionSpec addMonthField(String sourceName);

  /**
   * Add a new partition field with day transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param targetName the name of this partition field
   * @return this for method chaining
   */
  UpdatePartitionSpec addDayField(String sourceName, String targetName);

  /**
   * Add a new partition field with day transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @return this for method chaining
   */
  UpdatePartitionSpec addDayField(String sourceName);

  /**
   * Add a new partition field with hour transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param targetName the name of this partition field
   * @return this for method chaining
   */
  UpdatePartitionSpec addHourField(String sourceName, String targetName);

  /**
   * Add a new partition field with hour transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @return this for method chaining
   */
  UpdatePartitionSpec addHourField(String sourceName);

  /**
   * Add a new partition field with bucket transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param numBuckets the number of buckets
   * @param targetName the name of this partition field
   * @return this for method chaining
   */
  UpdatePartitionSpec addBucketField(String sourceName, int numBuckets, String targetName);

  /**
   * Add a new partition field with bucket transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param numBuckets the number of buckets
   * @return this for method chaining
   */
  UpdatePartitionSpec addBucketField(String sourceName, int numBuckets);

  /**
   * Add a new partition field with truncate transform to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param width the width of truncation
   * @param targetName the name of this partition field
   * @return this for method chaining
   */
  UpdatePartitionSpec addTruncateField(String sourceName, int width, String targetName);

  /**
   * Add a new partition field with truncate transform to the partition spec.
   * <p>
   * The partition field name is automatically assigned set.
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceName the field name of the source field in the {@link PartitionSpec spec's} table schema
   * @param width the width of truncation
   * @return this for method chaining
   */
  UpdatePartitionSpec addTruncateField(String sourceName, int width);

  /**
   * Add a new partition field to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceId the source field id in the {@link PartitionSpec spec's} table schema
   * @param name the name of this partition field
   * @param transform the partition transform in string format
   * @return this for method chaining
   */
  UpdatePartitionSpec addField(int sourceId, String name, String transform);

  /**
   * Add a new partition field to the partition spec.
   * <p>
   * The partition field id is automatically assigned and will be updated during the commit.
   *
   * @param sourceId the source field id in the {@link PartitionSpec spec's} table schema
   * @param name the name of this partition field
   * @param transform the partition transform
   * @return this for method chaining
   */
  UpdatePartitionSpec addField(int sourceId, String name, Transform<?, ?> transform);

  /**
   * Rename a partition field in the partition spec.
   * <p>
   *
   * @param name the name of a partition field to be renamed
   * @param newName the new name of the partition field
   * @return this for method chaining
   */
  UpdatePartitionSpec renameField(String name, String newName);

  /**
   * Remove a partition field in the partition spec.
   * <p>
   * The partition field will be soft deleted for a table with V1 metadata and hard deleted in a higher version.
   *
   * @param name the name of a partition field to be removed
   * @return this for method chaining
   */
  UpdatePartitionSpec removeField(String name);

  /**
   * Replace a partition field with a new transform in the partition spec.
   * <p>
   * It is equivalent to remove the partition field and then add it back with the new transform
   *
   * @param name the name of a partition field to be replaced
   * @param transform the new partition transform to be used in string format
   * @return this for method chaining
   */
  UpdatePartitionSpec replaceField(String name, String transform);

  /**
   * Replace a partition field with a new transform in the partition spec.
   * <p>
   * It is equivalent to remove the partition field and then add it back with the new transform
   *
   * @param name the name of a partition field to be replaced
   * @param transform the new partition transform to be used
   * @return this for method chaining
   */
  UpdatePartitionSpec replaceField(String name, Transform<?, ?> transform);

}
