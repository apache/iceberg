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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;

/**
 * API for partition spec evolution.
 *
 * <p>When committing, these changes will be applied to the current table metadata. Commit conflicts
 * will not be resolved and will result in a {@link CommitFailedException}.
 */
public interface UpdatePartitionSpec extends PendingUpdate<PartitionSpec> {
  /**
   * Set whether column resolution in the source schema should be case sensitive.
   *
   * @param isCaseSensitive whether column resolution should be case sensitive
   * @return this for method chaining
   */
  UpdatePartitionSpec caseSensitive(boolean isCaseSensitive);

  /**
   * Add a new partition field from a source column.
   *
   * <p>The partition field will be created as an identity partition field for the given source
   * column, with the same name as the source column.
   *
   * <p>The source column is located using {@link Schema#findField(String)}.
   *
   * @param sourceName source column name in the table schema
   * @return this for method chaining
   * @throws IllegalArgumentException If the an identity partition field for the source already
   *     exists, or if this change conflicts with other additions, removals, or renames.
   */
  UpdatePartitionSpec addField(String sourceName);

  /**
   * Add a new partition field from an {@link Expressions expression term}.
   *
   * <p>The partition field will use the term's transform or the identity transform if the term is a
   * reference.
   *
   * <p>The term's reference is used to locate the source column using {@link
   * Schema#findField(String)}.
   *
   * <p>The new partition field will be named for the source column and the transform.
   *
   * @param term source column name in the table schema
   * @return this for method chaining
   * @throws IllegalArgumentException If the a partition field for the transform and source already
   *     exists, or if this change conflicts with other additions, removals, or renames.
   */
  UpdatePartitionSpec addField(Term term);

  /**
   * Add a new partition field from an {@link Expressions expression term}, with the given partition
   * field name.
   *
   * <p>The partition field will use the term's transform or the identity transform if the term is a
   * reference.
   *
   * <p>The term's reference is used to locate the source column using {@link
   * Schema#findField(String)}.
   *
   * @param name name for the partition field
   * @param term expression for the partition transform
   * @return this for method chaining
   * @throws IllegalArgumentException If the a partition field for the transform and source already
   *     exists, if a partition field with the given name already exists, or if this change
   *     conflicts with other additions, removals, or renames.
   */
  UpdatePartitionSpec addField(String name, Term term);

  /**
   * Remove a partition field by name.
   *
   * @param name name of the partition field to remove
   * @return this for method chaining
   * @throws IllegalArgumentException If the a partition field with the given name does not exist,
   *     or if this change conflicts with other additions, removals, or renames.
   */
  UpdatePartitionSpec removeField(String name);

  /**
   * Remove a partition field by its transform {@link Expressions expression term}.
   *
   * <p>The partition field with the same transform and source reference will be removed. If the
   * term is a reference and does not have a transform, the identity transform is used.
   *
   * @param term expression for the partition transform to remove
   * @return this for method chaining
   * @throws IllegalArgumentException If the a partition field with the given transform and source
   *     does not exist, or if this change conflicts with other additions, removals, or renames.
   */
  UpdatePartitionSpec removeField(Term term);

  /**
   * Rename a field in the partition spec.
   *
   * @param name name of the partition field to rename
   * @param newName replacement name for the partition field
   * @return this for method chaining
   * @throws IllegalArgumentException If name doesn't identify a column in the schema or if this
   *     change conflicts with other additions, removals, or renames.
   */
  UpdatePartitionSpec renameField(String name, String newName);

  /**
   * Sets that the new partition spec will be NOT set as the default partition spec for the table,
   * the default behavior is to do so.
   *
   * @return this for method chaining
   */
  default UpdatePartitionSpec addNonDefaultSpec() {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement addNonDefaultSpec()");
  };
}
