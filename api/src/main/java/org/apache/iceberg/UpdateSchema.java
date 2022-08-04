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

import java.util.Collection;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;

/**
 * API for schema evolution.
 *
 * <p>When committing, these changes will be applied to the current table metadata. Commit conflicts
 * will not be resolved and will result in a {@link CommitFailedException}.
 */
public interface UpdateSchema extends PendingUpdate<Schema> {

  /**
   * Allow incompatible changes to the schema.
   *
   * <p>Incompatible changes can cause failures when attempting to read older data files. For
   * example, adding a required column and attempting to read data files without that column will
   * cause a failure. However, if there are no data files that are not compatible with the change,
   * it can be allowed.
   *
   * <p>This option allows incompatible changes to be made to a schema. This should be used when the
   * caller has validated that the change will not break. For example, if a column is added as
   * optional but always populated and data older than the column addition has been deleted from the
   * table, this can be used with {@link #requireColumn(String)} to mark the column required.
   *
   * @return this for method chaining
   */
  UpdateSchema allowIncompatibleChanges();

  /**
   * Add a new top-level column.
   *
   * <p>Because "." may be interpreted as a column path separator or may be used in field names, it
   * is not allowed in names passed to this method. To add to nested structures or to add fields
   * with names that contain ".", use {@link #addColumn(String, String, Type)}.
   *
   * <p>If type is a nested type, its field IDs are reassigned when added to the existing schema.
   *
   * @param name name for the new column
   * @param type type for the new column
   * @return this for method chaining
   * @throws IllegalArgumentException If name contains "."
   */
  default UpdateSchema addColumn(String name, Type type) {
    return addColumn(name, type, null);
  }

  /**
   * Add a new top-level column.
   *
   * <p>Because "." may be interpreted as a column path separator or may be used in field names, it
   * is not allowed in names passed to this method. To add to nested structures or to add fields
   * with names that contain ".", use {@link #addColumn(String, String, Type)}.
   *
   * <p>If type is a nested type, its field IDs are reassigned when added to the existing schema.
   *
   * @param name name for the new column
   * @param type type for the new column
   * @param doc documentation string for the new column
   * @return this for method chaining
   * @throws IllegalArgumentException If name contains "."
   */
  UpdateSchema addColumn(String name, Type type, String doc);

  /**
   * Add a new column to a nested struct.
   *
   * <p>The parent name is used to find the parent using {@link Schema#findField(String)}. If the
   * parent name is null, the new column will be added to the root as a top-level column. If parent
   * identifies a struct, a new column is added to that struct. If it identifies a list, the column
   * is added to the list element struct, and if it identifies a map, the new column is added to the
   * map's value struct.
   *
   * <p>The given name is used to name the new column and names containing "." are not handled
   * differently.
   *
   * <p>If type is a nested type, its field IDs are reassigned when added to the existing schema.
   *
   * @param parent name of the parent struct to the column will be added to
   * @param name name for the new column
   * @param type type for the new column
   * @return this for method chaining
   * @throws IllegalArgumentException If parent doesn't identify a struct
   */
  default UpdateSchema addColumn(String parent, String name, Type type) {
    return addColumn(parent, name, type, null);
  }

  /**
   * Add a new column to a nested struct.
   *
   * <p>The parent name is used to find the parent using {@link Schema#findField(String)}. If the
   * parent name is null, the new column will be added to the root as a top-level column. If parent
   * identifies a struct, a new column is added to that struct. If it identifies a list, the column
   * is added to the list element struct, and if it identifies a map, the new column is added to the
   * map's value struct.
   *
   * <p>The given name is used to name the new column and names containing "." are not handled
   * differently.
   *
   * <p>If type is a nested type, its field IDs are reassigned when added to the existing schema.
   *
   * @param parent name of the parent struct to the column will be added to
   * @param name name for the new column
   * @param type type for the new column
   * @param doc documentation string for the new column
   * @return this for method chaining
   * @throws IllegalArgumentException If parent doesn't identify a struct
   */
  UpdateSchema addColumn(String parent, String name, Type type, String doc);

  /**
   * Add a new required top-level column.
   *
   * <p>This is an incompatible change that can break reading older data. This method will result in
   * an exception unless {@link #allowIncompatibleChanges()} has been called.
   *
   * <p>Because "." may be interpreted as a column path separator or may be used in field names, it
   * is not allowed in names passed to this method. To add to nested structures or to add fields
   * with names that contain ".", use {@link #addRequiredColumn(String, String, Type)}.
   *
   * <p>If type is a nested type, its field IDs are reassigned when added to the existing schema.
   *
   * @param name name for the new column
   * @param type type for the new column
   * @return this for method chaining
   * @throws IllegalArgumentException If name contains "."
   */
  default UpdateSchema addRequiredColumn(String name, Type type) {
    return addRequiredColumn(name, type, null);
  }

  /**
   * Add a new required top-level column.
   *
   * <p>This is an incompatible change that can break reading older data. This method will result in
   * an exception unless {@link #allowIncompatibleChanges()} has been called.
   *
   * <p>Because "." may be interpreted as a column path separator or may be used in field names, it
   * is not allowed in names passed to this method. To add to nested structures or to add fields
   * with names that contain ".", use {@link #addRequiredColumn(String, String, Type)}.
   *
   * <p>If type is a nested type, its field IDs are reassigned when added to the existing schema.
   *
   * @param name name for the new column
   * @param type type for the new column
   * @param doc documentation string for the new column
   * @return this for method chaining
   * @throws IllegalArgumentException If name contains "."
   */
  UpdateSchema addRequiredColumn(String name, Type type, String doc);

  /**
   * Add a new required top-level column.
   *
   * <p>This is an incompatible change that can break reading older data. This method will result in
   * an exception unless {@link #allowIncompatibleChanges()} has been called.
   *
   * <p>The parent name is used to find the parent using {@link Schema#findField(String)}. If the
   * parent name is null, the new column will be added to the root as a top-level column. If parent
   * identifies a struct, a new column is added to that struct. If it identifies a list, the column
   * is added to the list element struct, and if it identifies a map, the new column is added to the
   * map's value struct.
   *
   * <p>The given name is used to name the new column and names containing "." are not handled
   * differently.
   *
   * <p>If type is a nested type, its field IDs are reassigned when added to the existing schema.
   *
   * @param parent name of the parent struct to the column will be added to
   * @param name name for the new column
   * @param type type for the new column
   * @return this for method chaining
   * @throws IllegalArgumentException If parent doesn't identify a struct
   */
  default UpdateSchema addRequiredColumn(String parent, String name, Type type) {
    return addRequiredColumn(parent, name, type, null);
  }

  /**
   * Add a new required top-level column.
   *
   * <p>This is an incompatible change that can break reading older data. This method will result in
   * an exception unless {@link #allowIncompatibleChanges()} has been called.
   *
   * <p>The parent name is used to find the parent using {@link Schema#findField(String)}. If the
   * parent name is null, the new column will be added to the root as a top-level column. If parent
   * identifies a struct, a new column is added to that struct. If it identifies a list, the column
   * is added to the list element struct, and if it identifies a map, the new column is added to the
   * map's value struct.
   *
   * <p>The given name is used to name the new column and names containing "." are not handled
   * differently.
   *
   * <p>If type is a nested type, its field IDs are reassigned when added to the existing schema.
   *
   * @param parent name of the parent struct to the column will be added to
   * @param name name for the new column
   * @param type type for the new column
   * @param doc documentation string for the new column
   * @return this for method chaining
   * @throws IllegalArgumentException If parent doesn't identify a struct
   */
  UpdateSchema addRequiredColumn(String parent, String name, Type type, String doc);

  /**
   * Rename a column in the schema.
   *
   * <p>The name is used to find the column to rename using {@link Schema#findField(String)}.
   *
   * <p>The new name may contain "." and such names are not parsed or handled differently.
   *
   * <p>Columns may be updated and renamed in the same schema update.
   *
   * @param name name of the column to rename
   * @param newName replacement name for the column
   * @return this for method chaining
   * @throws IllegalArgumentException If name doesn't identify a column in the schema or if this
   *     change conflicts with other additions, renames, or updates.
   */
  UpdateSchema renameColumn(String name, String newName);

  /**
   * Update a column in the schema to a new primitive type.
   *
   * <p>The name is used to find the column to update using {@link Schema#findField(String)}.
   *
   * <p>Only updates that widen types are allowed.
   *
   * <p>Columns may be updated and renamed in the same schema update.
   *
   * @param name name of the column to rename
   * @param newType replacement type for the column
   * @return this for method chaining
   * @throws IllegalArgumentException If name doesn't identify a column in the schema or if this
   *     change introduces a type incompatibility or if it conflicts with other additions, renames,
   *     or updates.
   */
  UpdateSchema updateColumn(String name, Type.PrimitiveType newType);

  /**
   * Update a column in the schema to a new primitive type.
   *
   * <p>The name is used to find the column to update using {@link Schema#findField(String)}.
   *
   * <p>Only updates that widen types are allowed.
   *
   * <p>Columns may be updated and renamed in the same schema update.
   *
   * @param name name of the column to rename
   * @param newType replacement type for the column
   * @param newDoc replacement documentation string for the column
   * @return this for method chaining
   * @throws IllegalArgumentException If name doesn't identify a column in the schema or if this
   *     change introduces a type incompatibility or if it conflicts with other additions, renames,
   *     or updates.
   */
  default UpdateSchema updateColumn(String name, Type.PrimitiveType newType, String newDoc) {
    return updateColumn(name, newType).updateColumnDoc(name, newDoc);
  }

  /**
   * Update a column in the schema to a new primitive type.
   *
   * <p>The name is used to find the column to update using {@link Schema#findField(String)}.
   *
   * <p>Columns may be updated and renamed in the same schema update.
   *
   * @param name name of the column to rename
   * @param newDoc replacement documentation string for the column
   * @return this for method chaining
   * @throws IllegalArgumentException If name doesn't identify a column in the schema or if this
   *     change introduces a type incompatibility or if it conflicts with other additions, renames,
   *     or updates.
   */
  UpdateSchema updateColumnDoc(String name, String newDoc);

  /**
   * Update a column to optional.
   *
   * @param name name of the column to mark optional
   * @return this for method chaining
   */
  UpdateSchema makeColumnOptional(String name);

  /**
   * Update a column to required.
   *
   * <p>This is an incompatible change that can break reading older data. This method will result in
   * an exception unless {@link #allowIncompatibleChanges()} has been called.
   *
   * @param name name of the column to mark required
   * @return this for method chaining
   */
  UpdateSchema requireColumn(String name);

  /**
   * Delete a column in the schema.
   *
   * <p>The name is used to find the column to delete using {@link Schema#findField(String)}.
   *
   * @param name name of the column to delete
   * @return this for method chaining
   * @throws IllegalArgumentException If name doesn't identify a column in the schema or if this
   *     change conflicts with other additions, renames, or updates.
   */
  UpdateSchema deleteColumn(String name);

  /**
   * Move a column from its current position to the start of the schema or its parent struct.
   *
   * @param name name of the column to move
   * @return this for method chaining
   * @throws IllegalArgumentException If name doesn't identify a column in the schema or if this
   *     change conflicts with other changes.
   */
  UpdateSchema moveFirst(String name);

  /**
   * Move a column from its current position to directly before a reference column.
   *
   * <p>The name is used to find the column to move using {@link Schema#findField(String)}. If the
   * name identifies a nested column, it can only be moved within the nested struct that contains
   * it.
   *
   * @param name name of the column to move
   * @param beforeName name of the reference column
   * @return this for method chaining
   * @throws IllegalArgumentException If name doesn't identify a column in the schema or if this
   *     change conflicts with other changes.
   */
  UpdateSchema moveBefore(String name, String beforeName);

  /**
   * Move a column from its current position to directly after a reference column.
   *
   * <p>The name is used to find the column to move using {@link Schema#findField(String)}. If the
   * name identifies a nested column, it can only be moved within the nested struct that contains
   * it.
   *
   * @param name name of the column to move
   * @param afterName name of the reference column
   * @return this for method chaining
   * @throws IllegalArgumentException If name doesn't identify a column in the schema or if this
   *     change conflicts with other changes.
   */
  UpdateSchema moveAfter(String name, String afterName);

  /**
   * Applies all field additions and updates from the provided new schema to the existing schema so
   * to create a union schema.
   *
   * <p>For fields with same canonical names in both schemas it is required that the widen types is
   * supported using {@link UpdateSchema#updateColumn(String, Type.PrimitiveType)}
   *
   * <p>Only supports turning a previously required field into an optional one if it is marked
   * optional in the provided new schema using {@link UpdateSchema#makeColumnOptional(String)}
   *
   * <p>Only supports updating existing field docs with fields docs from the provided new schema
   * using {@link UpdateSchema#updateColumnDoc(String, String)}
   *
   * @param newSchema a schema used in conjunction with the existing schema to create a union schema
   * @return this for method chaining
   * @throws IllegalStateException If it encounters errors during provided schema traversal
   * @throws IllegalArgumentException If name doesn't identify a column in the schema or if this
   *     change introduces a type incompatibility or if it conflicts with other additions, renames,
   *     or updates.
   */
  UpdateSchema unionByNameWith(Schema newSchema);

  /**
   * Set the identifier fields given a set of field names.
   *
   * <p>Because identifier fields are unique, duplicated names will be ignored. See {@link
   * Schema#identifierFieldIds()} to learn more about Iceberg identifier.
   *
   * @param names names of the columns to set as identifier fields
   * @return this for method chaining
   */
  UpdateSchema setIdentifierFields(Collection<String> names);

  /**
   * Set the identifier fields given some field names. See {@link
   * UpdateSchema#setIdentifierFields(Collection)} for more details.
   *
   * @param names names of the columns to set as identifier fields
   * @return this for method chaining
   */
  default UpdateSchema setIdentifierFields(String... names) {
    return setIdentifierFields(Sets.newHashSet(names));
  }

  /**
   * Determines if the case of schema needs to be considered when comparing column names
   *
   * @param caseSensitive when false case is not considered in column name comparisons.
   * @return this for method chaining
   */
  default UpdateSchema caseSensitive(boolean caseSensitive) {
    throw new UnsupportedOperationException();
  }
}
