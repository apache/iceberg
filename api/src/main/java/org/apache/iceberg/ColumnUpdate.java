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

/**
 * API for adding column update files to a table.
 *
 * <p>This API accumulates column update file additions and produces a new {@link Snapshot} of the
 * table. Column update files contain updated values for specific columns in existing data files.
 *
 * <p>Each column update operation associates an update file with its corresponding base data file
 * and the field IDs of the columns being updated. The update file contains the new values for the
 * specified columns that will be applied when reading the base data file.
 */
public interface ColumnUpdate extends SnapshotUpdate<ColumnUpdate> {

  ColumnUpdate withFieldIds(List<Integer> fieldIds);

  // TODO gaborkaszab: Alternatively we can pass a path instead of DataFile for baseFile?
  // Apparently, both are possible from Spark write. We'd loose a couple of verification
  // opportunities if it wasn't a DataFile but other than the checks only the path is needed for
  // baseFile.
  ColumnUpdate addColumnUpdate(DataFile baseFile, DataFile updateFile);

  // TODO gaborkaszab: We have to think about adding an API to detect conflicts before committing.
  // Have to think through how to solve this, since this is an extensive operation that can conflict
  // many ways.
}
