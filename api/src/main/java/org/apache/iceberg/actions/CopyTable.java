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

import org.apache.iceberg.Table;

public interface CopyTable extends Action<CopyTable, CopyTable.Result> {

  /**
   * Passes the source and target prefixes that will be used to replace the source prefix with the
   * target one.
   *
   * @param sourcePrefix the source prefix to be replaced
   * @param targetPrefix the target prefix
   * @return this for method chaining
   */
  CopyTable rewriteLocationPrefix(String sourcePrefix, String targetPrefix);

  /**
   * Pass the version copied last time. It is optional if the target table is provided. The default
   * value is the target table's current version. User needs to make sure whether the start version
   * is valid if target table is not provided.
   *
   * @param lastCopiedVersion only version file name is needed, not the metadata json file path. For
   *     example, the version file would be "v2.metadata.json" for a Hadoop table. For metastore
   *     tables, the version file would be like
   *     "00001-8893aa9e-f92e-4443-80e7-cfa42238a654.metadata.json".
   * @return this for method chaining
   */
  CopyTable lastCopiedVersion(String lastCopiedVersion);

  /**
   * The latest version of the table to copy. It is optional, the default value is the source
   * table's current version.
   *
   * @param endVersion only version file name is needed, not the metadata json file path. For
   *     example, the version file would be "v2.metadata.json" for a Hadoop table. For metastore
   *     tables, the version file would be like
   *     "00001-8893aa9e-f92e-4443-80e7-cfa42238a654.metadata.json".
   * @return this for method chaining
   */
  CopyTable endVersion(String endVersion);

  /**
   * Set the customized staging location. It is optional. By default, staging location is a subdirectory
   * under table's metadata directory.
   *
   * @param stagingLocation the staging location
   * @return this for method chaining
   */
  CopyTable stagingLocation(String stagingLocation);

  /**
   * Set the target table. It is optional if the start version is provided.
   *
   * @param targetTable the target table
   * @return this for method chaining
   */
  CopyTable targetTable(Table targetTable);

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** Return staging location */
    String stagingLocation();

    /** Return directory of data files list. */
    String dataFileListLocation();

    /** Return directory of metadata files list. */
    String metadataFileListLocation();

    /** Return the latest version */
    String latestVersion();
  }
}
