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

/**
 * An action that rewrites the table's metadata files to a staging directory, replacing all source
 * prefixes in absolute paths with a specified target prefix. There are two modes:
 *
 * <ul>
 *   <li><b>Complete copy:</b> Rewrites all metadata files to the staging directory.
 *   <li><b>Incremental copy:</b> Rewrites a subset of metadata files to the staging directory,
 *       consisting of metadata files added since a specified start version and/or until end
 *       version. The start/end version is identified by the name of a metadata.json file, and all
 *       metadata files added before/after these file are marked for rewrite.
 * </ul>
 *
 * This action can be used as the starting point to fully or incrementally copy an Iceberg table
 * located under the source prefix to the target prefix.
 *
 * <p>The action returns the following:
 *
 * <ol>
 *   <li>The name of the latest metadata.json file copied.
 *   <li>A listing of rewritten table metadata files, relative to the staging directory, that can be
 *       copied to the target prefix.
 *   <li>A listing of table data files, relative to the source prefix, that can be copied to the
 *       target prefix.
 * </ol>
 */
public interface RewriteTablePath extends Action<RewriteTablePath, RewriteTablePath.Result> {

  /**
   * Configure a source prefix that will be replaced by the specified target prefix in all paths
   *
   * @param sourcePrefix the source prefix to be replaced
   * @param targetPrefix the target prefix
   * @return this for method chaining
   */
  RewriteTablePath rewriteLocationPrefix(String sourcePrefix, String targetPrefix);

  /**
   * First metadata version to rewrite, identified by name of a metadata.json file in the table's
   * metadata log. It is optional, if provided then this action will only rewrite metadata files
   * added after this version.
   *
   * @param startVersion name of a metadata.json file. For example,
   *     "00001-8893aa9e-f92e-4443-80e7-cfa42238a654.metadata.json".
   * @return this for method chaining
   */
  RewriteTablePath startVersion(String startVersion);

  /**
   * Last metadata version to rewrite, identified by name of a metadata.json file in the table's
   * metadata log. It is optional, if provided then this action will only rewrite metadata files
   * added before this file, including the file itself.
   *
   * @param endVersion name of a metadata.json file. For example,
   *     "00001-8893aa9e-f92e-4443-80e7-cfa42238a654.metadata.json".
   * @return this for method chaining
   */
  RewriteTablePath endVersion(String endVersion);

  /**
   * Custom staging location. It is optional. By default, staging location is a subdirectory under
   * table's metadata directory.
   *
   * @param stagingLocation the staging location
   * @return this for method chaining
   */
  RewriteTablePath stagingLocation(String stagingLocation);

  /** The action result that contains a summary of the execution. */
  interface Result {
    /** staging location of rewritten files */
    String stagingLocation();

    /** path of data files list file. */
    String dataFileListLocation();

    /** path of metadata files list file. */
    String metadataFileListLocation();

    /** name of latest metadata file version */
    String latestVersion();
  }
}
