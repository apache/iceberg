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
package org.apache.iceberg.io;

/**
 * This interface is intended as an extension for FileIO implementations to provide additional
 * best-effort recovery operations that can be useful for repairing corrupted tables where there are
 * reachable files missing from disk. (e.g. a live manifest points to data file entry which no
 * longer exists on disk)
 */
public interface SupportsRecoveryOperations {

  /**
   * Perform a best-effort recovery of a file at a given path
   *
   * @param path Absolute path of file to attempt recovery for
   * @return true if recovery was successful, false otherwise
   */
  boolean recoverFile(String path);
}
