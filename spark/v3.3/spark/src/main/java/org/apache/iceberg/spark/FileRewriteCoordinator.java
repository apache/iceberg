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
package org.apache.iceberg.spark;

import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;

public class FileRewriteCoordinator extends BaseFileRewriteCoordinator<DataFile> {

  private static final FileRewriteCoordinator INSTANCE = new FileRewriteCoordinator();

  private FileRewriteCoordinator() {}

  public static FileRewriteCoordinator get() {
    return INSTANCE;
  }

  /** @deprecated will be removed in 1.4.0; use {@link #fetchNewFiles(Table, String)} instead. */
  @Deprecated
  public Set<DataFile> fetchNewDataFiles(Table table, String fileSetId) {
    return fetchNewFiles(table, fileSetId);
  }

  /** @deprecated will be removed in 1.4.0; use {@link #fetchSetIds(Table)} instead */
  @Deprecated
  public Set<String> fetchSetIDs(Table table) {
    return fetchSetIds(table);
  }
}
