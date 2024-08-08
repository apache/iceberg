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

public class BaseCopyTableActionResult implements CopyTable.Result {
  private final String stagingDirPath;
  private final String dataFileListPath;
  private final String metadataFileListPath;
  private final String latestVersion;

  public BaseCopyTableActionResult(
      String stagingDirPath,
      String dataFileListPath,
      String metadataFileListPath,
      String latestVersion) {
    this.stagingDirPath = stagingDirPath;
    this.dataFileListPath = dataFileListPath;
    this.metadataFileListPath = metadataFileListPath;
    this.latestVersion = latestVersion;
  }

  @Override
  public String stagingLocation() {
    return stagingDirPath;
  }

  @Override
  public String dataFileListLocation() {
    return dataFileListPath;
  }

  @Override
  public String metadataFileListLocation() {
    return metadataFileListPath;
  }

  @Override
  public String latestVersion() {
    return latestVersion;
  }
}
