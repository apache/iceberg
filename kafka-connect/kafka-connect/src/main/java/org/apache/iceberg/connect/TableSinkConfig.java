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
package org.apache.iceberg.connect;

import java.util.List;
import java.util.regex.Pattern;

public class TableSinkConfig {

  private final Pattern routeRegex;
  private final List<String> idColumns;
  private final List<String> partitionBy;
  private final String commitBranch;

  public TableSinkConfig(
      Pattern routeRegex, List<String> idColumns, List<String> partitionBy, String commitBranch) {
    this.routeRegex = routeRegex;
    this.idColumns = idColumns;
    this.partitionBy = partitionBy;
    this.commitBranch = commitBranch;
  }

  public Pattern routeRegex() {
    return routeRegex;
  }

  public List<String> idColumns() {
    return idColumns;
  }

  public List<String> partitionBy() {
    return partitionBy;
  }

  public String commitBranch() {
    return commitBranch;
  }
}
