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
package org.apache.iceberg.rest;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.rest.operations.RestAppendFiles;
import org.apache.iceberg.rest.operations.RestDeleteFiles;
import org.apache.iceberg.rest.operations.RestOverwriteFiles;

public class RESTTable extends BaseTable {
  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> headers;
  private final boolean dataCommitViaRestEnabled;

  public RESTTable(
      TableOperations ops,
      String name,
      MetricsReporter reporter,
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      boolean dataCommitViaRestEnabled) {
    super(ops, name, reporter);
    this.client = client;
    this.headers = headers;
    this.path = path;
    this.dataCommitViaRestEnabled = dataCommitViaRestEnabled;
  }

  @Override
  public AppendFiles newAppend() {
    if (dataCommitViaRestEnabled) {
      return new RestAppendFiles(client, path, headers, operations());
    }
    return super.newAppend();
  }

  @Override
  public DeleteFiles newDelete() {
    if (dataCommitViaRestEnabled) {
      return new RestDeleteFiles(client, path, headers, operations());
    }
    return super.newDelete();
  }

  @Override
  public OverwriteFiles newOverwrite() {
    if (dataCommitViaRestEnabled) {
      return new RestOverwriteFiles(client, path, headers, operations());
    }
    return super.newOverwrite();
  }
}
