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
package org.apache.iceberg.flink.sink;

import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;

/**
 * Commit policy for partitioned iceberg tables. default: add partition to meta data. success-file:
 * add '_SUCESS' file to directory. custom: use policy class to create a commit policy.
 */
public interface PartitionCommitPolicy {
  String SUCCESS_FILE = "success-file";
  String CUSTOM = "custom";
  String DEFAULT = "default";

  void commit(Table table, PartitionKey partitionKey);
}
