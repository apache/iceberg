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

import java.io.IOException;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;

/**
 * Success file Commit policy for partitioned iceberg tables. Each time a partition is committed, a
 * file will be created or overwritten in the partition directory.
 */
public class SuccessFileCommitPolicy implements PartitionCommitPolicy {

  private final String successFileName;

  public SuccessFileCommitPolicy(String successFileName) {
    this.successFileName = successFileName;
  }

  @Override
  public void commit(Table table, PartitionKey partitionKey) {

    String successFileCommitPath =
        table.locationProvider().newDataLocation(table.spec(), partitionKey, successFileName);

    try (FileIO io = table.io()) {
      io.newOutputFile(successFileCommitPath).createOrOverwrite().close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
