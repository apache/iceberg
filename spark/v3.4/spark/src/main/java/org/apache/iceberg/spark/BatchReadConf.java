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

import java.io.Serializable;

public class BatchReadConf implements Serializable {

  private int parquetBatchSize;
  private int orcBatchSize;
  private ParquetReaderType parquetReaderType;

  public BatchReadConf(
      int parquetBatchSize, int orcBatchSize, ParquetReaderType parquetReaderType) {
    this.parquetBatchSize = parquetBatchSize;
    this.orcBatchSize = orcBatchSize;
    this.parquetReaderType = parquetReaderType;
  }

  public int parquetBatchSize() {
    return parquetBatchSize;
  }

  public int orcBatchSize() {
    return orcBatchSize;
  }

  public ParquetReaderType parquetReaderType() {
    return parquetReaderType;
  }
}
