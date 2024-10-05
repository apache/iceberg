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
package org.apache.iceberg;

import java.util.Arrays;
import java.util.List;

public class TestLocalDataTableScan
    extends DataTableScanTestBase<TableScan, FileScanTask, CombinedScanTask> {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @Override
  protected TableScan useRef(TableScan scan, String ref) {
    return scan.useRef(ref);
  }

  @Override
  protected TableScan useSnapshot(TableScan scan, long snapshotId) {
    return scan.useSnapshot(snapshotId);
  }

  @Override
  protected TableScan asOfTime(TableScan scan, long timestampMillis) {
    return scan.asOfTime(timestampMillis);
  }

  @Override
  protected TableScan newScan() {
    return table.newScan();
  }
}
