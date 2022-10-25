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
package org.apache.iceberg.flink.source;

import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

public enum ScanMode {
  BATCH,
  INCREMENTAL_APPEND_SCAN,
  CHANGELOG_SCAN;

  public static ScanMode checkScanMode(ScanContext context) {

    if (!Strings.isNullOrEmpty(context.scanMode())) {
      return ScanMode.valueOf(context.scanMode().toUpperCase(Locale.ROOT));
    }

    if (context.startSnapshotId() != null
        || context.endSnapshotId() != null
        || context.startTag() != null
        || context.endTag() != null) {
      return ScanMode.INCREMENTAL_APPEND_SCAN;
    } else {
      return ScanMode.BATCH;
    }
  }
}
