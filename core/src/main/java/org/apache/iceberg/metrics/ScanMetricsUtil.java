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
package org.apache.iceberg.metrics;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;

public class ScanMetricsUtil {

  private ScanMetricsUtil() {}

  public static void indexedDeleteFile(ScanMetrics metrics, DeleteFile deleteFile) {
    metrics.indexedDeleteFiles().increment();

    if (deleteFile.content() == FileContent.POSITION_DELETES) {
      metrics.positionalDeleteFiles().increment();
    } else if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
      metrics.equalityDeleteFiles().increment();
    }
  }
}
