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
package org.apache.iceberg.spark.source;

import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;

interface SparkMicroBatchPlanner {
  /**
   * Return the {@link FileScanTask}s for data added between the start and end offsets.
   *
   * @param startOffset the offset to start planning from
   * @param endOffset the offset to plan up to
   * @return file scan tasks for data in the offset range
   */
  List<FileScanTask> planFiles(StreamingOffset startOffset, StreamingOffset endOffset);

  /**
   * Return the latest offset the stream can advance to from {@code startOffset}, respecting the
   * given {@link ReadLimit}.
   *
   * @param startOffset the current offset of the stream
   * @param limit the read limit bounding how far ahead to advance
   * @return the latest available offset, or {@code null} if no new data is available
   */
  StreamingOffset latestOffset(StreamingOffset startOffset, ReadLimit limit);

  /** Stop the planner and release any resources. */
  void stop();
}
